//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  printf("hash join executor constructor\n");
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();

  Tuple left_tuple;
  RID left_rid;

  // init the hash table vectors hash_table_
  for (size_t i = 0; i < plan_->LeftJoinKeyExpressions().size(); i++) {
    hash_tables_.emplace_back();
  }

  int left_table_size =0;

  while (left_child_->Next(&left_tuple, &left_rid)) {
    left_table_size++;
    std::vector<HashJoinKey> key = MakeHashJoinLeftKey(&left_tuple, left_child_->GetOutputSchema());
    for (size_t i = 0; i < key.size(); i++) {
      auto left_key = key[i];
      if (hash_tables_[i].find(left_key) == hash_tables_[i].end()) {
        hash_tables_[i][left_key] = std::vector<Tuple>();
      }
      hash_tables_[i][left_key].push_back(left_tuple);
    }

    // update the left_done_ map
    CompositeJoinKey composite_join_key;
    composite_join_key.keys = key;
    left_done_[composite_join_key] = false;

    printf("%s\n", left_tuple.ToString(&left_child_->GetOutputSchema()).c_str());
  }
  printf("done building left table, left table size is %d\n", left_table_size);
}

auto HashJoinExecutor::InnerJoinOutput(const Tuple &left, const Tuple &right) -> Tuple {
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  for (uint32_t idx = 0; idx < left_child_->GetOutputSchema().GetColumnCount(); idx++) {
    values.push_back(left.GetValue(&left_child_->GetOutputSchema(), idx));
  }
  for (uint32_t idx = 0; idx < right_child_->GetOutputSchema().GetColumnCount(); idx++) {
    values.push_back(right.GetValue(&right_child_->GetOutputSchema(), idx));
  }
  return Tuple(values, &GetOutputSchema());
}

auto HashJoinExecutor::LeftOuterJoinOutput(const Tuple &left) -> Tuple {
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  for (uint32_t idx = 0; idx < left_child_->GetOutputSchema().GetColumnCount(); idx++) {
    values.push_back(left.GetValue(&left_child_->GetOutputSchema(), idx));
  }
  for (uint32_t idx = 0; idx < right_child_->GetOutputSchema().GetColumnCount(); idx++) {
    values.push_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(idx).GetType()));
  }
  return Tuple(values, &GetOutputSchema());
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!queue_.empty()) {
    *tuple = queue_.front();
    queue_.pop();
    *rid = tuple->GetRid();
    return true;
  }

  while (true) {
    Tuple right_tuple;
    RID right_rid;
    if (!right_child_->Next(&right_tuple, &right_rid)) {
      break;
    }

    auto key = MakeHashJoinRightKey(&right_tuple, right_child_->GetOutputSchema());

    std::vector<Tuple> left_tuple_candidates_;
    for (size_t i = 0; i < key.size(); i++) {
      if (i == 0){
        left_tuple_candidates_ = hash_tables_[i][key[i]];
      } else {
        std::vector<Tuple> temp;
        for (auto &candidate : left_tuple_candidates_) {
          if (key[i] == MakeHashJoinLeftKey(&candidate, left_child_->GetOutputSchema())[i]) {
            temp.push_back(std::move(candidate));
          }
        }
        left_tuple_candidates_ = temp;
      }
    }

    for (auto &match : left_tuple_candidates_) {
      queue_.push(InnerJoinOutput(match, right_tuple));
      left_done_[{MakeHashJoinLeftKey(&match, left_child_->GetOutputSchema())}] = true;
      // printf("marking left done as true\n");
    }

    if (!queue_.empty()) {
      *tuple = queue_.front();
      queue_.pop();
      *rid = tuple->GetRid();
      return true;
    }
  }

  if (plan_->GetJoinType() == JoinType::LEFT && !left_join_check_) {
    int count = 0;
    int left_join_count = 0;
    // printf("size of left done: %lu\n", left_done_.size());
    // printf("checking left join\n");
    left_child_->Init();
    Tuple left_tuple;
    RID left_rid;
    while (left_child_->Next(&left_tuple, &left_rid)) {
      count++;
      auto key = MakeHashJoinLeftKey(&left_tuple, left_child_->GetOutputSchema());
      if (left_done_[{key}] == false) {
        queue_.push(LeftOuterJoinOutput(left_tuple));
        left_join_count++;
        // left_done_[{key}] = true;
        // printf("doing left outer join\n");
      }
    }
    left_join_check_ = true;
    for (auto &entry : left_done_) {
      entry.second = true;
    }
    left_done_.erase(left_done_.begin(), left_done_.end());
    // printf("cleainf left done\n");
    // printf("count of left table: %d\n", count);
    // printf("count of left join: %d\n", left_join_count);
  }

  if (!queue_.empty()) {
    *tuple = queue_.front();
    queue_.pop();
    *rid = tuple->GetRid();
    // printf("emitting after left join scan\n");
    // printf("queue size: %lu\n", queue_.size());
    return true;
  }


  return false;
}

}  // namespace bustub
