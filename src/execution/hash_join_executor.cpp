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
  // printf("hash join executor constructor\n");
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

  int left_table_size = 0;

  while (left_child_->Next(&left_tuple, &left_rid)) {
    left_table_size++;
    std::vector<HashJoinKey> key = MakeHashJoinLeftKey(&left_tuple, left_child_->GetOutputSchema());
    if (hash_table_.find({key}) == hash_table_.end()) {
      hash_table_[{key}] = std::vector<Tuple>{};
    }
    hash_table_[{key}].push_back(left_tuple);

    // update the left_done_ map
    CompositeJoinKey composite_join_key;
    composite_join_key.keys_ = key;
    left_done_[composite_join_key] = false;

    // printf("%s\n", left_tuple.ToString(&left_child_->GetOutputSchema()).c_str());
  }
  // printf("done building left table, left table size is %d\n", left_table_size);
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
  // return Tuple(values, &GetOutputSchema());
  return {values, &GetOutputSchema()};
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
  // return Tuple(values, &GetOutputSchema());
  return {values, &GetOutputSchema()};
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

    std::vector<Tuple> left_tuple_candidates;
    if (hash_table_.find({key}) != hash_table_.end()) {
      left_tuple_candidates = hash_table_[{key}];
    }

    for (auto &match : left_tuple_candidates) {
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
    for (const auto &iter : left_done_) {
      if (!iter.second) {
        for (const auto &left_tuple : hash_table_[iter.first]) {
          queue_.push(LeftOuterJoinOutput(left_tuple));
        }
      }
    }
    left_join_check_ = true;
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
