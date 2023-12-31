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

  while (left_child_->Next(&left_tuple, &left_rid)) {
    auto key = MakeHashJoinLeftKey(&left_tuple, left_child_->GetOutputSchema());
    for (size_t i = 0; i < key.size(); i++) {
      auto left_key = key[i];
      if (hash_tables_[i].find(left_key) == hash_tables_[i].end()) {
        hash_tables_[i][left_key] = std::vector<Tuple>();
      }
      hash_tables_[i][left_key].push_back(left_tuple);
    }
  }
}

auto HashJoinExecutor::InnerJoinOutput(const Tuple &left, const Tuple &right) -> Tuple {
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
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
      return false;
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
    }

    if (!queue_.empty()) {
      *tuple = queue_.front();
      queue_.pop();
      *rid = tuple->GetRid();
      return true;
    }
  }
}

}  // namespace bustub
