//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  // right_executor_->Init();
  // left_done_ = false;
  right_done_ = true;
  left_tuple_ = Tuple();
  right_tuple_ = Tuple();
  mathch_found_ = true;
  // left_done_ = !left_executor_->Next(&left_tuple_, &left_rid_);
  left_done_ = false;
  // printf("left done initialized to %d\n", left_done_);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // left_done_ = !left_executor_->Next(&left_tuple_, &left_rid_);
  while (!left_done_) {
    // init the right executor and update the right done variable
    if (right_done_) {
      // printf("init the right executor and update the right done variable\n");
      right_executor_->Init();
      right_done_ = false;
      mathch_found_ = false;
      right_done_ = !right_executor_->Next(&right_tuple_, &right_rid_);
      // printf("fetching the next left tuple\n");
      left_done_ = !left_executor_->Next(&left_tuple_, &left_rid_);
      // printf("next left tuple fetched, left_done: %d\n", left_done_);
      if (left_done_) {
        // printf("left done, %d\n", left_done_);
        // printf("right done, %d\n", right_done_);
        return false;
      }
    }

    // check if there is matching right tuple
    while (!right_done_) {
      // printf("start interating over all right tuples\n");
      if (plan_->predicate_ == nullptr || plan_->predicate_
                                              ->EvaluateJoin(&left_tuple_, plan_->GetLeftPlan()->OutputSchema(),
                                                             &right_tuple_, plan_->GetRightPlan()->OutputSchema())
                                              .GetAs<bool>()) {
        // printf("matching tuple found\n");
        std::vector<Value> values;
        // Append all values from the left tuple
        for (const auto &col : left_executor_->GetOutputSchema().GetColumns()) {
          values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(),
                                                left_executor_->GetOutputSchema().GetColIdx(col.GetName())));
        }
        // then the right tuple
        for (const auto &col : right_executor_->GetOutputSchema().GetColumns()) {
          values.push_back(right_tuple_.GetValue(&right_executor_->GetOutputSchema(),
                                                 right_executor_->GetOutputSchema().GetColIdx(col.GetName())));
        }
        *tuple = Tuple(values, &plan_->OutputSchema());
        right_done_ = !right_executor_->Next(&right_tuple_, &right_rid_);
        mathch_found_ = true;
        return true;
      }
      // printf("get next right tuple\n");
      right_done_ = !right_executor_->Next(&right_tuple_, &right_rid_);
    }
    if (!mathch_found_ && plan_->GetJoinType() == JoinType::LEFT && right_done_) {
      // printf("matching tuple not found, but left join here\n");
      std::vector<Value> values;
      // Append all values from the left tuple
      for (const auto &col : left_executor_->GetOutputSchema().GetColumns()) {
        values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(),
                                              left_executor_->GetOutputSchema().GetColIdx(col.GetName())));
      }
      // then the right tuple
      for (const auto &col : right_executor_->GetOutputSchema().GetColumns()) {
        values.push_back(ValueFactory::GetNullValueByType(col.GetType()));
      }
      *tuple = Tuple(values, &plan_->OutputSchema());
      mathch_found_ = true;
      return true;
    }
    // printf("left done, %d\n", left_done_);
    // printf("one left tuple done, get next left tuple\n");
    // left_done_ = !left_executor_->Next(&left_tuple_, &left_rid_);
  }
  // printf("left done, %d\n", left_done_);
  // printf("right done, %d\n", right_done_);
  // BUSTUB_ASSERT(left_done_ && right_done_, "both left and right are done");
  return false;
}

}  // namespace bustub