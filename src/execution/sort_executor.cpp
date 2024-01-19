#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();

  // this is important for repeated queries:
  sorted_tuples_.clear();

  Tuple tuple;
  RID rid;
  current_tuple_idx_ = 0;
  while (child_executor_->Next(&tuple, &rid)) {
    sorted_tuples_.push_back(tuple);
  }

  auto comparator = [this](const Tuple &a, const Tuple &b) {
    for (const auto &order_by : plan_->GetOrderBy()) {
      const auto &sort_key_expression = order_by.second;
      const auto &sort_key_type = sort_key_expression->GetReturnType();
      const Type *type = Type::GetInstance(sort_key_type);

      Value a_val = sort_key_expression->Evaluate(&a, this->child_executor_->GetOutputSchema());
      Value b_val = sort_key_expression->Evaluate(&b, this->child_executor_->GetOutputSchema());

      if (order_by.first == OrderByType::ASC || order_by.first == OrderByType::DEFAULT ||
          order_by.first == OrderByType::INVALID) {
        if (type->CompareLessThan(a_val, b_val) == CmpBool::CmpTrue) {
          return true;
        }
        if (type->CompareGreaterThan(a_val, b_val) == CmpBool::CmpTrue) {
          return false;
        }
      } else {  // OrderByType::DESC
        if (type->CompareGreaterThan(a_val, b_val) == CmpBool::CmpTrue) {
          return true;
        }
        if (type->CompareLessThan(a_val, b_val) == CmpBool::CmpTrue) {
          return false;
        }
      }
      // Continue to the next order_by if the current one results in equality
      // No need for an explicit check here, the loop will continue automatically
    }
    return false;  // Return false if all comparisons resulted in equality
  };

  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(), comparator);
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (current_tuple_idx_ < sorted_tuples_.size()) {
    *tuple = sorted_tuples_[current_tuple_idx_];
    *rid = tuple->GetRid();
    // printf("Sort executor emitting tuple: %s\n", tuple->ToString(&child_executor_->GetOutputSchema()).c_str());
    current_tuple_idx_++;
    return true;
  }

  return false;
}

}  // namespace bustub
