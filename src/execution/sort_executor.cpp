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

      CmpBool cmp_result;
      if (order_by.first == OrderByType::ASC || order_by.first == OrderByType::DEFAULT ||
          order_by.first == OrderByType::INVALID) {
        cmp_result = type->CompareLessThan(sort_key_expression->Evaluate(&a, this->child_executor_->GetOutputSchema()),
                                           sort_key_expression->Evaluate(&b, this->child_executor_->GetOutputSchema()));
        if (cmp_result == CmpBool::CmpTrue) {
          return true;
          // important thing, if the first round is the same, check the next predicate
        } else {
          cmp_result =
              type->CompareGreaterThan(sort_key_expression->Evaluate(&a, this->child_executor_->GetOutputSchema()),
                                       sort_key_expression->Evaluate(&b, this->child_executor_->GetOutputSchema()));
          if (cmp_result == CmpBool::CmpTrue) {
            return false;
          }
        }
      } else {  // OrderByType::DESC
        // I am not sure if Evaluate(&a, this->GetOutputSchema() is the correct expression
        cmp_result =
            type->CompareGreaterThan(sort_key_expression->Evaluate(&a, this->child_executor_->GetOutputSchema()),
                                     sort_key_expression->Evaluate(&b, this->child_executor_->GetOutputSchema()));
        if (cmp_result == CmpBool::CmpTrue) {
          return true;
        } else {
          cmp_result =
              type->CompareLessThan(sort_key_expression->Evaluate(&a, this->child_executor_->GetOutputSchema()),
                                    sort_key_expression->Evaluate(&b, this->child_executor_->GetOutputSchema()));
          if (cmp_result == CmpBool::CmpTrue) {
            return false;
          }
        }
      }
      // Continue to the next order_by if the current one results in equality
      if (cmp_result == CmpBool::CmpNull || cmp_result == CmpBool::CmpFalse) {
        continue;
      }
    }
    return false;
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
  } else {
    return false;
  }
}

}  // namespace bustub
