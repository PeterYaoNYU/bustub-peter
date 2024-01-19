#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
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

  using TupleHeap = std::priority_queue<Tuple, std::vector<Tuple>, decltype(comparator)>;
  TupleHeap heap(comparator);

  //   printf("planner size %lu\n", plan_->GetN());
  Tuple next_tuple;
  RID next_rid;
  while (child_executor_->Next(&next_tuple, &next_rid)) {
    // printf("from child executor %s\n", next_tuple.ToString(&child_executor_->GetOutputSchema()).c_str());

    heap.push(next_tuple);
    // printf("heap size: %lu\n", heap.size());
    if (heap.size() > plan_->GetN()) {
      heap.pop();
    }
  }

  //   printf("heap size: %lu\n", heap.size());
  top_entries_.reserve(heap.size());
  while (!heap.empty()) {
    top_entries_.push_back(heap.top());
    heap.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  //   printf("heap size: %lu\n", top_entries_.size());
  if (!top_entries_.empty()) {
    *tuple = top_entries_.back();
    // printf("topn executor emitting: %s\n", tuple->ToString(&child_executor_->GetOutputSchema()).c_str());
    *rid = tuple->GetRid();
    top_entries_.pop_back();
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return top_entries_.size(); };

}  // namespace bustub
