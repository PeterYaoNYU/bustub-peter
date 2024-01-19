#include "execution/executors/window_function_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  child_executor_->Init();

  Tuple tuple;
  RID rid;
  std::vector<Tuple> child_tuples;
  while (child_executor_->Next(&tuple, &rid)) {
    child_tuples.push_back(tuple);
  }

  // here we reuse the code from the sort executor, but here we need to provide the order by clause explicitly
  // instead of relying on the plan node itself
  // given that a sql statement may contain multiple window functions
  auto comparator = [this](const Tuple &a, const Tuple &b,
                           const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys,
                           const Schema &chema) {
    for (const auto &order_by : order_bys) {
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

  std::unordered_set<uint32_t> window_agg_indexes;
  for (const auto &[index, window_function] : plan_->window_functions_) {
    window_agg_indexes.insert(index);
  }

  for (auto &[function_col_index, window_function] : plan_->window_functions_) {
    std::vector<std::pair<OrderByType, AbstractExpressionRef>> total_order_bys;
    std::vector<std::pair<OrderByType, AbstractExpressionRef>> partition_order_bys;
    for (const auto &partition_by : window_function.partition_by_) {
      partition_order_bys.emplace_back(OrderByType::ASC, partition_by);
      total_order_bys.emplace_back(OrderByType::ASC, partition_by);
    }

    for (const auto &order_by : window_function.order_by_) {
      total_order_bys.push_back(order_by);
    }

    std::sort(child_tuples.begin(), child_tuples.end(),
              [order_bys = total_order_bys, schema = child_executor_->GetOutputSchema(), comparator](
                  const Tuple &a, const Tuple &b) { return comparator(a, b, order_bys, schema); });

    auto group_bys = window_function.partition_by_;
    auto agg_expr = window_function.function_;

    WindowAggregate window_agg{child_executor_->GetOutputSchema(),
                               plan_->OutputSchema(),
                               tuples_,
                               group_bys,
                               total_order_bys,
                               plan_->columns_,
                               function_col_index,
                               window_agg_indexes,
                               window_function.type_,
                               agg_expr,
                               window_function.order_by_.empty()};

    auto iter = child_tuples.begin();
    while (iter != child_tuples.end()) {
      auto upper_bound_iter =
          std::upper_bound(iter, child_tuples.end(), *iter,
                           [order_bys = partition_order_bys, schema = child_executor_->GetOutputSchema(), comparator](
                               const Tuple &a, const Tuple &b) { return comparator(a, b, order_bys, schema); });
      window_agg.ComputePartition(iter, upper_bound_iter, child_tuples.begin());
      iter = upper_bound_iter;
    }
  }

  current_tuple_iterator_ = tuples_.begin();
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (current_tuple_iterator_ != tuples_.end()) {
    *tuple = *current_tuple_iterator_;
    *rid = RID();
    ++current_tuple_iterator_;
    return true;
  }
  return false;
}
}  // namespace bustub
