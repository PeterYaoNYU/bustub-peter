//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

// borrow from the aggregation executor code
#include "execution/executors/aggregation_executor.h"

namespace bustub {

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputing rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functionss and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */
class WindowFunctionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  // store all the tuples (computed, can be iterated over and given out as the final result)
  std::vector<Tuple> tuples_;

  // store the current tuple iterator
  std::vector<Tuple>::iterator current_tuple_iterator_;
};

class WindowAggregate {
 public:
  WindowAggregate(const Schema &child_schema, const Schema &output_schema, std::vector<Tuple> &result_tuples,
                  const std::vector<AbstractExpressionRef> &partition_by,
                  const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_by,
                  const std::vector<AbstractExpressionRef> &columns, uint32_t function_col_index,
                  std::unordered_set<uint32_t> &window_agg_indexes, const WindowFunctionType &window_agg_type,
                  AbstractExpressionRef agg_expr, bool no_order_by)
      : child_schema_(child_schema),
        output_schema_(output_schema),
        result_tuples_(result_tuples),
        partition_by_(partition_by),
        order_by_(order_by),
        columns_(columns),
        function_col_index_(function_col_index),
        window_agg_indexes_(window_agg_indexes),
        window_agg_type_(window_agg_type),
        agg_expr_(std::move(agg_expr)),
        no_order_by_(no_order_by) {}

  // I don't think that we need the begin_iter here in this function
  // keep it for now
  // borrow from the aggregation executor code
  // window function only support one type
  void ComputePartition(std::vector<Tuple>::iterator lower_bound_iter, std::vector<Tuple>::iterator upper_bound_iter,
                        std::vector<Tuple>::iterator begin_iter) {
    AggregationType aggregate_type;
    Value default_value;

    switch (window_agg_type_) {
      case WindowFunctionType::SumAggregate:
        aggregate_type = AggregationType::SumAggregate;
        default_value = ValueFactory::GetNullValueByType(TypeId::INTEGER);
        break;
      case WindowFunctionType::MinAggregate:
        aggregate_type = AggregationType::MinAggregate;
        default_value = ValueFactory::GetNullValueByType(TypeId::INTEGER);
        break;
      case WindowFunctionType::MaxAggregate:
        aggregate_type = AggregationType::MaxAggregate;
        default_value = ValueFactory::GetNullValueByType(TypeId::INTEGER);
        break;
      case WindowFunctionType::CountAggregate:
        aggregate_type = AggregationType::CountAggregate;
        default_value = ValueFactory::GetNullValueByType(TypeId::INTEGER);
        break;
      case WindowFunctionType::CountStarAggregate:
        aggregate_type = AggregationType::CountStarAggregate;
        default_value = ValueFactory::GetIntegerValue(0);
        break;
      case WindowFunctionType::Rank:
        aggregate_type = AggregationType::CountStarAggregate;
        default_value = ValueFactory::GetNullValueByType(TypeId::INTEGER);
        is_simple_agg_ = false;
        break;
    }

    // if there is no rank function involved
    if (is_simple_agg_) {
      // using the aggregation hash table to compute the aggregation
      // borrow from the aggregation executor code
      // construct the aggregation hash table
      std::vector<AbstractExpressionRef> agg_exprs = {agg_expr_};
      std::vector<AggregationType> agg_types = {aggregate_type};
      SimpleAggregationHashTable aht(agg_exprs, agg_types);
      aht.Clear();

      // the same partition shares the same aggregate key, actually only 1 row of the simple aggregation hash table is
      // used
      AggregateKey agg_key = MakeAggregateKey(&(*lower_bound_iter));

      // no_order_by_ = order_by_.empty();
      // printf("length of order by: %zu\n", order_by_.size());

      // if there is no order by, then for the same partition, the generate value should be the same
      if (no_order_by_) {
        for (auto iter = lower_bound_iter; iter != upper_bound_iter; iter++) {
          AggregateValue agg_val = MakeAggregateValue(&(*iter));
          aht.InsertCombine(agg_key, agg_val);
        }

        for (auto iter = lower_bound_iter; iter != upper_bound_iter; iter++) {
          // we are now changing the result set directly
          auto result_tuple_iter = result_tuples_.begin() + (iter - begin_iter);

          std::vector<Value> values;
          for (uint32_t idx = 0; idx < output_schema_.GetColumns().size(); idx++) {
            if (idx == function_col_index_) {
              values.emplace_back(aht.Begin().Val().aggregates_[0]);
            } else if (window_agg_indexes_.count(idx) == 0) {
              values.emplace_back(columns_[idx]->Evaluate(&(*iter), child_schema_));
            } else if (result_tuple_iter < result_tuples_.end()) {
              values.emplace_back(result_tuple_iter->GetValue(&output_schema_, idx));
            } else {
              values.emplace_back(default_value);
            }
          }

          if (result_tuple_iter < result_tuples_.end()) {
            *result_tuple_iter = Tuple(values, &output_schema_);
          } else {
            result_tuples_.emplace_back(values, &output_schema_);
          }
        }
      } else if (!no_order_by_) {
        printf("there is order by\n");
        // if there is order by, then we need to compute the aggregation for each row
        for (auto iter = lower_bound_iter; iter != upper_bound_iter; iter++) {
          AggregateValue agg_val = MakeAggregateValue(&(*iter));
          aht.InsertCombine(agg_key, agg_val);

          // we are now changing the result set directly
          auto result_tuple_iter = result_tuples_.begin() + (iter - begin_iter);

          std::vector<Value> values;
          for (uint32_t idx = 0; idx < output_schema_.GetColumns().size(); idx++) {
            if (idx == function_col_index_) {
              values.emplace_back(aht.Begin().Val().aggregates_[0]);
            } else if (window_agg_indexes_.count(idx) == 0) {
              values.emplace_back(columns_[idx]->Evaluate(&(*iter), child_schema_));
            } else if (result_tuple_iter < result_tuples_.end()) {
              values.emplace_back(result_tuple_iter->GetValue(&output_schema_, idx));
            } else {
              values.emplace_back(default_value);
            }
          }

          if (result_tuple_iter < result_tuples_.end()) {
            *result_tuple_iter = Tuple(values, &output_schema_);
          } else {
            result_tuples_.emplace_back(values, &output_schema_);
          }
        }
      }
    } else if (!is_simple_agg_) {
      // contains rank function
      int global_rank = 0;
      int local_rank = 0;

      for (auto iter = lower_bound_iter; iter != upper_bound_iter; iter++) {
        // we are now changing the result set directly
        auto result_tuple_iter = result_tuples_.begin() + (iter - begin_iter);

        std::vector<Value> values;
        for (uint32_t idx = 0; idx < output_schema_.GetColumns().size(); idx++) {
          if (idx == function_col_index_) {
            if (iter == lower_bound_iter) {
              values.emplace_back(ValueFactory::GetIntegerValue(1));
              global_rank = 1;
              local_rank = 1;
            } else {
              // if the current row is the same as the previous row
              if (Equal(*iter, *(iter - 1))) {
                global_rank += 1;
                values.emplace_back(ValueFactory::GetIntegerValue(local_rank));
              } else {
                global_rank += 1;
                local_rank = global_rank;
                values.emplace_back(ValueFactory::GetIntegerValue(local_rank));
              }
            }
          } else if (window_agg_indexes_.count(idx) == 0) {
            values.emplace_back(columns_[idx]->Evaluate(&(*iter), child_schema_));
          } else if (result_tuple_iter < result_tuples_.end()) {
            values.emplace_back(result_tuple_iter->GetValue(&output_schema_, idx));
          } else {
            values.emplace_back(default_value);
          }
        }

        if (result_tuple_iter < result_tuples_.end()) {
          *result_tuple_iter = Tuple(values, &output_schema_);
        } else {
          result_tuples_.emplace_back(values, &output_schema_);
        }
      }
    }
  }

 private:
  auto MakeAggregateKey(const Tuple *tuple) -> AggregateKey {
    std::vector<Value> values;
    for (const auto &group_by_expr : partition_by_) {
      // i am not sure if this should be the child schma here, or the output schema
      values.emplace_back(group_by_expr->Evaluate(tuple, child_schema_));
    }
    return {values};
  }

  auto MakeAggregateValue(const Tuple *tuple) -> AggregateValue {
    std::vector<Value> values;
    // i am not sure if this should be the child schma here, or the output schema
    values.emplace_back(agg_expr_->Evaluate(tuple, child_schema_));
    return {values};
  }

  auto Equal(const Tuple &a, const Tuple &b) -> bool {
    for (const auto &order_by : order_by_) {
      CmpBool cmp_result;
      cmp_result =
          order_by.second->Evaluate(&a, child_schema_).CompareEquals(order_by.second->Evaluate(&b, child_schema_));
      if (cmp_result == CmpBool::CmpFalse || cmp_result == CmpBool::CmpNull) {
        return false;
      }
    }
    return true;
  }

  const Schema &child_schema_;
  const Schema &output_schema_;
  std::vector<Tuple> &result_tuples_;
  const std::vector<AbstractExpressionRef> &partition_by_;
  const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_by_;
  const std::vector<AbstractExpressionRef> &columns_;

  uint32_t function_col_index_;
  std::unordered_set<uint32_t> window_agg_indexes_;

  // the aggregation function that this window is using
  // only support one type
  WindowFunctionType window_agg_type_;

  // if the window function is a simple aggregation
  bool is_simple_agg_ {true};

  AbstractExpressionRef agg_expr_;

  bool no_order_by_;
};

}  // namespace bustub
