//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  // this Clear is very important, otherwise leads to problems in repeated subquery
  // like this: select * from __mock_table_123, (select count(*) as cnt from t1);
  aht_.Clear();
  child_executor_->Init();

  // Iterate over all tuples from the child executor
  Tuple tuple;
  RID rid;

  while (child_executor_->Next(&tuple, &rid)) {
    // Make an aggregate key and value for the tuple
    auto agg_key = MakeAggregateKey(&tuple);
    auto agg_val = MakeAggregateValue(&tuple);

    // Insert and combine the values in the hash table
    aht_.InsertCombine(agg_key, agg_val);
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // remember to handle the corner cases where the table is empty
  if (aht_iterator_ == aht_.End()) {
    if (called_) {
      return false;
    }
  }

  // handling the corner case where the table is empty
  // per the instruction from the assingment pdf
  // Hint: When performing aggregation on an empty table, CountStarAggregate should return zero and all other aggregate
  // types should return integer_null. This is why GenerateInitialAggregateValue initializes most aggregate values as
  // NULL.
  if (aht_.Begin() == aht_.End()) {
    // printf("the table is empty\n");
    std::vector<Value> values;
    // don't forget the group by columns
    if (!plan_->GetGroupBys().empty()) {
      return false;
    }

    for (const auto &group_by_expr : plan_->GetGroupBys()) {
      values.emplace_back(ValueFactory::GetNullValueByType(group_by_expr->GetReturnType()));
    }

    for (const auto &agg_type : plan_->agg_types_) {
      switch (agg_type) {
        case AggregationType::CountStarAggregate:
          // For COUNT, return 0 if there are no tuples
          values.emplace_back(ValueFactory::GetIntegerValue(0));
          break;
        default:
          // For other aggregates, return NULL if there are no tuples
          values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
          break;
      }
    }
    // printf("empty table: values.size() = %zu\n", values.size());
    // printf("empty table: GetOutputSchema().GetColumnCount() = %u\n", GetOutputSchema().GetColumnCount());
    *tuple = Tuple(values, &GetOutputSchema());
    *rid = RID();
    called_ = true;
    return true;  // Return true for the first call, then set to end
  }

  // Get the current aggregated value
  const auto &agg_val = aht_iterator_.Val();

  // Create a tuple from the aggregated value
  std::vector<Value> values;
  for (const auto &val : agg_val.aggregates_) {
    values.push_back(val);
  }

  // Include group-by values if present
  if (!plan_->GetGroupBys().empty()) {
    const auto &agg_key = aht_iterator_.Key();
    values.insert(values.begin(), agg_key.group_bys_.begin(), agg_key.group_bys_.end());
  }
  //   printf("values.size() = %zu\n", values.size());
  //   printf("GetOutputSchema().GetColumnCount() = %u\n", GetOutputSchema().GetColumnCount());
  //   BUSTUB_ASSERT(values.size() == GetOutputSchema().GetColumnCount(), "The size of the values is not correct");
  // Create a new tuple with the aggregated values
  *tuple = Tuple(values, &GetOutputSchema());
  *rid = RID();  // Aggregations do not have a meaningful RID

  // Move to the next group
  ++aht_iterator_;
  called_ = true;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
