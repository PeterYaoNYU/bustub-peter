//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  table_heap_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  indexes_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  auto table_name_ = table_info_->name_;
  Transaction *tx = GetExecutorContext()->GetTransaction();
  TupleMeta tuple_meta = {.is_deleted_ = false, .ts_ = tx->GetTransactionTempTs()};
  Schema schema = GetExecutorContext()->GetCatalog()->GetTable(table_name_)->schema_;

  // keep a track of how many rows have been updated
  int count = 0;

  while (true){
    // this child_tuple is important,keep a copy of the old tuple, so that we can delete it from the index
    Tuple child_tuple{};
    auto status = child_executor_->Next(&child_tuple, rid);
    if (!status) {
      break;
    }
    std::vector<Value> values;
    // reserve the right number of outputs
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    for (const auto &update_clause : plan_->target_expressions_) {
      values.push_back(update_clause->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    bool updated = table_heap_->UpdateTupleInPlace(tuple_meta, Tuple{values, &child_executor_->GetOutputSchema()}, *rid);
    if (updated){
      for (auto &index_info : indexes_) {
        index_info->index_->DeleteEntry(child_tuple.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs()), *rid, tx);
        // printf("assertion pending\n");
        assert(values.size() == child_executor_->GetOutputSchema().GetColumnCount());
        // printf("Assertion passed\n");
        index_info->index_->InsertEntry(Tuple{values, &child_executor_->GetOutputSchema()}.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs()), *rid, tx);
      }
    } else if (!updated){
      return false;
    }
    count++;
  }

  // printf("count: %d, while loop finished\n", count);
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(INTEGER, count);
  *tuple = Tuple{values, &GetOutputSchema()};
  if (count == 0 and !called) {
    called = true;
    return true;
  }
  called = true;
  return count != 0;
}

}  // namespace bustub
