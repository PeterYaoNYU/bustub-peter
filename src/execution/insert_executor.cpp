//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  // printf("InsertExecutor::Init\n");
  child_executor_->Init();
  table_name_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->name_;
  table_heap_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  indexes_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_name_);
  called = false;
  // printf("InsertExecutor::Init end\n");
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Transaction *tx = GetExecutorContext()->GetTransaction();
  LockManager *lock_manager = GetExecutorContext()->GetLockManager();
  TupleMeta tuple_meta = {.is_deleted_ = false, .ts_ = tx->GetTransactionTempTs()};
  Schema schema = GetExecutorContext()->GetCatalog()->GetTable(table_name_)->schema_;

  int count = 0;

  Tuple child_tuple{};
  // printf("InsertExecutor::Next\n");

  while (true) {
    // printf("count: %d\n", count);
    auto status = child_executor_->Next(&child_tuple, rid);
    if (!status) {
      break;
    }
    auto rid_optional = table_heap_->InsertTuple(tuple_meta, child_tuple, lock_manager, tx);
    if (rid_optional.has_value()) {
      *rid = rid_optional.value();
    } else {
      return false;
    }
    for (auto &index_info : indexes_) {
      index_info->index_->InsertEntry(
          child_tuple.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs()), *rid, tx);
    }
    count++;
  }

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
