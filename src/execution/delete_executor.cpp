//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  table_heap_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  indexes_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  auto table_name = table_info_->name_;
  Transaction *tx = GetExecutorContext()->GetTransaction();
  TupleMeta tuple_meta = {.ts_ = tx->GetTransactionTempTs(), .is_deleted_ = true};
  Schema schema = GetExecutorContext()->GetCatalog()->GetTable(table_name)->schema_;

  // printf("DeleteExecutor::Next\n");
  int count = 0;
  while (true) {
    Tuple child_tuple{};
    auto status = child_executor_->Next(&child_tuple, rid);
    if (!status) {
      break;
    }
    // printf("child output got\n");
    table_heap_->UpdateTupleMeta(tuple_meta, *rid);
    count++;
    // printf("tuple meta updated\n");

    for (auto &index_info : indexes_) {
      index_info->index_->DeleteEntry(
          child_tuple.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs()), *rid, tx);
    }
    // printf("index updated\n");
  }
  // printf("returning\n");
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(INTEGER, count);
  *tuple = Tuple{values, &GetOutputSchema()};

  if (count == 0 and !called_) {
    called_ = true;
    return true;
  }
  called_ = true;
  // printf("returning %d\n", count!=0);
  return count != 0;
}

}  // namespace bustub
