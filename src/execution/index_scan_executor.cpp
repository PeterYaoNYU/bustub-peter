//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  // get the raw pointer to the table heap
  table_heap_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  auto index_info_ = GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexOid());
  htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());

  // do the actual scan
  rids_.clear();

  // printf("init index scan\n");
  if (plan_->pred_key_ != nullptr) {
    Value v = plan_->pred_key_->val_;
    std::vector<Value> values;
    values.push_back(v);
    htable_->ScanKey(Tuple(values, &index_info_->key_schema_), &rids_, GetExecutorContext()->GetTransaction());
    // printf("index scan finished, rids size is %zu\n", rids_.size());
  }

  rids_iterator_ = rids_.begin();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (rids_iterator_ == rids_.end()) {
      break;
    }

    // first fetch the rid of this tuple
    *rid = *rids_iterator_;
    TupleMeta tuple_meta = table_heap_->GetTupleMeta(*rid);

    // if the tuple is already marked deleted, do not return it
    if (tuple_meta.is_deleted_) {
      ++rids_iterator_;
      continue;
    }

    // ok, it is not deleted, so retrieve it
    *tuple = table_heap_->GetTuple(*rid).second;

    // if the tuple that the cursor is pointing to does not meet the predicate, do not return it
    if (plan_->filter_predicate_ != nullptr) {
      auto value = plan_->filter_predicate_->Evaluate(tuple, plan_->OutputSchema());
      if (value.IsNull() || value.GetAs<bool>() == false) {
        ++rids_iterator_;
        continue;
      }
    }

    ++rids_iterator_;
    return true;
  }
  return false;
}

}  // namespace bustub
