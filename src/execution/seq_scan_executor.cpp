//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_heap_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  TableIterator iter = table_heap_->MakeIterator();
  rids_.clear();
  while (!iter.IsEnd()) {
    rids_.push_back(iter.GetRID());
    ++iter;
  }
  rids_iterator_ = rids_.begin();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  TupleMeta tuple_meta{};

  while (true) {
    if (rids_iterator_ == rids_.end()) {
      return false;
    }

    // first fetch the rid of this tuple
    *rid = *rids_iterator_;
    tuple_meta = table_heap_->GetTupleMeta(*rid);

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
      if (value.IsNull() || !value.GetAs<bool>()) {
        ++rids_iterator_;
        continue;
      }
    }

    // ok, we have a tuple that meets the predicate, so return it
    ++rids_iterator_;
    return true;
  }
}

}  // namespace bustub
