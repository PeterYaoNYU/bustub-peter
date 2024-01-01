//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)){}

void LimitExecutor::Init() {
    child_executor_->Init();
    cnt_ = 0;
    // limit_ = plan_->GetLimit();
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cnt_ >= plan_->GetLimit()) {
    return false;
  }

  if (!child_executor_->Next(tuple, rid)) {
    return false;
  }

  cnt_++;
//   printf("limit executor emitting: %s\n", tuple->ToString(&child_executor_->GetOutputSchema()).c_str());

  return true;
//     if (limit_ == 0) {
//         return false;
//     }
//     if (child_executor_->Next(tuple, rid)) {
//         printf("limit executor emitting: %s\n", tuple->ToString(&child_executor_->GetOutputSchema()).c_str());
//         limit_--;
//         return true;
//     }
//     return false;
}
}  // namespace bustub
