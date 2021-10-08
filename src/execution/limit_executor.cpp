//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  child_executor_->Init();
  auto offset = plan_->GetOffset();
  remain = plan_->GetLimit();
  while (offset > 0U) {
    Tuple tuple;
    RID rid;
    if (!child_executor_->Next(&tuple, &rid)) {
      break;
    }
    offset--;
  }
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  if (remain == 0 || !child_executor_->Next(tuple, rid)) {
    return false;
  }
  remain--;
  return true;
}

}  // namespace bustub
