//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() { child_executor_->Init(); }

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  if (GetExecutorContext()->GetTransaction()->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(GetExecutorContext()->GetTransaction()->GetTransactionId(), AbortReason::DEADLOCK);
  }
  while (true) {
    if (!child_executor_->Next(tuple, rid)) {
      return false;
    }
    std::vector<Value> cols;
    auto col_num = child_executor_->GetOutputSchema()->GetColumnCount();
    for (uint32_t i = 0; i < col_num; i++) {
      cols.push_back(tuple->GetValue(child_executor_->GetOutputSchema(), i));
    }
    if (dtb_.Insert(DistinctKey(cols))) {
      break;
    }
  }
  return true;
}

}  // namespace bustub
