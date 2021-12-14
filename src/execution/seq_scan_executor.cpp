//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      txn_(exec_ctx->GetTransaction()),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid())),
      table_heap_(table_info_->table_.get()),
      next_itr_(table_heap_->End()) {}

void SeqScanExecutor::Init() { next_itr_ = table_heap_->Begin(txn_); }

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  if (GetExecutorContext()->GetTransaction()->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(GetExecutorContext()->GetTransaction()->GetTransactionId(), AbortReason::DEADLOCK);
  }
  while (next_itr_ != table_heap_->End()) {
    Tuple cur_tuple;
    RID cur_rid = next_itr_->GetRid();
    auto lock_manager = GetExecutorContext()->GetLockManager();
    if (txn_->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && !txn_->IsSharedLocked(cur_rid) &&
        !txn_->IsExclusiveLocked(cur_rid)) {
      // LOG_DEBUG("%d sharedlock %s", txn_->GetTransactionId(), cur_rid.ToString().c_str());
      lock_manager->LockShared(txn_, cur_rid);
      // LOG_DEBUG("%d sharedlock %s sucess", txn_->GetTransactionId(), cur_rid.ToString().c_str());
    }
    if (!table_heap_->GetTuple(cur_rid, &cur_tuple, txn_)) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "get tuple fail");
    }
    bool pass = true;
    auto predicate = plan_->GetPredicate();
    if (predicate != nullptr) {
      pass = predicate->Evaluate(&cur_tuple, &table_info_->schema_).GetAs<bool>();
    }
    if (pass) {
      std::vector<Value> valus;
      for (auto &col : GetOutputSchema()->GetColumns()) {
        valus.push_back(col.GetExpr()->Evaluate(&cur_tuple, &table_info_->schema_));
      }
      *tuple = Tuple(valus, GetOutputSchema());
      *rid = next_itr_->GetRid();
      ++next_itr_;
      return true;
    }
    ++next_itr_;
  }
  return false;
}

}  // namespace bustub
