//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      txn_(exec_ctx->GetTransaction()),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->TableOid())),
      table_heap_(table_info_->table_.get()),
      indexs_(GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (GetExecutorContext()->GetTransaction()->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(GetExecutorContext()->GetTransaction()->GetTransactionId(), AbortReason::DEADLOCK);
  }
  if (child_executor_->Next(tuple, rid)) {
    auto lock_manager = GetExecutorContext()->GetLockManager();
    if (txn_->IsSharedLocked(*rid)) {
      // LOG_DEBUG("%d want upgrade %s", txn_->GetTransactionId(), rid->ToString().c_str());
      lock_manager->LockUpgrade(txn_, *rid);
      // LOG_DEBUG("%d upgrade %s sucess", txn_->GetTransactionId(), rid->ToString().c_str());
    } else if (!txn_->IsExclusiveLocked(*rid)) {
      // LOG_DEBUG("%d want exclusive %s", txn_->GetTransactionId(), rid->ToString().c_str());
      lock_manager->LockExclusive(txn_, *rid);
      // LOG_DEBUG("%d exclusive %s sucess", txn_->GetTransactionId(), rid->ToString().c_str());
    }
    if (!table_heap_->MarkDelete(*rid, txn_)) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "mark delete fail");
    }
    for (auto &index : indexs_) {
      Tuple index_tuple =
          tuple->KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(index_tuple, *rid, txn_);
      txn_->AppendTableWriteRecord(IndexWriteRecord(*rid, table_info_->oid_, WType::DELETE, *tuple, index->index_oid_,
                                                    GetExecutorContext()->GetCatalog()));
    }
    return true;
  }
  return false;
}

}  // namespace bustub
