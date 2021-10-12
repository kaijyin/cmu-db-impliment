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
      table_meta_data_(exec_ctx->GetCatalog()->GetTable(plan_->TableOid())),
      table_heap_(table_meta_data_->table_.get()),
      indexs_(GetExecutorContext()->GetCatalog()->GetTableIndexes(table_meta_data_->name_)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (child_executor_->Next(tuple, rid)) {
    auto lock_manager = GetExecutorContext()->GetLockManager();
    if (!lock_manager->LockExclusive(txn_, *rid)) {
      GetExecutorContext()->GetTransactionManager()->Abort(txn_);
      return false;
    }
    table_heap_->MarkDelete(*rid, txn_);
    txn_->AppendTableWriteRecord(TableWriteRecord(*rid, WType::DELETE, Tuple(), table_heap_));
    for (auto &index : indexs_) {
      Tuple index_tuple =
          tuple->KeyFromTuple(table_meta_data_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
      index->index_->InsertEntry(*tuple, *rid, txn_);
      txn_->AppendTableWriteRecord(IndexWriteRecord(*rid, table_meta_data_->oid_, WType::DELETE, *tuple,
                                                    index->index_oid_, GetExecutorContext()->GetCatalog()));
    }
    return true;
  }
  if (child_executor_->GetExecutorContext()->GetTransaction()->GetState() == TransactionState::ABORTED) {
    GetExecutorContext()->GetTransactionManager()->Abort(txn_);
  }
  return false;
}

}  // namespace bustub
