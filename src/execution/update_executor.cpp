//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      txn_(exec_ctx->GetTransaction()),
      child_txn_(child_executor->GetExecutorContext()->GetTransaction()),
      table_meta_data_(exec_ctx->GetCatalog()->GetTable(plan_->TableOid())),
      child_executor_(std::move(child_executor)),
      table_heap_(table_meta_data_->table_.get()),
      indexs_(GetExecutorContext()->GetCatalog()->GetTableIndexes(table_meta_data_->name_)) {}

void UpdateExecutor::Init() { child_executor_->Init(); }

bool UpdateExecutor::Next([[maybe_unused]] Tuple *old_tuple, RID *rid) {
  if (child_executor_->Next(old_tuple, rid)) {
    Tuple new_tuple = GenerateUpdatedTuple(*old_tuple);
    auto lock_manager = GetExecutorContext()->GetLockManager();
    if (!lock_manager->LockExclusive(txn_, *rid)) {
      GetExecutorContext()->GetTransactionManager()->Abort(txn_);
      return false;
    }
    bool update_sucess = table_heap_->UpdateTuple(new_tuple, *rid, txn_);
    if (!update_sucess) {
      GetExecutorContext()->GetTransactionManager()->Abort(txn_);
      return false;
    }
    txn_->AppendTableWriteRecord(TableWriteRecord(*rid, WType::UPDATE, *old_tuple, table_heap_));
    for (auto &index : indexs_) {
      Tuple old_index_tuple = old_tuple->KeyFromTuple(table_meta_data_->schema_, *index->index_->GetKeySchema(),
                                                      index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(old_index_tuple, *rid, txn_);
      Tuple new_index_tuple = old_tuple->KeyFromTuple(table_meta_data_->schema_, *index->index_->GetKeySchema(),
                                                      index->index_->GetKeyAttrs());
      index->index_->InsertEntry(new_index_tuple, *rid, txn_);
      IndexWriteRecord index_write_record =
          IndexWriteRecord(*rid, table_meta_data_->oid_, WType::UPDATE, new_index_tuple, index->index_oid_,
                           GetExecutorContext()->GetCatalog());
      index_write_record.old_tuple_ = *old_tuple;
      txn_->AppendTableWriteRecord(index_write_record);
    }
    return true;
  }
  if (child_txn_->GetState() == TransactionState::ABORTED) {
    GetExecutorContext()->GetTransactionManager()->Abort(txn_);
  }
  return false;
}
}  // namespace bustub
