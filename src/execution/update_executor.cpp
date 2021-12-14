//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
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
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->TableOid())),
      child_executor_(std::move(child_executor)),
      table_heap_(table_info_->table_.get()),
      indexs_(GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_)) {}

void UpdateExecutor::Init() { child_executor_->Init(); }

bool UpdateExecutor::Next([[maybe_unused]] Tuple *old_tuple, RID *rid) {
  if (GetExecutorContext()->GetTransaction()->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(GetExecutorContext()->GetTransaction()->GetTransactionId(), AbortReason::DEADLOCK);
  }
  if (child_executor_->Next(old_tuple, rid)) {
    Tuple new_tuple = GenerateUpdatedTuple(*old_tuple);
    auto lock_manager = GetExecutorContext()->GetLockManager();
    if (txn_->IsSharedLocked(*rid)) {
      lock_manager->LockUpgrade(txn_, *rid);
    } else if (!txn_->IsExclusiveLocked(*rid)) {
      lock_manager->LockExclusive(txn_, *rid);
    }
    // write record 已经在update中添加了
    if (!table_heap_->UpdateTuple(new_tuple, *rid, txn_)) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "update tuple fail");
    }
    for (auto &index : indexs_) {
      Tuple old_index_tuple =
          old_tuple->KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(old_index_tuple, *rid, txn_);
      Tuple new_index_tuple =
          new_tuple.KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
      index->index_->InsertEntry(new_index_tuple, *rid, txn_);
      IndexWriteRecord index_write_record = IndexWriteRecord(*rid, table_info_->oid_, WType::UPDATE, new_tuple,
                                                             index->index_oid_, GetExecutorContext()->GetCatalog());
      index_write_record.old_tuple_ = *old_tuple;
      txn_->AppendTableWriteRecord(index_write_record);
    }
    return true;
  }
  return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
