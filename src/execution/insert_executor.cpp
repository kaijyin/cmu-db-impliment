//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      txn_(exec_ctx->GetTransaction()),
      table_meta_data_(exec_ctx->GetCatalog()->GetTable(plan_->TableOid())),
      table_heap_(table_meta_data_->table_.get()),
      indexs_(GetExecutorContext()->GetCatalog()->GetTableIndexes(table_meta_data_->name_)) {}

void InsertExecutor::Init() {
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (plan_->IsRawInsert()) {
    auto rows = plan_->RawValues();
    if (now_row == rows.size()) {
      return false;
    }
    auto &row = rows[now_row++];
    *tuple = Tuple(row, &table_meta_data_->schema_);
  } else {
    if (!child_executor_->Next(tuple, rid)) {
      if (child_executor_->GetExecutorContext()->GetTransaction()->GetState() == TransactionState::ABORTED) {
        GetExecutorContext()->GetTransactionManager()->Abort(txn_);
      }
      return false;
    }
  }
  bool insert_into_table = table_heap_->InsertTuple(*tuple, rid, txn_);
  if (!insert_into_table) {
    GetExecutorContext()->GetTransactionManager()->Abort(txn_);
    return false;
  }
  txn_->AppendTableWriteRecord(TableWriteRecord(*rid, WType::INSERT, Tuple(), table_heap_));
  for (auto &index : indexs_) {
    Tuple index_tuple =
        tuple->KeyFromTuple(table_meta_data_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
    index->index_->InsertEntry(index_tuple, *rid, txn_);
    txn_->AppendTableWriteRecord(IndexWriteRecord(*rid, table_meta_data_->oid_, WType::INSERT, *tuple,
                                                  index->index_oid_, GetExecutorContext()->GetCatalog()));
  }
  return true;
}

}  // namespace bustub
