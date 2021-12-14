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
      txn_(exec_ctx->GetTransaction()),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan_->TableOid())),
      table_heap_(table_info_->table_.get()),
      indexs_(GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_)) {}

void InsertExecutor::Init() {
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *insert_tuple, RID *rid) {
  if (GetExecutorContext()->GetTransaction()->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(GetExecutorContext()->GetTransaction()->GetTransactionId(), AbortReason::DEADLOCK);
  }
  if (plan_->IsRawInsert()) {
    auto rows = plan_->RawValues();
    if (now_row_ == rows.size()) {
      return false;
    }
    auto &row = rows[now_row_++];
    *insert_tuple = Tuple(row, &table_info_->schema_);
  } else {
    if (!child_executor_->Next(insert_tuple, rid)) {
      return false;
    }
  }
  RID insert_rid;
  // 无需添加write record,table_heap已经添加
  bool insert_into_table = table_heap_->InsertTuple(*insert_tuple, &insert_rid, txn_);
  if (!insert_into_table) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "insert tuple faild");
  }
  auto lock_manager = GetExecutorContext()->GetLockManager();
  // rid是新生成的,但是需要去持有互斥锁,因为可能生成之后马上有线程去持有共享锁
  // LOG_DEBUG("%d want exclusive %s", txn_->GetTransactionId(), insert_rid.ToString().c_str());
  lock_manager->LockExclusive(txn_, insert_rid);
  // LOG_DEBUG("%d want exclusvie %s sucess", txn_->GetTransactionId(), insert_rid.ToString().c_str());
  for (auto &index : indexs_) {
    Tuple index_tuple =
        insert_tuple->KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
    index->index_->InsertEntry(index_tuple, insert_rid, txn_);
    txn_->AppendTableWriteRecord(IndexWriteRecord(insert_rid, table_info_->oid_, WType::INSERT, *insert_tuple,
                                                  index->index_oid_, GetExecutorContext()->GetCatalog()));
  }
  return true;
}

}  // namespace bustub
