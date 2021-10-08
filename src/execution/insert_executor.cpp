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
      schema_(table_meta_data_->schema_) {}

void InsertExecutor::Init() {
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple cur_tuple;
  RID cur_rid;
  if (plan_->IsRawInsert()) {
    auto rows = plan_->RawValues();
    if (now_row == rows.size()) {
      return false;
    }
    auto &row = rows[now_row++];
    cur_tuple = Tuple(row, &schema_);
  } else {
    if (!child_executor_->Next(&cur_tuple, &cur_rid)) {
      return false;
    }
  }
  bool insert_into_table = table_heap_->InsertTuple(cur_tuple, &cur_rid, txn_);
  if (!insert_into_table) {
    return false;
  }
  auto indexs = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_meta_data_->name_);
  for (auto &index : indexs) {
    auto cur_index = index->index_.get();
    Tuple index_tuple = cur_tuple.KeyFromTuple(schema_, *cur_index->GetKeySchema(), cur_index->GetKeyAttrs());
    cur_index->InsertEntry(index_tuple, cur_rid, txn_);
  }
  return true;
}

}  // namespace bustub
