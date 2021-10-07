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
      schema_(table_meta_data_->schema_) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (child_executor_->Next(tuple, rid)) {
    table_heap_->MarkDelete(*rid, txn_);
    auto indexs = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_meta_data_->name_);
    for (auto &index : indexs) {
    auto cur_index = index->index_.get();
    Tuple index_tuple = tuple->KeyFromTuple(schema_, *cur_index->GetKeySchema(), cur_index->GetKeyAttrs());
    cur_index->DeleteEntry(*tuple, *rid, txn_);
  }
    return true;
  }
  return false;
}

}  // namespace bustub
