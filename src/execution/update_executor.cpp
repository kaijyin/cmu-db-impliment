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
      child_executor_(std::move(child_executor)),
      txn_(exec_ctx->GetTransaction()),
      table_heap_(exec_ctx->GetCatalog()->GetTable(plan_->TableOid())->table_.get()) {}

void UpdateExecutor::Init() { child_executor_->Init(); }

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple cur_tuple;
  if (child_executor_->Next(&cur_tuple, rid)) {
    Tuple new_tuple = GenerateUpdatedTuple(cur_tuple);
    table_heap_->UpdateTuple(new_tuple, *rid, txn_);
    return true;
  }
  return false;
}
}  // namespace bustub
