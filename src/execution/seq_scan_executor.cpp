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
      table_meta_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid())),
      table_heap_(table_meta_->table_.get()),
      next_itr_(table_heap_->End()),
      schema_(table_meta_->schema_) {}

void SeqScanExecutor::Init() {
  next_itr_ = table_heap_->Begin(txn_);
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (next_itr_ != table_heap_->End()) {
    bool pass = true;
    auto predicate = plan_->GetPredicate();
    if (predicate != nullptr) {
      pass = predicate->Evaluate(&*next_itr_, &schema_).GetAs<bool>();
    }
    if (pass) {
      std::vector<Value> valus;
      for (auto &col : GetOutputSchema()->GetColumns()) {
        valus.push_back(col.GetExpr()->Evaluate(&*next_itr_,&schema_));
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