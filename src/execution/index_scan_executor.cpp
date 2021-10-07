//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      txn_(exec_ctx->GetTransaction()),
      index_info_(exec_ctx->GetCatalog()->GetIndex(plan_->GetIndexOid())),
      table_meta_data_(exec_ctx->GetCatalog()->GetTable(index_info_->table_name_)),
      table_heap_(table_meta_data_->table_.get()),
      index_(reinterpret_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_info_->index_.get())),
      next_itr_(index_->GetEndIterator()),
      schema_(table_meta_data_->schema_) {}

void IndexScanExecutor::Init() {
  next_itr_ = index_->GetBeginIterator();
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple cur_tuple;
  RID cur_rid;
  while (!next_itr_.isEnd()) {
    bool pass = true;
    auto predicate = plan_->GetPredicate();
    if (predicate != nullptr) {
      cur_rid = (*next_itr_).second;
      table_heap_->GetTuple(cur_rid, &cur_tuple, txn_);
      pass = predicate->Evaluate(&cur_tuple, &schema_).GetAs<bool>();
    }
    if (pass) {
      std::vector<Value> valus;
      for (auto &col:GetOutputSchema()->GetColumns()) {
        valus.push_back(col.GetExpr()->Evaluate(&cur_tuple,&schema_));
      }
      *tuple = Tuple(valus, GetOutputSchema());
      *rid = cur_rid;
      ++next_itr_;
      return true;
    }
    ++next_itr_;
  }
  return false;
}

}  // namespace bustub
