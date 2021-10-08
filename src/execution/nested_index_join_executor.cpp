//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      txn_(exec_ctx->GetTransaction()),
      table_meta_data_(exec_ctx->GetCatalog()->GetTable(plan_->GetInnerTableOid())),
      table_heap_(table_meta_data_->table_.get()),
      index_info_(exec_ctx->GetCatalog()->GetIndex(plan_->GetIndexName(), table_meta_data_->name_)),
      index_(reinterpret_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_info_->index_.get())) {}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple outer_tuple;
  Tuple inner_tuple;
  RID outer_rid;
  while (true) {
    if (child_executor_->Next(&outer_tuple, &outer_rid)) {
      std::vector<RID> result;
      Tuple key_tuple =
          outer_tuple.KeyFromTuple(*plan_->OuterTableSchema(), *index_->GetKeySchema(), index_->GetKeyAttrs());
      index_->ScanKey(key_tuple, &result, txn_);
      if (!result.empty()) {
        if (!table_heap_->GetTuple(result[0], &inner_tuple, txn_)) {
          return false;
        }
        break;
      }
    } else {
      return false;
    }
  }
  std::vector<Value> valus;
  for (auto &col : GetOutputSchema()->GetColumns()) {
    valus.push_back(
        col.GetExpr()->EvaluateJoin(&inner_tuple, plan_->InnerTableSchema(), &outer_tuple, plan_->OuterTableSchema()));
  }
  *tuple = Tuple(valus, GetOutputSchema());
  return true;
}

}  // namespace bustub
