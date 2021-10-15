//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      txn_(exec_ctx->GetTransaction()),
      child_executor_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.End()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_executor_.get(); }

void AggregationExecutor::Init() {
  child_executor_->Init();
  Tuple cur_tuple;
  RID cur_rid;
  while (child_executor_->Next(&cur_tuple, &cur_rid)) {
    AggregateKey key = MakeKey(&cur_tuple);
    AggregateValue val = MakeVal(&cur_tuple);
    aht_.InsertCombine(key, val);
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (aht_iterator_ != aht_.End()) {
    auto &group_bys = aht_iterator_.Key().group_bys_;
    auto &aggregates = aht_iterator_.Val().aggregates_;
    auto having = plan_->GetHaving();
    if (having != nullptr) {
      if (!having->EvaluateAggregate(group_bys, aggregates).GetAs<bool>()) {
        ++aht_iterator_;
        continue;
      }
    }
    std::vector<Value> values;
    for (auto &col : GetOutputSchema()->GetColumns()) {
      values.emplace_back(col.GetExpr()->EvaluateAggregate(group_bys, aggregates));
    }
    *tuple = Tuple(values, GetOutputSchema());
    ++aht_iterator_;
    return true;
  }
  return false;
}
// namespace bustub
}  // namespace bustub
