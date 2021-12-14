//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_executor,
                                   std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      txn_(exec_ctx->GetTransaction()),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  auto left_expr = plan_->LeftJoinKeyExpression();
  Tuple tuple;
  RID rid;
  while (left_executor_->Next(&tuple, &rid)) {
    auto key_val = left_expr->Evaluate(&tuple, left_executor_->GetOutputSchema());
    jht_.InsertCombine(HashjoinKey(key_val), tuple);
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (GetExecutorContext()->GetTransaction()->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(GetExecutorContext()->GetTransaction()->GetTransactionId(), AbortReason::DEADLOCK);
  }
  if (cur_idx_ == cur_tuples_.size()) {
    while (true) {
      if (!right_executor_->Next(&right_tuple_, &right_rid_)) {
        return false;
      }
      auto right_expr = plan_->RightJoinKeyExpression();
      auto cur_key = right_expr->Evaluate(&right_tuple_, right_executor_->GetOutputSchema());
      cur_tuples_ = jht_.GetTuples(HashjoinKey(cur_key)).tuples_;
      cur_idx_ = 0;
      if (!cur_tuples_.empty()) {
        break;
      }
    }
  }
  Tuple left_tuple = cur_tuples_[cur_idx_++];
  std::vector<Value> valus;
  for (auto &col : GetOutputSchema()->GetColumns()) {
    valus.push_back(col.GetExpr()->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple_,
                                                right_executor_->GetOutputSchema()));
  }
  *tuple = Tuple(valus, GetOutputSchema());
  return true;
}

}  // namespace bustub
