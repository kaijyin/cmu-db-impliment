//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      txn_(exec_ctx->GetTransaction()),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  left_executor_->Next(&left_tuple, &left_rid);
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (left_rid.GetPageId() == INVALID_PAGE_ID) {
    return false;
  }
  Tuple right_tuple;
  RID right_rid;
  bool res = false;
  while (!res) {
    while (!right_executor_->Next(&right_tuple, &right_rid)) {
      if (!left_executor_->Next(&left_tuple, &left_rid)) {
        return false;
      }
      right_executor_->Init();
    }
    auto predicate = plan_->Predicate();
    res = predicate
              ->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                             right_executor_->GetOutputSchema())
              .GetAs<bool>();
  }
  std::vector<Value> valus;
  for (auto &col : GetOutputSchema()->GetColumns()) {
    valus.push_back(col.GetExpr()->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                                                right_executor_->GetOutputSchema()));
  }
  *tuple = Tuple(valus, GetOutputSchema());
  return true;
}

}  // namespace bustub
