//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  child_executor_->Init();
  remain = plan_->GetLimit();
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple cur_tuple;
  if (remain == 0 || !child_executor_->Next(&cur_tuple, rid)) {
    return false;
  }
  std::vector<Value> valus;
  for (auto &col : GetOutputSchema()->GetColumns()) {
    valus.push_back(col.GetExpr()->Evaluate(&cur_tuple, child_executor_->GetOutputSchema()));
  }
  *tuple = Tuple(valus, GetOutputSchema());
  remain--;
  return true;
}

}  // namespace bustub
