//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {}

void DistinctExecutor::Init() {}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple cur_tuple;
  while (true) {
    if (!child_executor_->Next(&cur_tuple, rid)) {
      return false;
    }
    std::vector<Value> cols;
    auto col_num=child_executor_->GetOutputSchema()->GetColumnCount();
    for(uint32_t i=0;i<col_num;i++) {
         cols.push_back(cur_tuple.GetValue(child_executor_->GetOutputSchema(),i));
     }
     if(dtb_.Insert(DistinctKey(cols))){
         break;
     }
  }
  std::vector<Value> valus;
  for (auto &col : GetOutputSchema()->GetColumns()) {
    valus.push_back(col.GetExpr()->Evaluate(&cur_tuple, child_executor_->GetOutputSchema()));
  }
  *tuple = Tuple(valus, GetOutputSchema());
  return true;
}

}  // namespace bustub
