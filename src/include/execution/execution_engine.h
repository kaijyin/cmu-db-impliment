//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_engine.h
//
// Identification: src/include/execution/execution_engine.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "concurrency/transaction_manager.h"
#include "execution/executor_context.h"
#include "execution/executor_factory.h"
#include "execution/plans/abstract_plan.h"
#include "storage/table/tuple.h"
namespace bustub {
class ExecutionEngine {
 public:
  ExecutionEngine(BufferPoolManager *bpm, TransactionManager *txn_mgr, Catalog *catalog)
      : bpm_(bpm), txn_mgr_(txn_mgr), catalog_(catalog) {}

  DISALLOW_COPY_AND_MOVE(ExecutionEngine);

  bool Execute(const AbstractPlanNode *plan, std::vector<Tuple> *result_set, Transaction *txn,
               ExecutorContext *exec_ctx) {
    // construct executor
    auto executor = ExecutorFactory::CreateExecutor(exec_ctx, plan);

    // prepare
    executor->Init();

    // execute
    try {
      Tuple tuple;
      RID rid;
      while (executor->Next(&tuple, &rid)) {
        if (result_set != nullptr && executor->GetOutputSchema() != nullptr) {
          result_set->push_back(tuple);
        }
      }
    } catch (TransactionAbortException &e) {
      LOG_DEBUG("execute txn abort exception:%s", e.GetInfo().c_str());
      txn_mgr_->Abort(txn);
      return false;
    } catch (Exception &e) {
      LOG_DEBUG("execute exception!");
      txn_mgr_->Abort(txn);
      return false;
    }
    return true;
  }

 private:
  [[maybe_unused]] BufferPoolManager *bpm_;
  [[maybe_unused]] TransactionManager *txn_mgr_;
  [[maybe_unused]] Catalog *catalog_;
};

}  // namespace bustub
