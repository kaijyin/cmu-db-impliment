//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.h
//
// Identification: src/include/execution/executors/distinct_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "container/hash/hash_function.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/distinct_plan.h"

namespace bustub {
struct DistinctKey {
  std::vector<Value> cols_;
  explicit DistinctKey(std::vector<Value> other_cols) : cols_(std::move(other_cols)) {}
  bool operator==(const DistinctKey &other) const {
    for (size_t i = 0; i < cols_.size(); i++) {
      if (cols_[i].CompareEquals(other.cols_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace bustub
namespace std {
/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::DistinctKey> {
  std::size_t operator()(const bustub::DistinctKey &hash_key) const {
    size_t curr_hash = 0;
    for (const auto &key : hash_key.cols_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};
}  // namespace std
namespace bustub {
class SimpleDistinctHashTable {
 public:
  bool Insert(const DistinctKey &key) {
    if (dset_.count(key) == 0) {
      dset_.insert(key);
      return true;
    }
    return false;
  }

 private:
  std::unordered_set<DistinctKey> dset_;
};

/**
 * DistinctExecutor removes duplicate rows from child ouput.
 */
class DistinctExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DistinctExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The limit plan to be executed
   * @param child_executor The child executor from which tuples are pulled
   */
  DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the distinct */
  void Init() override;

  /**
   * Yield the next tuple from the distinct.
   * @param[out] tuple The next tuple produced by the distinct
   * @param[out] rid The next tuple RID produced by the distinct
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the distinct */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** The distinct plan node to be executed */
  const DistinctPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  SimpleDistinctHashTable dtb_{};
};
}  // namespace bustub
