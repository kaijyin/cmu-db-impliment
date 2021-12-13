//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct HashjoinKey {
  Value key_;
  explicit HashjoinKey(const Value &value) { key_ = value; }
  bool operator==(const HashjoinKey &other) const { return key_.CompareEquals(other.key_) == CmpBool::CmpTrue; }
};

struct HashjoinValue {
  // cosntruct left join table's tuple in hash table
  std::vector<Tuple> tuples_;
};

}  // namespace bustub
namespace std {

/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::HashjoinKey> {
  std::size_t operator()(const bustub::HashjoinKey &hash_key) const {
    if (hash_key.key_.IsNull()) {
      return 0;
    }
    return bustub::HashUtil::HashValue(&hash_key.key_);
  }
};

}  // namespace std

namespace bustub {
class SimpleHashjoinTable {
 public:
  void InsertCombine(const HashjoinKey &hash_key, const Tuple &tuple) { ht_[hash_key].tuples_.emplace_back(tuple); }
  HashjoinValue GetTuples(const HashjoinKey &hash_key) {
    if (ht_.count(hash_key) != 0) {
      return ht_[hash_key];
    }
    return HashjoinValue{};
  }

 private:
  std::unordered_map<HashjoinKey, HashjoinValue> ht_{};
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_executor,
                   std::unique_ptr<AbstractExecutor> &&right_executor);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  Transaction *txn_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  /** Simple aggregation hash table */
  SimpleHashjoinTable jht_{};
  std::vector<Tuple> cur_tuples_{};
  size_t cur_idx_{0};
  Tuple right_tuple_;
  RID right_rid_;
};

}  // namespace bustub
