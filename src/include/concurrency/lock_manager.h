//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <map>
#include <memory>
#include <mutex>  // NOLINT
#include <set>
#include <unordered_map>
#include <unordered_set>

#include <utility>
#include <vector>

#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {
class TransactionManager;
/**
 * LockManager handles transactions asking for locks on records.
 */
class LockManager {
  enum class LockMode { SHARED, EXCLUSIVE };

  class LockRequestQueue {
   public:
    std::unordered_set<txn_id_t> share_locked_req_sets_;
    std::unordered_map<txn_id_t, LockMode> req_sets_;
    std::condition_variable cv_;  // for notifying blocked transactions on this rid
    bool upgrading_ = false;
    txn_id_t exclusive_locked_txn_ = INVALID_TXN_ID;
  };

 public:
  /**
   * Creates a new lock manager configured for the deadlock detection policy.
   */
  LockManager() {
    enable_cycle_detection_ = true;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
    LOG_INFO("Cycle detection thread launched");
  }

  ~LockManager() {
    enable_cycle_detection_ = false;
    cycle_detection_thread_->join();
    delete cycle_detection_thread_;
    LOG_INFO("Cycle detection thread stopped");
  }

  /*
   * [LOCK_NOTE]: For all locking functions, we:
   * 1. return false if the transaction is aborted; and
   * 2. block on wait, return true when the lock request is granted; and
   * 3. it is undefined behavior to try locking an already locked RID in the same transaction, i.e. the transaction
   *    is responsible for keeping track of its current locks.
   */

  /**
   * Acquire a lock on RID in shared mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the shared lock
   * @param rid the RID to be locked in shared mode
   * @return true if the lock is granted, false otherwise
   */
  bool LockShared(Transaction *txn, const RID &rid);

  /**
   * Acquire a lock on RID in exclusive mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the exclusive lock
   * @param rid the RID to be locked in exclusive mode
   * @return true if the lock is granted, false otherwise
   */
  bool LockExclusive(Transaction *txn, const RID &rid);

  /**
   * Upgrade a lock from a shared lock to an exclusive lock.
   * @param txn the transaction requesting the lock upgrade
   * @param rid the RID that should already be locked in shared mode by the requesting transaction
   * @return true if the upgrade is successful, false otherwise
   */
  bool LockUpgrade(Transaction *txn, const RID &rid);

  /**
   * Release the lock held by the transaction.
   * @param txn the transaction releasing the lock, it should actually hold the lock
   * @param rid the RID that is locked by the transaction
   * @return true if the unlock is successful, false otherwise
   */
  bool Unlock(Transaction *txn, const RID &rid);

  /*** Graph API ***/
  /**
   * Adds edge t1->t2
   */

  /** Adds an edge from t1 -> t2. */
  void AddEdge(txn_id_t t1, txn_id_t t2);

  /** Removes an edge from t1 -> t2. */
  void RemoveEdge(txn_id_t t1, txn_id_t t2);

  /**
   * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
   * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
   * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
   */
  bool HasCycle(txn_id_t *txn_id);

  /** @return the set of all edges in the graph, used for testing only! */
  std::vector<std::pair<txn_id_t, txn_id_t>> GetEdgeList();

  /** Runs cycle detection in the background. */
  void RunCycleDetection();

 private:
  struct TarjanGrap {
   public:
    std::unordered_map<txn_id_t, int> low;
    std::unordered_map<txn_id_t, int> dfn;
    std::unordered_map<txn_id_t, bool> in_stk;
    std::vector<txn_id_t> stk;
    std::map<txn_id_t, std::set<txn_id_t>> waits_for_;
    int cnt;
  };
  bool CheckGrant(Transaction *txn, const RID &rid) {
    if (lock_table_[rid].exclusive_locked_txn_ != INVALID_TXN_ID) {
      return false;
    }
    if (lock_table_[rid].req_sets_[txn->GetTransactionId()] == LockMode::SHARED) {
      return true;
    }
    return lock_table_[rid].share_locked_req_sets_.empty();
  }
  txn_id_t tarjan(txn_id_t u) {
    graph.low[u] = graph.dfn[u] = ++graph.cnt;
    graph.stk.push_back(u);
    graph.in_stk[u] = true;
    for (auto &v : graph.waits_for_[u]) {
      if (graph.dfn.find(v) == graph.dfn.end()) {
        int res = tarjan(v);
        if (res != INVALID_TXN_ID) {
          return res;
        }
        graph.low[u] = std::min(graph.low[v], graph.low[u]);
      } else if (graph.in_stk[v]) {
        LOG_DEBUG("find cycle");
        txn_id_t max_id = INVALID_TXN_ID;
        while (true) {
          auto now = graph.stk.back();
          LOG_DEBUG("now:%d", now);
          graph.stk.pop_back();
          max_id = std::max(max_id, now);
          graph.in_stk[now] = false;
          if (v == now) {
            break;
          }
        }
        return max_id;
      }
    }
    if (graph.low[u] == graph.dfn[u]) {
      while (true) {
        auto v = graph.stk.back();
        graph.stk.pop_back();
        graph.in_stk[v] = false;
        if (v == u) {
          break;
        }
      }
    }
    return INVALID_TXN_ID;
  }
  std::mutex latch_;
  std::mutex mu_;
  std::atomic<bool> enable_cycle_detection_;
  std::thread *cycle_detection_thread_;

  /** Lock table for lock requests. */
  std::unordered_map<RID, LockRequestQueue> lock_table_;
  std::unordered_map<txn_id_t, Transaction *> txn_map_;
  /** Waits-for graph representation. */
  TarjanGrap graph;
};

}  // namespace bustub
