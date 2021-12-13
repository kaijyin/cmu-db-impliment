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
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {

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
   * Creates a new lock manager configured for the deadlock prevention policy.
   */
  LockManager() = default;

  ~LockManager() = default;

  /*
   * [LOCK_NOTE]: For all locking functions, we:
   * 1. return false if the transaction is aborted; and
   * 2. block on wait, return true when the lock request is granted; and
   * 3. it is undefined behavior to try locking an already locked RID in the
   * same transaction, i.e. the transaction is responsible for keeping track of
   * its current locks.
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
   * @param rid the RID that should already be locked in shared mode by the
   * requesting transaction
   * @return true if the upgrade is successful, false otherwise
   */
  bool LockUpgrade(Transaction *txn, const RID &rid);

  /**
   * Release the lock held by the transaction.
   * @param txn the transaction releasing the lock, it should actually hold the
   * lock
   * @param rid the RID that is locked by the transaction
   * @return true if the unlock is successful, false otherwise
   */
  bool Unlock(Transaction *txn, const RID &rid);

 private:
  void AbortTxn(txn_id_t txn_id, const RID &rid) {
    Transaction *ex_txn = txn_map_[txn_id];
    // abort the yunger transaction
    ex_txn->SetState(TransactionState::ABORTED);
    std::unordered_set<RID> lock_set;
    for (auto item : *ex_txn->GetExclusiveLockSet()) {
      lock_set.emplace(item);
    }
    for (auto item : *ex_txn->GetSharedLockSet()) {
      lock_set.emplace(item);
    }
    for (auto locked_rid : lock_set) {
      if (txn_id == lock_table_[locked_rid].exclusive_locked_txn_) {
        lock_table_[locked_rid].exclusive_locked_txn_ = INVALID_TXN_ID;
      }
      lock_table_[locked_rid].share_locked_req_sets_.erase(txn_id);
      // don't notify other txn in this rid sets
      if (locked_rid.Get() != rid.Get()) {
        lock_table_[locked_rid].cv_.notify_all();
      }
      ex_txn->GetSharedLockSet()->erase(locked_rid);
      ex_txn->GetExclusiveLockSet()->erase(locked_rid);
    }
  }
  void WoundWait(txn_id_t txn_id, const RID &rid) {
    txn_id_t ex_txn_id = lock_table_[rid].exclusive_locked_txn_;
    if (ex_txn_id != INVALID_TXN_ID && ex_txn_id > txn_id) {
      AbortTxn(ex_txn_id, rid);
    }
    if(lock_table_[rid].req_sets_[txn_id] == LockMode::EXCLUSIVE&&!lock_table_[rid].share_locked_req_sets_.empty()){
      for(auto&sh_txn_id:lock_table_[rid].share_locked_req_sets_){
        if(sh_txn_id>txn_id){
          AbortTxn(sh_txn_id,rid);
        }
      }         
    }
  }
  bool CheckGrant(txn_id_t txn_id, const RID &rid) {
    WoundWait(txn_id, rid);
    txn_id_t txn_id = lock_table_[rid].exclusive_locked_txn_;
    if (txn_id != INVALID_TXN_ID) {
      return false;
    }
    if (lock_table_[rid].req_sets_[txn_id] == LockMode::SHARED) {
      return true;
    }
    return lock_table_[rid].share_locked_req_sets_.empty();
  }
  std::mutex latch_;
  std::mutex mu_;
  /** Lock table for lock requests. */
  std::unordered_map<RID, LockRequestQueue> lock_table_;
  std::unordered_map<txn_id_t, Transaction *> txn_map_;
};

}  // namespace bustub
