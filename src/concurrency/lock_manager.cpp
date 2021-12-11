//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  const auto txn_id = txn->GetTransactionId();
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
  }
   if(txn->IsSharedLocked(rid)){
    return true;
  }
  mu_.lock();
  lock_table_[rid].req_sets_[txn_id] = LockMode::SHARED;
  txn_map_[txn_id] = txn;
  mu_.unlock();
  std::unique_lock lock(latch_);
  while (txn->GetState() != TransactionState::ABORTED) {
    mu_.lock();
    if (CheckGrant(txn, rid)) {
      lock_table_[rid].req_sets_.erase(txn_id);
      lock_table_[rid].share_locked_req_sets_.emplace(txn_id);
      mu_.unlock();
      break;
    }
    mu_.unlock();
    lock_table_[rid].cv_.wait(lock);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    mu_.lock();
    lock_table_[rid].req_sets_.erase(txn_id);
    mu_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
  txn->GetSharedLockSet()->emplace(rid);
  // LOG_DEBUG("%d sharedlock %s sucess", txn->GetTransactionId(), rid.ToString().c_str());
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  const auto txn_id = txn->GetTransactionId();
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
  }
  if(txn->IsExclusiveLocked(rid)){
    return true;
  }
  mu_.lock();
  lock_table_[rid].req_sets_[txn_id] = LockMode::EXCLUSIVE;
  txn_map_[txn_id] = txn;
  mu_.unlock();
  std::unique_lock lock(latch_);
  while (txn->GetState() != TransactionState::ABORTED) {
    mu_.lock();
    if (CheckGrant(txn, rid)) {
      lock_table_[rid].req_sets_.erase(txn_id);
      lock_table_[rid].exclusive_locked_txn_ = txn_id;
      mu_.unlock();
      break;
    }
    mu_.unlock();
    lock_table_[rid].cv_.wait(lock);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    mu_.lock();
    lock_table_[rid].req_sets_.erase(txn_id);
    mu_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}
bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  const auto txn_id = txn->GetTransactionId();
  mu_.lock();
  if (lock_table_[rid].upgrading_) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
  }
  lock_table_[rid].upgrading_ = true;
  lock_table_[rid].share_locked_req_sets_.erase(txn_id);
  txn->GetSharedLockSet()->erase(rid);
  lock_table_[rid].req_sets_[txn_id] = LockMode::EXCLUSIVE;
  mu_.unlock();
  std::unique_lock lock(latch_);
  while (txn->GetState() != TransactionState::ABORTED) {
    mu_.lock();
    if (CheckGrant(txn, rid)) {
      lock_table_[rid].req_sets_.erase(txn_id);
      lock_table_[rid].exclusive_locked_txn_ = txn_id;
      mu_.unlock();
      break;
    }
    mu_.unlock();
    lock_table_[rid].cv_.wait(lock);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    mu_.lock();
    lock_table_[rid].req_sets_.erase(txn_id);
    lock_table_[rid].upgrading_ = false;
    mu_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
  mu_.lock();
  lock_table_[rid].upgrading_ = false;
  mu_.unlock();
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}
bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  const auto txn_id = txn->GetTransactionId();
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  mu_.lock();
  if (txn_id == lock_table_[rid].exclusive_locked_txn_) {
    lock_table_[rid].exclusive_locked_txn_ = -1;
  }
  lock_table_[rid].share_locked_req_sets_.erase(txn_id);
  mu_.unlock();
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  lock_table_[rid].cv_.notify_all();
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { graph.waits_for_[t1].emplace(t2); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) { graph.waits_for_[t1].erase(t2); }

bool LockManager::HasCycle(txn_id_t *txn_id) {
  graph.low.clear();
  graph.dfn.clear();
  graph.in_stk.clear();
  graph.stk.clear();
  graph.cnt = 0;
  for (const auto &start : graph.waits_for_) {
    if (graph.dfn.find(start.first) == graph.dfn.end()) {
      txn_id_t res = tarjan(start.first);
      if (res != INVALID_TXN_ID) {
        *txn_id = res;
        return true;
      }
    }
  }
  return false;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (auto &it : graph.waits_for_) {
    auto left = it.first;
    for (auto &right : it.second) {
      std::pair<txn_id_t, txn_id_t> pair(left, right);
      edges.push_back(pair);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      txn_id_t min_cycle_id = INVALID_TXN_ID;
      bool res = false;
      mu_.lock();
      graph.waits_for_.clear();
      for (auto &it : lock_table_) {
        for (auto &request : it.second.req_sets_) {
          auto left = request.first;
          auto lock_mode = request.second;
          if (it.second.exclusive_locked_txn_ != -1) {
            AddEdge(left, it.second.exclusive_locked_txn_);
          }
          if (lock_mode == LockMode::EXCLUSIVE) {
            for (auto &right : it.second.share_locked_req_sets_) {
              AddEdge(left, right);
            }
          }
        }
      }
      res = HasCycle(&min_cycle_id);
      mu_.unlock();
      if (res) {
        // 找到死锁环中最新的事务,释放其拥有的锁
        Transaction *txn = txn_map_[min_cycle_id];
        // in dead lock,dont worry about the currency
        txn->SetState(TransactionState::ABORTED);
        std::unordered_set<RID> lock_set;
        for (auto item : *txn->GetExclusiveLockSet()) {
          lock_set.emplace(item);
        }
        for (auto item : *txn->GetSharedLockSet()) {
          lock_set.emplace(item);
        }
        for (auto locked_rid : lock_set) {
          Unlock(txn, locked_rid);
        }
      }
    }
  }
}

}  // namespace bustub
