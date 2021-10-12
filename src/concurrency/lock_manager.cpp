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
    return false;
  } else if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  mu_.lock();
  if (lock_table_[rid].exclusive_locked_txn_ != -1) {
    AddEdge(txn_id, lock_table_[rid].exclusive_locked_txn_);
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    // read commit flag
    lock_table_[rid].req_list_[txn_id] = true;
  } else {
    for (auto &id : lock_table_[rid].share_locked_req_sets_) {
      AddEdge(txn_id, id);
    }
    lock_table_[rid].req_list_[txn_id] = false;
  }
  mu_.unlock();
  std::unique_lock lock(latch_);
  while (txn->GetState() != TransactionState::ABORTED) {
    mu_.lock();
    if (waits_for_[txn_id].empty()) {
      lock_table_[rid].req_list_.erase(txn_id);
      lock_table_[rid].share_locked_req_sets_.emplace(txn_id);
      for (const auto &it : lock_table_[rid].req_list_) {
        if (!it.second) {
          AddEdge(it.first, txn_id);
        }
      }
      mu_.unlock();
      break;
    }
    mu_.unlock();
    lock_table_[rid].cv_.wait(lock);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  const auto txn_id = txn->GetTransactionId();
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  mu_.lock();
  if (lock_table_[rid].exclusive_locked_txn_ != -1) {
    AddEdge(txn_id, lock_table_[rid].exclusive_locked_txn_);
  }
  for (auto &id : lock_table_[rid].share_locked_req_sets_) {
    AddEdge(txn_id, id);
  }
  lock_table_[rid].req_list_[txn_id] = false;
  mu_.unlock();
  std::unique_lock lock(latch_);
  while (txn->GetState() != TransactionState::ABORTED) {
    mu_.lock();
    if (waits_for_[txn_id].empty()) {
      lock_table_[rid].req_list_.erase(txn_id);
      lock_table_[rid].exclusive_locked_txn_ = txn_id;
      for (const auto &it : lock_table_[rid].req_list_) {
        AddEdge(it.first, txn_id);
      }
      mu_.unlock();
      break;
    }
    mu_.unlock();
    lock_table_[rid].cv_.wait(lock);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
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
    return false;
  }
  lock_table_[rid].upgrading_ = true;
  txn->GetSharedLockSet()->erase(rid);
  lock_table_[rid].share_locked_req_sets_.erase(txn_id);
  for (auto &id : be_wait_[txn_id]) {
    RemoveEdge(id, txn_id);
  }
  be_wait_[txn_id].clear();
  mu_.unlock();
  bool res = LockExclusive(txn, rid);
  mu_.lock();
  lock_table_[rid].upgrading_ = false;
  mu_.unlock();
  return res;
}
bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  const auto txn_id = txn->GetTransactionId();
  txn->SetState(TransactionState::SHRINKING);
  mu_.lock();
  if (txn_id == lock_table_[rid].exclusive_locked_txn_) {
    lock_table_[rid].exclusive_locked_txn_ = -1;
  } else {
    lock_table_[rid].share_locked_req_sets_.erase(txn_id);
  }
  for (auto &id : be_wait_[txn_id]) {
    RemoveEdge(id, txn_id);
  }
  be_wait_[txn_id].clear();
  mu_.unlock();
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  lock_table_[rid].cv_.notify_all();
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_[t1].emplace(t2);
  be_wait_[t2].emplace(t1);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].erase(t2); }

bool LockManager::HasCycle(txn_id_t *txn_id) {
  std::unordered_map<txn_id_t, bool> vis;
  txn_id_t res = -1;
  txn_id_t cycle_node = -1;
  dfs(waits_for_.begin()->first, vis, cycle_node, &res);
  if (res != -1) {
    *txn_id = res;
    return true;
  } else {
    return false;
  }
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> res;
  for (auto &it : waits_for_) {
    auto l_id = it.first;
    for (auto &r_id : it.second) {
      res.push_back({l_id, r_id});
    }
  }
  return res;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      mu_.lock();
      txn_id_t min_cycle_id;
      while (HasCycle(&min_cycle_id)) {
        for (auto &id : be_wait_[min_cycle_id]) {
          RemoveEdge(id, min_cycle_id);
        }
        be_wait_[min_cycle_id].clear();
        for (auto &id : waits_for_[min_cycle_id]) {
          be_wait_[id].erase(min_cycle_id);
        }
        waits_for_[min_cycle_id].clear();
        Transaction *txn = TransactionManager::GetTransaction(min_cycle_id);
        txn->SetState(TransactionState::ABORTED);
        for (auto &rid : *txn->GetSharedLockSet()) {
          lock_table_[rid].cv_.notify_all();
        }
        for (auto &rid : *txn->GetExclusiveLockSet()) {
          lock_table_[rid].cv_.notify_all();
        }
      }
      mu_.unlock();
    }
  }
}

}  // namespace bustub
