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
  if (txn->IsSharedLocked(rid)) {
    return true;
  }
  mu_.lock();
  if (txn->GetState() == TransactionState::ABORTED) {
    mu_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
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
    if (txn->GetState() != TransactionState::ABORTED) {
      lock_table_[rid].cv_.wait(lock);
    }
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
  if (txn->IsExclusiveLocked(rid)) {
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
    if (txn->GetState() != TransactionState::ABORTED) {
      lock_table_[rid].cv_.wait(lock);
    }
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
    // LOG_DEBUG("%d checkGrant on:%s",txn_id,rid.ToString().c_str());
    if (CheckGrant(txn, rid)) {
      lock_table_[rid].req_sets_.erase(txn_id);
      lock_table_[rid].exclusive_locked_txn_ = txn_id;
      mu_.unlock();
      break;
    }
    // LOG_DEBUG("%d checkGrant on:%s finished",txn_id,rid.ToString().c_str());
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
    lock_table_[rid].exclusive_locked_txn_ = INVALID_TXN_ID;
  }
  lock_table_[rid].share_locked_req_sets_.erase(txn_id);
  mu_.unlock();
  lock_table_[rid].cv_.notify_all();
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

}  // namespace bustub
