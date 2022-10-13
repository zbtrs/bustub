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

auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  if (txn ->GetState() == TransactionState::ABORTED) {
    return false;
  }
  txn->GetSharedLockSet()->emplace(rid);
  if (txn ->GetState() == TransactionState::SHRINKING || txn ->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn ->SetState(TransactionState::ABORTED);
    txn ->GetSharedLockSet() ->erase(rid);
    return false;
  }
  txn ->SetState(TransactionState::GROWING);
  std::unique_lock<std::mutex> txn_latch(latch_);
  auto *lock_queue = &lock_table_[rid];
  if (lock_queue ->rid_lock_state_ == RIDLockState::EXCLUSIVELOCKED) {
    lock_queue ->request_queue_.emplace_back(txn ->GetTransactionId(),LockMode::SHARED);
    while (true) {
      lock_queue->cv_.wait(txn_latch);
      if (lock_queue ->request_queue_.front().txn_id_ == txn ->GetTransactionId()) {
        lock_queue ->request_queue_.pop_front();
        break;
      }
    }
  }
  lock_queue ->rid_lock_state_ = RIDLockState::SHARELOCKED;

  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  if (txn ->GetState() == TransactionState::ABORTED) {
    return false;
  }
  txn->GetExclusiveLockSet()->emplace(rid);
  if (txn ->GetState() == TransactionState::SHRINKING) {
    txn ->SetState(TransactionState::ABORTED);
    txn ->GetExclusiveLockSet() ->erase(rid);
    return false;
  }
  txn ->SetState(TransactionState::GROWING);
  std::unique_lock<std::mutex> txn_latch(latch_);
  auto *lock_queue = &lock_table_[rid];
  if (lock_queue ->rid_lock_state_ != RIDLockState::NONLOCK) {
    lock_queue ->request_queue_.emplace_back(txn ->GetTransactionId(),LockMode::EXCLUSIVE);
    while (true) {
      lock_queue->cv_.wait(txn_latch);
      if (lock_queue ->request_queue_.front().txn_id_ == txn ->GetTransactionId()) {
        lock_queue ->request_queue_.pop_front();
        break;
      }
    }
  }
  lock_queue ->rid_lock_state_ = RIDLockState::EXCLUSIVELOCKED;

  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  std::unique_lock<std::mutex> txn_latch(latch_);
  auto *lock_queue = &lock_table_[rid];
  if (txn ->GetState() != TransactionState::ABORTED && !(lock_queue ->rid_lock_state_ == RIDLockState::SHARELOCKED && txn ->GetIsolationLevel() == IsolationLevel::READ_COMMITTED)) {
    txn->SetState(TransactionState::SHRINKING);
  }
  lock_queue ->rid_lock_state_ = RIDLockState::NONLOCK;
  txn_latch.unlock();
  lock_queue ->cv_.notify_all();

  return true;
}

}  // namespace bustub
