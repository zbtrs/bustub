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
  if (txn ->GetState() == TransactionState::SHRINKING || txn ->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn ->SetState(TransactionState::ABORTED);
    //throw (TransactionAbortException(txn ->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING));
    return false;
  }
  txn->GetSharedLockSet()->emplace(rid);
  txn ->SetState(TransactionState::GROWING);
  std::unique_lock<std::mutex> txn_latch(latch_);
  auto *lock_queue = &lock_table_[rid];
  bool flag = true;
  for (const auto& lock_request : lock_queue ->request_queue_) {
    if (!lock_request.granted_ || lock_request.lock_mode_ == LockMode::EXCLUSIVE) {
      flag = false;
      break;
    }
  }
  LockRequest new_lock_request(txn ->GetTransactionId(),LockMode::SHARED,txn);
  if (flag) {
    new_lock_request.granted_ = true;
    lock_queue ->request_queue_.push_back(new_lock_request);
  } else {
    auto is_compatible = [&] {
      for (auto & lock_request : lock_queue ->request_queue_) {
        if (lock_request.txn_id_ == txn ->GetTransactionId()) {
          lock_request.granted_ = true;
          return true;
        } if (lock_request.lock_mode_ == LockMode::EXCLUSIVE || !lock_request.granted_) {
          return false;
        }
      }
      return true;
    };

    for (auto &lock_request : lock_queue ->request_queue_) {
      if (lock_request.lock_mode_ == LockMode::EXCLUSIVE && lock_request.txn_id_ > txn ->GetTransactionId()) {
        lock_request.txn_ ->SetState(TransactionState::ABORTED);
      }
    }
    txn_latch.unlock();
    lock_queue ->cv_.notify_all();
    txn_latch.lock();
    bool get_lock = true;
    for (auto &lock_request : lock_queue ->request_queue_) {
      if (lock_request.lock_mode_ == LockMode::EXCLUSIVE || !lock_request.granted_) {
        get_lock = false;
        break;
      }
    }
    if (get_lock) {
      new_lock_request.granted_ = true;
      lock_queue->request_queue_.push_back(new_lock_request);

      return true;
    }

    lock_queue ->request_queue_.push_back(new_lock_request);
    lock_queue ->cv_.wait(txn_latch,[&is_compatible, &txn] { return txn ->GetState() == TransactionState::ABORTED || is_compatible(); });
    if (txn ->GetState() == TransactionState::ABORTED) {
      return false;
    }
  }

  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  if (txn ->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn ->GetState() == TransactionState::SHRINKING) {
    txn ->SetState(TransactionState::ABORTED);
    //throw (TransactionAbortException(txn ->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING));
    return false;
  }
  txn->GetExclusiveLockSet()->emplace(rid);
  txn ->SetState(TransactionState::GROWING);
  std::unique_lock<std::mutex> txn_latch(latch_);
  auto *lock_queue = &lock_table_[rid];

  bool flag = lock_queue ->request_queue_.empty();
  LockRequest new_lock_request(txn ->GetTransactionId(),LockMode::EXCLUSIVE,txn);
  if (flag) {
    new_lock_request.granted_ = true;
    lock_queue ->request_queue_.push_back(new_lock_request);
  } else {
    /*
    lock_queue ->request_queue_.push_back(new_lock_request);
    auto is_compatible = [&] {
      auto &lock_request = lock_queue ->request_queue_.front();
      if (lock_request.txn_id_ == txn ->GetTransactionId()) {
        lock_request.granted_ = true;
        return true;
      }
      return false;
    };
    lock_queue ->cv_.wait(txn_latch,[&is_compatible,&txn] { return txn ->GetState() == TransactionState::ABORTED || is_compatible();  });
    */
    auto is_compatible = [&] {
      auto &lock_request = lock_queue ->request_queue_.front();
      if (lock_request.txn_id_ == txn ->GetTransactionId()) {
        lock_request.granted_ = true;
        return true;
      }
      return false;
    };

    for (auto &lock_request : lock_queue ->request_queue_) {
      if (lock_request.txn_id_ > txn ->GetTransactionId()) {
        lock_request.txn_ ->SetState(TransactionState::ABORTED);
      }
    }
    txn_latch.unlock();
    lock_queue ->cv_.notify_all();
    txn_latch.lock();
    if (lock_queue ->request_queue_.front().txn_id_ == txn ->GetTransactionId()) {
      new_lock_request.granted_ = true;
      lock_queue->request_queue_.push_back(new_lock_request);

      return true;
    }
    lock_queue ->request_queue_.push_back(new_lock_request);
    lock_queue ->cv_.wait(txn_latch,[&is_compatible, &txn] { return txn ->GetState() == TransactionState::ABORTED || is_compatible(); });
    if (txn ->GetState() == TransactionState::ABORTED) {
      return false;
    }
  }

  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  if (txn ->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn ->GetState() == TransactionState::SHRINKING) {
    txn ->SetState(TransactionState::ABORTED);
    //throw (TransactionAbortException(txn ->GetTransactionId(),AbortReason::LOCK_ON_SHRINKING));
    return false;
  }
  if (!txn ->IsSharedLocked(rid)) {
    txn ->SetState(TransactionState::ABORTED);
    //throw (TransactionAbortException(txn ->GetTransactionId(),AbortReason::UPGRADE_CONFLICT));
    return false;
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  std::unique_lock<std::mutex> txn_latch(latch_);
  auto *lock_queue = &lock_table_[rid];
  for (auto &lock_request : lock_queue ->request_queue_) {
    if (lock_request.txn_id_ == txn ->GetTransactionId()) {
      lock_request.lock_mode_ = LockMode::EXCLUSIVE;
      if (lock_queue ->request_queue_.front().txn_id_ == txn ->GetTransactionId()) {
        return true;
      }
      lock_request.granted_ = false;
      auto is_compatible = [&] {
        auto &lock_request = lock_queue ->request_queue_.front();
        if (lock_request.txn_id_ == txn ->GetTransactionId()) {
          lock_request.granted_ = true;
          return true;
        }
        return false;
      };
      lock_queue ->cv_.wait(txn_latch,[&is_compatible,&txn] { return txn ->GetState() == TransactionState::ABORTED || is_compatible();  });
      if (txn ->GetState() == TransactionState::ABORTED) {
        return false;
      }
      break;
    }
  }

  return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  std::unique_lock<std::mutex> txn_latch(latch_);
  auto *lock_queue = &lock_table_[rid];
  std::list<LockRequest>::iterator pos;
  for (auto it = lock_queue ->request_queue_.begin(); it != lock_queue ->request_queue_.end(); ++it) {
    auto lock_request = *it;
    if (lock_request.txn_id_ == txn ->GetTransactionId()) {
      if (txn ->GetState() != TransactionState::ABORTED && txn ->GetState() != TransactionState::COMMITTED && !(lock_request.lock_mode_ == LockMode::SHARED && txn ->GetIsolationLevel() == IsolationLevel::READ_COMMITTED)) {
        txn->SetState(TransactionState::SHRINKING);
      }
      pos = it;
      break;
    }
  }
  lock_queue ->request_queue_.erase(pos);
  txn_latch.unlock();
  lock_queue ->cv_.notify_all();

  return true;
}

}  // namespace bustub
