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

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (!TableLockValidate(txn, lock_mode)) {
    return false;
  }
  std::shared_ptr<LockRequestQueue> lock_queue;
  {
    std::lock_guard<std::mutex> lock(table_lock_map_latch_);
    if (table_lock_map_.find(oid) == table_lock_map_.end()) {
      lock_queue = std::make_shared<LockRequestQueue>();
    } else {
      lock_queue = table_lock_map_[oid];
    }
  }
  return TryLockTable(txn, lock_mode, lock_queue, oid);
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { return true; }

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { return true; }

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}
auto LockManager::TryLockTable(Transaction *txn, LockManager::LockMode lock_mode,
                               const std::shared_ptr<LockRequestQueue> &lock_queue, const table_oid_t &oid) -> bool {
  std::unique_lock<std::mutex> lock(lock_queue->latch_);
  auto old_req = CheckUpgrade(txn, lock_mode, lock_queue, oid);
  if (old_req != nullptr) {
    lock_queue->request_queue_.remove(old_req);
    lock_queue->upgrading_ = txn->GetTransactionId();
  }
  auto new_req = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  lock_queue->request_queue_.push_back(new_req);

  while (!ApplyLock(txn, lock_queue, lock_mode) && txn->GetState() != TransactionState::ABORTED) {
    lock_queue->cv_.wait(lock);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  TableBookKeeping(txn, lock_mode, oid);
  new_req->granted_ = true;
  return true;
}

//! core func
//! check if can acquire lock ?
auto LockManager::ApplyLock(Transaction *txn, const std::shared_ptr<LockRequestQueue> &lock_queue, LockMode lock_mode)
    -> bool {
  bool can_lock = true;
  for (const auto &req : lock_queue->request_queue_) {
    if (req->txn_id_ == txn->GetTransactionId()) {
      continue;
    }
    if (req->granted_) {
      if (!CheckCompatible(req->lock_mode_, lock_mode)) {
        return false;
      }
    }
  }
  if (lock_queue->upgrading_ != INVALID_TXN_ID) {
    return lock_queue->upgrading_ == txn->GetTransactionId();
  }
  for (const auto &req : lock_queue->request_queue_) {
    if (req->txn_id_ == txn->GetTransactionId()) {
      break;
    }
    if (!req->granted_) {
      if (!CheckCompatible(req->lock_mode_, lock_mode)) {
        return false;
      }
    }
  }
  return true;
}
auto LockManager::TableLockValidate(Transaction *txn, LockManager::LockMode lock_mode) -> bool {
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
        (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED)) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // growing
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
      (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
       lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
  }
  return true;
}

auto LockManager::CheckUpgrade(Transaction *txn, LockManager::LockMode lock_mode,
                               const std::shared_ptr<LockRequestQueue> &lock_queue, const table_oid_t &oid)
    -> LockRequest * {
  bool if_upgrade;
  LockRequest *before_upgrade = nullptr;
  for (auto req : lock_queue->request_queue_) {
    if (req->txn_id_ == txn->GetTransactionId()) {
      BUSTUB_ASSERT(req->granted_ == true, "must be granted");
      before_upgrade = req;
      if_upgrade = true;
      break;
    }
  }
  if (if_upgrade && lock_queue->upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
  if (if_upgrade) {
    if (before_upgrade->lock_mode_ == lock_mode) {
      return nullptr;
    }
    switch (before_upgrade->lock_mode_) {
      case LockMode::SHARED:
        if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE) {
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        }
        break;
      case LockMode::EXCLUSIVE:
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        break;
      case LockMode::INTENTION_SHARED:
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED) {
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        }
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        if (lock_mode != LockMode::EXCLUSIVE) {
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        }
        break;
    }
  }
  return before_upgrade;
}
auto LockManager::CheckCompatible(LockManager::LockMode old_mode, LockManager::LockMode new_mode) -> bool {
  if (new_mode == LockMode::EXCLUSIVE) {
    return false;
  }
  if (new_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return old_mode == LockMode::INTENTION_SHARED;
  }
  if (new_mode == LockMode::SHARED) {
    return old_mode == LockMode::INTENTION_SHARED || old_mode == LockMode::SHARED;
  }
  if (new_mode == LockMode::INTENTION_EXCLUSIVE) {
    return old_mode == LockMode::INTENTION_SHARED || old_mode == LockMode::INTENTION_EXCLUSIVE;
  }
  if (new_mode == LockMode::INTENTION_SHARED) {
    return old_mode != LockMode::EXCLUSIVE;
  }
  return true;
}
void LockManager::TableBookKeeping(Transaction *txn, LockManager::LockMode lock_mode, table_oid_t table_oid) {
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(table_oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(table_oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(table_oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(table_oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(table_oid);
      break;
  }
}

}  // namespace bustub
