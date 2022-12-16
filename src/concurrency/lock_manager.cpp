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
  if (DoLockTable(txn, lock_mode, lock_queue, oid)) {
    return true;
  }
  lock_queue->cv_.wait(lock, [&]() {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    };
    return false;
  });
  return txn->GetState() != TransactionState::ABORTED;
}

auto LockManager::DoLockTable(Transaction *txn, LockManager::LockMode lock_mode,
                              const std::shared_ptr<LockRequestQueue> &lock_queue, const table_oid_t &oid) -> bool {
  if (lock_queue->request_queue_.empty()) {
    auto req = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
    lock_queue->request_queue_.push_back(req);
    return true;
  }
  for (auto req : lock_queue->request_queue_) {

  }
  return false;
}
auto LockManager::TableLockValidate(Transaction *txn, LockManager::LockMode lock_mode) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
      (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
       lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
        (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED)) {
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  return true;
}

}  // namespace bustub
