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
  LOG_DEBUG("txn :%d Try lock table :%d in mode:%d ,txn status:%d", txn->GetTransactionId(), oid,
            static_cast<std::underlying_type<LockMode>::type>(lock_mode),
            static_cast<std::underlying_type<TransactionState>::type>(txn->GetState()));
  if (!TableLockValidate(txn, lock_mode)) {
    LOG_DEBUG("txn :%d lock table :%d in mode:%d error", txn->GetTransactionId(), oid,
              static_cast<std::underlying_type<LockMode>::type>(lock_mode));
    return false;
  }
  std::shared_ptr<LockRequestQueue> lock_queue;
  {
    std::lock_guard<std::mutex> lock(table_lock_map_latch_);
    if (table_lock_map_.find(oid) == table_lock_map_.end()) {
      lock_queue = std::make_shared<LockRequestQueue>();
      table_lock_map_[oid] = lock_queue;
    } else {
      lock_queue = table_lock_map_[oid];
    }
  }
  return TryLockTable(txn, lock_mode, lock_queue, oid);
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  LOG_DEBUG("txn :%d Try Unlock table :%d ,txn status:%d", txn->GetTransactionId(), oid,
            static_cast<std::underlying_type<TransactionState>::type>(txn->GetState()));

  if (txn->GetSharedRowLockSet()->find(oid) != txn->GetSharedRowLockSet()->end() &&
      !txn->GetSharedRowLockSet()->find(oid)->second.empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  if (txn->GetExclusiveRowLockSet()->find(oid) != txn->GetExclusiveRowLockSet()->end() &&
      !txn->GetExclusiveRowLockSet()->find(oid)->second.empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  std::shared_ptr<LockRequestQueue> queue;
  {
    std::lock_guard<std::mutex> lock(table_lock_map_latch_);
    if (table_lock_map_.find(oid) == table_lock_map_.end()) {
      queue = std::make_shared<LockRequestQueue>();
      table_lock_map_[oid] = queue;
    } else {
      queue = table_lock_map_[oid];
    }
  }
  std::lock_guard<std::mutex> queue_lock(queue->latch_);
  std::shared_ptr<LockRequest> unlock_req = nullptr;
  for (const auto &req : queue->request_queue_) {
    if (req->granted_ && req->txn_id_ == txn->GetTransactionId()) {
      unlock_req = req;
      break;
    }
  }
  if (unlock_req == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  queue->request_queue_.remove(unlock_req);
  TwoPCPhaseChange(txn, unlock_req);
  if (!RemoveTxnTableSet(txn, oid)) {
    BUSTUB_ASSERT(false, "Cannot happen");
  }
  LOG_DEBUG("unlock table notify all");
  queue->cv_.notify_all();
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  LOG_DEBUG("txn :%d Try lock row :%s in table:%d,lock mode:%d ,txn status:%d", txn->GetTransactionId(),
            rid.ToString().c_str(), oid, static_cast<std::underlying_type<LockMode>::type>(lock_mode),
            static_cast<std::underlying_type<TransactionState>::type>(txn->GetState()));
  if (!RowLockValidate(txn, lock_mode)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  std::shared_ptr<LockRequestQueue> lock_queue;
  {
    std::lock_guard<std::mutex> lock(row_lock_map_latch_);
    if (row_lock_map_.find(rid) == row_lock_map_.end()) {
      lock_queue = std::make_shared<LockRequestQueue>();
      row_lock_map_[rid] = lock_queue;
    } else {
      lock_queue = row_lock_map_[rid];
    }
  }
  return LockManager::TryLockRow(txn, lock_mode, oid, rid, lock_queue);
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  LOG_DEBUG("txn :%d Try Unlock Row :%s in table:%d ,txn status:%d", txn->GetTransactionId(), rid.ToString().c_str(),
            oid, static_cast<std::underlying_type<TransactionState>::type>(txn->GetState()));
  std::shared_ptr<LockRequestQueue> queue;
  {
    std::lock_guard<std::mutex> lock(row_lock_map_latch_);
    if (row_lock_map_.find(rid) == row_lock_map_.end()) {
      queue = std::make_shared<LockRequestQueue>();
      row_lock_map_[rid] = queue;
    } else {
      queue = row_lock_map_[rid];
    }
  }
  std::lock_guard<std::mutex> queue_lock(queue->latch_);
  std::shared_ptr<LockRequest> unlock_req = nullptr;
  for (const auto &req : queue->request_queue_) {
    if (req->granted_ && req->txn_id_ == txn->GetTransactionId()) {
      unlock_req = req;
      break;
    }
  }
  if (unlock_req == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  queue->request_queue_.remove(unlock_req);
  TwoPCPhaseChange(txn, unlock_req);
  if (!RemoveTxnRowSet(txn, rid, oid)) {
    BUSTUB_ASSERT(false, "Cannot happen");
  }
  LOG_DEBUG("unlock row notify all");
  queue->cv_.notify_all();
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  auto exist = std::find_if(waits_for_[t1].begin(), waits_for_[t1].end(), [&](const auto &item) { return item == t2; });
  if (exist != waits_for_[t1].end()) {
    return;
  }
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_[t1].erase(
      std::remove_if(waits_for_[t1].begin(), waits_for_[t1].end(), [&](const auto &item) { return item == t2; }),
      waits_for_[t1].end());
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::vector<txn_id_t> keys;
  std::unordered_set<txn_id_t> visited;
  std::vector<txn_id_t> path;

  for (const auto &p : waits_for_) {
    keys.push_back(p.first);
  }
  std::sort(keys.begin(), keys.end());
  txn_id_t circled;
  for (auto key : keys) {
    if (DFS(key, waits_for_[key], visited, path, &circled)) {
      auto iter = std::max_element(path.begin(), path.end());
      if (iter == (path.end() - 1)) {
        to_remove_ = std::make_pair(*iter, circled);
      } else {
        to_remove_ = std::make_pair(*iter, *(iter + 1));
      }
      *txn_id = *iter;
      return true;
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto &pair : waits_for_) {
    for (auto t : pair.second) {
      edges.emplace_back(pair.first, t);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
//      LOG_DEBUG("Cycle Detection");
      std::lock_guard<std::mutex> wait_for_lock(waits_for_latch_);

      std::lock_guard<std::mutex> table_lock(table_lock_map_latch_);
      std::lock_guard<std::mutex> row_lock(row_lock_map_latch_);
      BuildGraph();
      for (const auto &p : waits_for_) {
        std::sort(waits_for_[p.first].begin(), waits_for_[p.first].end());
      }

      txn_id_t dead;
      while (HasCycle(&dead)) {
        LOG_WARN("abort %d txn, delete edge %d->%d", dead, to_remove_.first, to_remove_.second);
        RemoveEdge(to_remove_.first, to_remove_.second);
        auto tnx = TransactionManager::GetTransaction(dead);
        tnx->SetState(TransactionState::ABORTED);
        for (const auto &p : table_lock_map_) {
          auto it = std::find_if(
              p.second->request_queue_.begin(), p.second->request_queue_.end(),
              [&](const std::shared_ptr<LockRequest> &req) { return !req->granted_ && req->txn_id_ == dead; });
          if (it != p.second->request_queue_.end()) {
            p.second->cv_.notify_all();
            break;
          }
        }
        for (const auto &p : row_lock_map_) {
          auto it = std::find_if(
              p.second->request_queue_.begin(), p.second->request_queue_.end(),
              [&](const std::shared_ptr<LockRequest> &req) { return !req->granted_ && req->txn_id_ == dead; });
          if (it != p.second->request_queue_.end()) {
            p.second->cv_.notify_all();
            break;
          }
        }
      }
      waits_for_.clear();
      to_remove_ = {};
//      LOG_DEBUG("Cycle Detection End");
    }
  }
}
void LockManager::BuildGraph() {
  for (const auto &p : table_lock_map_) {
    std::lock_guard<std::mutex> l(p.second->latch_);
    for (const auto &req : p.second->request_queue_) {
      if (!req->granted_) {
        for (const auto &r : p.second->request_queue_) {
          if (r->granted_ && !CheckCompatible(r->lock_mode_, req->lock_mode_)) {
            AddEdge(req->txn_id_, r->txn_id_);
          }
        }
      }
    }
  }
  for (const auto &p : row_lock_map_) {
    std::lock_guard<std::mutex> l(p.second->latch_);
    for (const auto &req : p.second->request_queue_) {
      if (!req->granted_) {
        for (const auto &r : p.second->request_queue_) {
          if (r->granted_ && !CheckCompatible(r->lock_mode_, req->lock_mode_)) {
            AddEdge(req->txn_id_, r->txn_id_);
          }
        }
      }
    }
  }
}
auto LockManager::TryLockTable(Transaction *txn, LockManager::LockMode lock_mode,
                               const std::shared_ptr<LockRequestQueue> &lock_queue, const table_oid_t &oid) -> bool {
  std::unique_lock<std::mutex> lock(lock_queue->latch_);
  auto old_req = LockManager::CheckUpgrade(txn, lock_mode, lock_queue);
  bool upgraded = false;
  if (old_req != nullptr) {
    if (old_req->lock_mode_ == lock_mode) {
      LOG_DEBUG("txn :%d upgrade lock in same lock mode, just return", txn->GetTransactionId());
      return true;
    }
    LOG_DEBUG("txn :%d Upgrade lock table :%d to mode:%d ,txn status:%d", txn->GetTransactionId(), oid,
              static_cast<std::underlying_type<LockMode>::type>(lock_mode),
              static_cast<std::underlying_type<TransactionState>::type>(txn->GetState()));
    lock_queue->request_queue_.remove(old_req);
    upgraded = true;
    if (!RemoveTxnTableSet(txn, oid)) {
      BUSTUB_ASSERT(false, "upgrade fail!");
    }
    lock_queue->upgrading_ = txn->GetTransactionId();
  }
  auto new_req = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lock_queue->request_queue_.push_back(new_req);

  while (txn->GetState() != TransactionState::ABORTED && !ApplyLock(txn, lock_queue, lock_mode)) {
    lock_queue->cv_.wait(lock);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    LOG_DEBUG("txn %d is Aborted", txn->GetTransactionId());
    if (upgraded) {
      lock_queue->upgrading_ = INVALID_TXN_ID;
    }
    lock_queue->request_queue_.remove(new_req);
    lock_queue->cv_.notify_all();
    return false;
  }
  TableBookKeeping(txn, lock_mode, oid);
  new_req->granted_ = true;
  if (upgraded) {
    lock_queue->upgrading_ = INVALID_TXN_ID;
  }
  LOG_DEBUG("txn :%d Success Get lock table :%d in mode:%d ,txn status:%d", txn->GetTransactionId(), oid,
            static_cast<std::underlying_type<LockMode>::type>(lock_mode),
            static_cast<std::underlying_type<TransactionState>::type>(txn->GetState()));
  return true;
}

//! core func
//! check if can acquire lock ?
auto LockManager::ApplyLock(Transaction *txn, const std::shared_ptr<LockRequestQueue> &lock_queue, LockMode lock_mode)
    -> bool {
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
  // FIFO
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
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
        (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // growing
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
      (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
       lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
  }
  return true;
}

auto LockManager::CheckUpgrade(Transaction *txn, LockManager::LockMode lock_mode,
                               const std::shared_ptr<LockRequestQueue> &lock_queue) -> std::shared_ptr<LockRequest> {
  bool if_upgrade = false;
  std::shared_ptr<LockRequest> before_upgrade = nullptr;
  for (const auto &req : lock_queue->request_queue_) {
    if (req->txn_id_ == txn->GetTransactionId()) {
      BUSTUB_ASSERT(req->granted_ == true, "must be granted");
      before_upgrade = req;
      if_upgrade = true;
      break;
    }
  }
  if (if_upgrade && lock_queue->upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    LOG_DEBUG("concurrent upgrade");
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
  if (if_upgrade) {
    if (before_upgrade->lock_mode_ == lock_mode) {
      return before_upgrade;
    }
    switch (before_upgrade->lock_mode_) {
      case LockMode::SHARED:
        if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE) {
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        }
        break;
      case LockMode::EXCLUSIVE:
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        break;
      case LockMode::INTENTION_SHARED:
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED) {
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        }
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        if (lock_mode != LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::ABORTED);
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
auto LockManager::RemoveTxnTableSet(Transaction *txn, const table_oid_t &oid) const -> bool {
  if (txn->GetSharedTableLockSet()->find(oid) != txn->GetSharedTableLockSet()->end()) {
    txn->GetSharedTableLockSet()->erase(oid);
    return true;
  }
  if (txn->GetExclusiveTableLockSet()->find(oid) != txn->GetExclusiveTableLockSet()->end()) {
    txn->GetExclusiveTableLockSet()->erase(oid);
    return true;
  }

  if (txn->GetIntentionSharedTableLockSet()->find(oid) != txn->GetIntentionSharedTableLockSet()->end()) {
    txn->GetIntentionSharedTableLockSet()->erase(oid);
    return true;
  }
  if (txn->GetIntentionExclusiveTableLockSet()->find(oid) != txn->GetIntentionExclusiveTableLockSet()->end()) {
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
    return true;
  }
  if (txn->GetSharedIntentionExclusiveTableLockSet()->find(oid) !=
      txn->GetSharedIntentionExclusiveTableLockSet()->end()) {
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
    return true;
  }
  return false;
}

void LockManager::TwoPCPhaseChange(Transaction *txn, const std::shared_ptr<LockRequest> &req) {
  if (txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED) {
    return;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (req->lock_mode_ == LockMode::SHARED || req->lock_mode_ == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (req->lock_mode_ == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (req->lock_mode_ == LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    }
    if (req->lock_mode_ == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }
}
auto LockManager::RowLockValidate(Transaction *txn, LockManager::LockMode lock_mode) -> bool {
  if (txn->GetState() == TransactionState::ABORTED || txn->GetState() == TransactionState::COMMITTED) {
    return false;
  }
  if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (lock_mode == LockMode::SHARED) {
        return true;
      }
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    BUSTUB_ASSERT(false, "");
  }
  //! Growing
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && lock_mode == LockMode::SHARED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
  }

  return true;
}
auto LockManager::TryLockRow(Transaction *txn, LockManager::LockMode lock_mode, const table_oid_t &oid, const RID &rid,
                             const std::shared_ptr<LockRequestQueue> &lock_queue) -> bool {
  if (!CheckTableLockForRow(txn, lock_mode, oid)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }
  bool upgraded = false;
  std::unique_lock<std::mutex> row_queue_lock(lock_queue->latch_);
  auto old_req = CheckUpgrade(txn, lock_mode, lock_queue);
  if (old_req != nullptr) {
    if (old_req->lock_mode_ == lock_mode) {
      LOG_DEBUG("txn :%d upgrade lock in same lock mode, just return", txn->GetTransactionId());
      return true;
    }
    LOG_DEBUG("txn :%d Upgrade lock table:%d, row :%s in mode:%d ,txn status:%d", txn->GetTransactionId(), oid,
              rid.ToString().c_str(), static_cast<std::underlying_type<LockMode>::type>(lock_mode),
              static_cast<std::underlying_type<TransactionState>::type>(txn->GetState()));
    upgraded = true;
    lock_queue->request_queue_.remove(old_req);
    if (!RemoveTxnRowSet(txn, rid, oid)) {
      BUSTUB_ASSERT(false, "upgrade fail");
    }
    lock_queue->upgrading_ = txn->GetTransactionId();
  }
  auto new_req = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  lock_queue->request_queue_.push_back(new_req);
  while (txn->GetState() != TransactionState::ABORTED && !ApplyLock(txn, lock_queue, lock_mode)) {
    lock_queue->cv_.wait(row_queue_lock);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    LOG_DEBUG("txn %d is Aborted", txn->GetTransactionId());
    lock_queue->request_queue_.remove(new_req);
    if (upgraded) {
      lock_queue->upgrading_ = INVALID_TXN_ID;
    }
    lock_queue->cv_.notify_all();
    return false;
  }
  if (upgraded) {
    lock_queue->upgrading_ = INVALID_TXN_ID;
  }
  RowBookKeeping(txn, lock_mode, oid, rid);
  new_req->granted_ = true;
  LOG_DEBUG("txn :%d Success Get lock table:%d, row :%s in mode:%d ,txn status:%d", txn->GetTransactionId(), oid,
            rid.ToString().c_str(), static_cast<std::underlying_type<LockMode>::type>(lock_mode),
            static_cast<std::underlying_type<TransactionState>::type>(txn->GetState()));
  return true;
}
auto LockManager::CheckTableLockForRow(Transaction *txn, LockManager::LockMode lock_mode, const table_oid_t &oid)
    -> bool {
  if (lock_mode == LockMode::SHARED) {
    return txn->IsTableExclusiveLocked(oid) || txn->IsTableIntentionSharedLocked(oid) ||
           txn->IsTableSharedLocked(oid) || txn->IsTableSharedIntentionExclusiveLocked(oid) ||
           txn->IsTableIntentionExclusiveLocked(oid);
  }
  if (lock_mode == LockMode::EXCLUSIVE) {
    return txn->IsTableExclusiveLocked(oid) || txn->IsTableSharedIntentionExclusiveLocked(oid) ||
           txn->IsTableIntentionExclusiveLocked(oid);
  }
  BUSTUB_ASSERT(false, "");
  return false;
}
auto LockManager::RemoveTxnRowSet(Transaction *txn, const RID &rid, const table_oid_t &oid) -> bool {
  auto x_set = txn->GetExclusiveRowLockSet()->find(oid);
  auto s_set = txn->GetSharedRowLockSet()->find(oid);
  if (x_set != txn->GetExclusiveRowLockSet()->end() && x_set->second.find(rid) != x_set->second.end()) {
    x_set->second.erase(rid);
    return true;
  }
  if (s_set != txn->GetSharedRowLockSet()->end() && s_set->second.find(rid) != s_set->second.end()) {
    s_set->second.erase(rid);
    return true;
  }
  return false;
}
void LockManager::RowBookKeeping(Transaction *txn, LockManager::LockMode lock_mode, const table_oid_t &oid,
                                 const RID &rid) {
  auto s_pair = txn->GetSharedRowLockSet()->find(oid);
  auto x_pair = txn->GetExclusiveRowLockSet()->find(oid);

  switch (lock_mode) {
    case LockMode::SHARED:
      if (s_pair == txn->GetSharedRowLockSet()->end()) {
        txn->GetSharedRowLockSet()->insert(std::make_pair(oid, std::unordered_set<RID>{rid}));
      } else {
        (*(txn->GetSharedRowLockSet()))[oid].insert(rid);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (x_pair == txn->GetExclusiveRowLockSet()->end()) {
        txn->GetExclusiveRowLockSet()->insert(std::make_pair(oid, std::unordered_set<RID>{rid}));
      } else {
        (*(txn->GetExclusiveRowLockSet()))[oid].insert(rid);
      }
      break;
    default:
      BUSTUB_ASSERT(false, "");
  }
}
auto LockManager::DFS(txn_id_t txn, const std::vector<txn_id_t> &adj, std::unordered_set<txn_id_t> &visited,
                      std::vector<txn_id_t> &path, txn_id_t *circled_knot) -> bool {
  if (adj.empty()) {
    return false;
  }
  visited.insert(txn);
  path.push_back(txn);
  for (const auto &next : adj) {
    if (visited.count(next) == 0) {
      if (DFS(next, waits_for_[next], visited, path, circled_knot)) {
        return true;
      }
    } else {
      *circled_knot = next;
      return true;
    }
  }
  visited.erase(txn);
  path.pop_back();
  return false;
}

}  // namespace bustub
