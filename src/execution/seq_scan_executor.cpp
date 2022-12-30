//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iterator_(exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->Begin(exec_ctx_->GetTransaction())) {}

void SeqScanExecutor::Init() {
  auto txn = exec_ctx_->GetTransaction();
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED ||
      txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    auto res = exec_ctx_->GetLockManager()->LockTable(txn, LockManager::LockMode::SHARED, plan_->table_oid_);
    if (!res) {
      throw ExecutionException("Cannot get Shared lock for table");
    }
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto out_schema = plan_->OutputSchema();
  auto total_schema = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->schema_;

  while (iterator_ != exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->End()) {
    if (plan_->filter_predicate_ != nullptr &&
        !plan_->filter_predicate_->Evaluate(iterator_.operator->(), total_schema).GetAs<bool>()) {
      iterator_++;
      continue;
    }
    auto res_values = GetRetValues(&out_schema, &total_schema, iterator_);
    *tuple = Tuple(std::move(res_values), &out_schema);
    *rid = iterator_->GetRid();
    iterator_++;
    return true;
  }
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), plan_->table_oid_);
  }
  return false;
}

auto GetRetValues(const Schema *out_schema, const Schema *total_schema, TableIterator iterator) -> std::vector<Value> {
  std::vector<Value> values;
  values.reserve(out_schema->GetColumnCount());
  for (size_t i = 0; i < out_schema->GetColumnCount(); ++i) {
    auto column_name = out_schema->GetColumn(i).GetName();
    size_t pos = -1;
    if ((pos = column_name.find_last_of('.')) != std::string::npos) {
      column_name = column_name.substr(pos + 1, column_name.length());
    }
    values.push_back(iterator->GetValue(total_schema, total_schema->GetColIdx(column_name)));
  }
  return values;
}

}  // namespace bustub
