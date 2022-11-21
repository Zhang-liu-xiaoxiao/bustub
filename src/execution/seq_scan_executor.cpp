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

void SeqScanExecutor::Init() {}

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
  return false;
}

auto GetRetValues(const Schema *out_schema, const Schema *total_schema, TableIterator iterator) -> std::vector<Value> {
  std::vector<Value> values;
  values.reserve(out_schema->GetColumnCount());
  for (size_t i = 0; i < out_schema->GetColumnCount(); ++i) {
    auto column_name = out_schema->GetColumn(i).GetName();
    values.push_back(iterator->GetValue(total_schema, total_schema->GetColIdx(column_name)));
  }
  return values;
}

}  // namespace bustub
