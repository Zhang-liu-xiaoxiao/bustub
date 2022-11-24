//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      tree_index_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(
          exec_ctx->GetCatalog()->GetIndex(plan_->index_oid_)->index_.get())),
      iterator_(tree_index_->GetBeginIterator()) {}

void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto table_name = tree_index_->GetMetadata()->GetTableName();
  auto out_schema = plan_->OutputSchema();
  auto total_schema = exec_ctx_->GetCatalog()->GetTable(table_name)->schema_;

  Tuple index_tuple{};
  while (iterator_ != tree_index_->GetEndIterator() && !iterator_.IsEnd()) {
    auto pair = *iterator_;
    auto scan_rid = pair.second;
    bool res = exec_ctx_->GetCatalog()
                   ->GetTable(table_name)
                   ->table_->GetTuple(scan_rid, &index_tuple, exec_ctx_->GetTransaction());
    BUSTUB_ASSERT(res, "ERROR index scan");
    auto values = GetRetValues(&out_schema, &total_schema, index_tuple);
    *tuple = Tuple(std::move(values), &out_schema);
    *rid = scan_rid;
    ++iterator_;
    return true;
  }
  return false;
}

auto GetRetValues(const Schema *out_schema, const Schema *total_schema, const Tuple &tuple) -> std::vector<Value> {
  std::vector<Value> values;
  values.reserve(out_schema->GetColumnCount());
  for (size_t i = 0; i < out_schema->GetColumnCount(); ++i) {
    auto column_name = out_schema->GetColumn(i).GetName();
    size_t pos;
    if ((pos = column_name.find_last_of('.')) != std::string::npos) {
      column_name = column_name.substr(pos + 1, column_name.length());
    }
    values.push_back(tuple.GetValue(total_schema, total_schema->GetColIdx(column_name)));
  }
  return values;
}
}  // namespace bustub
