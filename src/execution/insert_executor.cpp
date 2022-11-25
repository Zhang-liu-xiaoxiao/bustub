//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), values_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { values_executor_->Init(); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (finished_) {
    return false;
  }
  auto catalog = exec_ctx_->GetCatalog();
  Tuple insert_tuple{};
  RID insert_rid{};
  int count = 0;
  auto table_schema = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->schema_;
  while (values_executor_->Next(&insert_tuple, &insert_rid)) {
    auto res = catalog->GetTable(plan_->table_oid_)
                   ->table_->InsertTuple(insert_tuple, &insert_rid, exec_ctx_->GetTransaction());
    BUSTUB_ASSERT(res, "insert error");
    auto indexes = catalog->GetTableIndexes(catalog->GetTable(plan_->table_oid_)->name_);
    for (auto index : indexes) {
      auto key = insert_tuple.KeyFromTuple(table_schema, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key, insert_rid, exec_ctx_->GetTransaction());
    }
    count++;
  }
  std::vector<Value> t = {Value(INTEGER, count)};
  auto ret_schema = Schema({Column("", INTEGER)});
  *tuple = Tuple(t, &ret_schema);
  finished_ = true;
  return true;
}

}  // namespace bustub
