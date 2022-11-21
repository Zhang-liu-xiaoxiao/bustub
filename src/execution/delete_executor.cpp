//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (finished_) {
    return false;
  }
  auto catalog = exec_ctx_->GetCatalog();
  Tuple deleted_tuple{};
  RID deleted_rid{};
  int count = 0;
  auto table_schema = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->schema_;
  while (child_executor_->Next(&deleted_tuple, &deleted_rid)) {
    auto res = catalog->GetTable(plan_->table_oid_)->table_->MarkDelete(deleted_rid, exec_ctx_->GetTransaction());
    BUSTUB_ASSERT(res, "insert error");
    auto indexes = catalog->GetTableIndexes(catalog->GetTable(plan_->table_oid_)->name_);
    for (auto index : indexes) {
      auto key = deleted_tuple.KeyFromTuple(table_schema, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key, deleted_rid, exec_ctx_->GetTransaction());
    }
    count++;
  }
  finished_ = true;
  std::vector<Value> t = {Value(INTEGER, count)};
  auto ret_schema = Schema({Column("", INTEGER)});
  *tuple = Tuple(t, &ret_schema);
  return true;
}

}  // namespace bustub
