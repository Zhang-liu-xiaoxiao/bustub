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

void InsertExecutor::Init() {}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  auto catalog = exec_ctx_->GetCatalog();
  Tuple insert_tuple{};
  RID insert_rid{};
  while (values_executor_->Next(&insert_tuple, &insert_rid)) {
    auto res = catalog->GetTable(plan_->table_oid_)
                   ->table_->InsertTuple(insert_tuple, &insert_rid, exec_ctx_->GetTransaction());
    BUSTUB_ASSERT(res,"insert error");
    auto indexes = catalog->GetTableIndexes(catalog->GetTable(plan_->table_oid_)->name_);
    for (auto index : indexes) {
        auto key = insert_tuple.KeyFromTuple();
      index->index_->InsertEntry();
    }
  }
  return false;
}

}  // namespace bustub
