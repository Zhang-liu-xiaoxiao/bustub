//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->aggregates_, plan_->agg_types_),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    empty_table_ = false;
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (empty_table_ && plan_->group_bys_.empty()) {
    *tuple = Tuple(aht_.GenerateInitialAggregateValue().aggregates_, &plan_->OutputSchema());
    empty_table_ = false;
    return true;
  }
  if (aht_iterator_ != aht_.End()) {
    auto v = aht_iterator_.Val();
    auto k = aht_iterator_.Key();
    std::vector<Value> values;
    if (plan_->OutputSchema().GetColumnCount() > v.aggregates_.size()) {
      v.aggregates_.insert(v.aggregates_.begin(), k.group_bys_.begin(), k.group_bys_.end());
    }
    *tuple = Tuple(v.aggregates_, &plan_->OutputSchema());
    ++aht_iterator_;
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
