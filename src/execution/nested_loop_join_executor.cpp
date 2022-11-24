//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple outer_tuple;
  RID outer_rid;
  Tuple inner_tuple;
  RID inner_rid;
  auto outer_schema = left_executor_->GetOutputSchema();
  auto inner_schema = right_executor_->GetOutputSchema();
  std::vector<Tuple> inner_tuples;
  while (right_executor_->Next(&inner_tuple, &inner_rid)) {
    inner_tuples.emplace_back(inner_tuple);
  }
  std::deque<Tuple> empty_join;
  while (left_executor_->Next(&outer_tuple, &outer_rid)) {
    bool joined = false;
    for (const auto &t : inner_tuples) {
      inner_tuple = t;
      auto value = plan_->predicate_->EvaluateJoin(&outer_tuple, outer_schema, &inner_tuple, inner_schema);
      if (!value.IsNull() && value.GetAs<bool>()) {
        joined = true;
        auto values =
            GetJoinValues(&plan_->OutputSchema(), &outer_schema, &inner_schema, outer_tuple, inner_tuple, false);
        join_res_.emplace_back(values, &plan_->OutputSchema());
      }
    }
    if (!joined && plan_->join_type_ == JoinType::LEFT) {
      auto values = GetJoinValues(&plan_->OutputSchema(), &outer_schema, &inner_schema, outer_tuple, inner_tuple, true);
      empty_join.emplace_back(values, &plan_->OutputSchema());
    }
  }
  if (!empty_join.empty()) {
    join_res_.insert(join_res_.end(), empty_join.begin(), empty_join.end());
    empty_join.clear();
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (join_res_.empty()) {
    return false;
  }
  auto t = join_res_.front();
  join_res_.pop_front();
  *tuple = t;
  return true;
}

auto GetJoinValues(const Schema *out_schema, const Schema *left_schema, const Schema *right_schema,
                   const Tuple &left_tuple, const Tuple &right_tuple, bool left_only) -> std::vector<Value> {
  std::vector<Value> values;
  values.reserve(out_schema->GetColumnCount());
  for (size_t i = 0; i < out_schema->GetColumnCount(); ++i) {
    auto column_name = out_schema->GetColumn(i).GetName();
    if (std::find_if(left_schema->GetColumns().begin(), left_schema->GetColumns().end(), [column_name](const Column& c) {
          return c.GetName() == column_name;
        }) != left_schema->GetColumns().end()) {
      values.push_back(left_tuple.GetValue(left_schema, left_schema->GetColIdx(column_name)));
    } else {
      auto right_col_idx = right_schema->GetColIdx(column_name);
      if (left_only) {
        auto col_type = right_schema->GetColumn(right_col_idx).GetType();
        values.push_back(ValueFactory::GetNullValueByType(col_type));
      } else {
        values.push_back(right_tuple.GetValue(right_schema, right_col_idx));
      }
    }
  }
  return values;
}

}  // namespace bustub
