//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  Tuple outer_tuple;
  RID outer_rid;
  Tuple inner_tuple;
  auto output_schema = &plan_->OutputSchema();
  auto outer_schema = child_executor_->GetOutputSchema();
  auto inner_schema = exec_ctx_->GetCatalog()->GetTable(plan_->index_table_name_)->schema_;
  std::deque<Tuple> empty_join;
  while (child_executor_->Next(&outer_tuple, &outer_rid)) {
    auto key_v = plan_->KeyPredicate()->Evaluate(&outer_tuple, outer_schema);
    auto key_tuple = Tuple(std::vector<Value>{key_v}, &index_info_->key_schema_);
    std::vector<RID> index_scan_res;
    index_info_->index_->ScanKey(key_tuple, &index_scan_res, exec_ctx_->GetTransaction());
    if (index_scan_res.empty() && plan_->join_type_ == JoinType::LEFT) {
      auto values =
          GetIndexJoinValues(&plan_->OutputSchema(), &outer_schema, &inner_schema, outer_tuple, inner_tuple, true);
      empty_join.emplace_back(values, output_schema);
    } else {
      for (auto rid : index_scan_res) {
        auto res = exec_ctx_->GetCatalog()
                       ->GetTable(plan_->index_table_name_)
                       ->table_->GetTuple(rid, &inner_tuple, exec_ctx_->GetTransaction());
        auto values =
            GetIndexJoinValues(&plan_->OutputSchema(), &outer_schema, &inner_schema, outer_tuple, inner_tuple, false);
        join_res_.emplace_back(values, output_schema);
        BUSTUB_ASSERT(res, "read index result error");
      }
    }
  }
  if (!empty_join.empty()) {
    join_res_.insert(join_res_.end(), empty_join.begin(), empty_join.end());
    empty_join.clear();
  }
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (join_res_.empty()) {
    return false;
  }
  auto t = join_res_.front();
  join_res_.pop_front();
  *tuple = t;
  return true;
}
auto GetIndexJoinValues(const Schema *out_schema, const Schema *left_schema, const Schema *right_schema,
                        const Tuple &left_tuple, const Tuple &right_tuple, bool left_only) -> std::vector<Value> {
  std::vector<Value> values;
  values.reserve(out_schema->GetColumnCount());
  for (size_t i = 0; i < out_schema->GetColumnCount(); ++i) {
    auto column_name = out_schema->GetColumn(i).GetName();
    if (std::find_if(left_schema->GetColumns().begin(), left_schema->GetColumns().end(),
                     [column_name](const Column &c) { return c.GetName() == column_name; }) !=
        left_schema->GetColumns().end()) {
      values.push_back(left_tuple.GetValue(left_schema, left_schema->GetColIdx(column_name)));
    } else {
      size_t pos = -1;
      if ((pos = column_name.find('.')) != std::string::npos) {
        column_name = column_name.substr(pos + 1, column_name.length());
      }
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
