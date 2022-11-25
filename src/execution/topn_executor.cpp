#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  std::deque<Tuple> output;
  auto schema = child_executor_->GetOutputSchema();
  auto order_by = plan_->GetOrderBy();
  while (child_executor_->Next(&tuple, &rid)) {
    output.emplace_back(tuple);
  }
  std::sort(output.begin(), output.end(), [order_by, schema](const Tuple &t1, const Tuple &t2) {
    for (const auto &sort_by : order_by) {
      if (sort_by.first == OrderByType::INVALID) {
        continue;
      }
      if (sort_by.first == OrderByType::DESC) {
        if (sort_by.second->Evaluate(&t1, schema).CompareEquals(sort_by.second->Evaluate(&t2, schema)) ==
            CmpBool::CmpTrue) {
          continue;
        }
        return sort_by.second->Evaluate(&t1, schema).CompareGreaterThan(sort_by.second->Evaluate(&t2, schema));
      }
      //! ASC
      if (sort_by.second->Evaluate(&t1, schema).CompareEquals(sort_by.second->Evaluate(&t2, schema)) ==
          CmpBool::CmpTrue) {
        continue;
      }
      return sort_by.second->Evaluate(&t1, schema).CompareLessThan(sort_by.second->Evaluate(&t2, schema));
    }
    return CmpBool::CmpTrue;
  });
  size_t count = 0;
  while (count < plan_->n_ && count < output.size()) {
    out_.push_back(output[count]);
    count++;
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (out_.empty()) {
    return false;
  }
  *tuple = out_.front();
  out_.pop_front();
  return true;
}

auto CmpTuple(const Schema &schema, const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_by,
              const Tuple &t1, const Tuple &t2) -> CmpBool {
  for (const auto &sort_by : order_by) {
    if (sort_by.first == OrderByType::INVALID) {
      continue;
    }
    if (sort_by.first == OrderByType::DESC) {
      if (sort_by.second->Evaluate(&t1, schema).CompareEquals(sort_by.second->Evaluate(&t2, schema)) ==
          CmpBool::CmpTrue) {
        continue;
      }
      return sort_by.second->Evaluate(&t1, schema).CompareGreaterThan(sort_by.second->Evaluate(&t2, schema));
    }
    //! ASC
    if (sort_by.second->Evaluate(&t1, schema).CompareEquals(sort_by.second->Evaluate(&t2, schema)) ==
        CmpBool::CmpTrue) {
      continue;
    }
    return sort_by.second->Evaluate(&t1, schema).CompareLessThan(sort_by.second->Evaluate(&t2, schema));
  }
  return CmpBool::CmpTrue;
}

}  // namespace bustub
