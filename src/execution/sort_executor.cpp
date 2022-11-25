#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    to_be_sorted_.emplace_back(tuple);
  }
  auto schema = child_executor_->GetOutputSchema();
  auto order_by = plan_->GetOrderBy();
  std::sort(to_be_sorted_.begin(), to_be_sorted_.end(), [order_by, schema](const Tuple &t1, const Tuple &t2) {
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
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (to_be_sorted_.empty()) {
    return false;
  }
  *tuple = to_be_sorted_.front();
  to_be_sorted_.pop_front();
  return true;
}

}  // namespace bustub
