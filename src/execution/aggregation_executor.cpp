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
#include "execution/expressions/aggregate_value_expression.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple child_tuple;
  RID child_rid;
  aht_.GenerateInitialAggregateValue();
  while (child_->Next(&child_tuple, &child_rid)) {
    aht_.InsertCombine(MakeAggregateKey(&child_tuple), MakeAggregateValue(&child_tuple));
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (aht_iterator_ != aht_.End()) {
    /*
std::vector<Value> values;
auto aggregate_tuple = Tuple(aht_iterator_ .Val().aggregates_,plan_ ->OutputSchema());
for (const auto group_by : plan_ ->GetGroupBys()) {
  values.push_back(group_by ->Evaluate(&aggregate_tuple,plan_ ->OutputSchema()));
}
if (plan_ ->GetHaving() ->EvaluateAggregate(values,aht_iterator_ .Val().aggregates_).GetAs<bool>()) {
  ++aht_iterator_;
  *tuple = aggregate_tuple;
  *rid = aggregate_tuple.GetRid();
  return true;
}
++aht_iterator_;
 */
    auto temp_iter = aht_iterator_;
    ++aht_iterator_;
    if (plan_->GetHaving() != nullptr &&
        !plan_->GetHaving()->EvaluateAggregate(temp_iter.Key().group_bys_, temp_iter.Val().aggregates_).GetAs<bool>()) {
      continue;
    }
    std::vector<Value> values;
    values.reserve(plan_->OutputSchema()->GetColumnCount());
    for (const auto &column : plan_->OutputSchema()->GetColumns()) {
      auto agg_expr = dynamic_cast<const AggregateValueExpression *>(column.GetExpr());
      values.push_back(agg_expr->EvaluateAggregate(temp_iter.Key().group_bys_, temp_iter.Val().aggregates_));
    }
    *tuple = Tuple(values, plan_->OutputSchema());
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
