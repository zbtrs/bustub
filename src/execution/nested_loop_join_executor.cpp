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
#include "execution/expressions/column_value_expression.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),left_executor_(std::move(left_executor)),right_executor_(std::move(right_executor)),cur_(0) {
}

void NestedLoopJoinExecutor::Init() {
  left_executor_ ->Init();
  right_executor_ ->Init();
  Tuple child_tuple;
  RID child_rid;
  std::vector<Tuple> left_child_tuples;
  std::vector<Tuple> right_child_tuples;
  while (left_executor_ ->Next(&child_tuple,&child_rid)) {
    left_child_tuples.push_back(child_tuple);
  }
  while (right_executor_ ->Next(&child_tuple,&child_rid)) {
    right_child_tuples.push_back(child_tuple);
  }
  for (const auto& left_child_tuple : left_child_tuples) {
    for (const auto& right_child_tuple : right_child_tuples) {
      if (plan_ ->Predicate() ->EvaluateJoin(&left_child_tuple,plan_ ->GetLeftPlan() ->OutputSchema(),&right_child_tuple,plan_ ->GetRightPlan() ->OutputSchema()).GetAs<bool>()) {
        std::vector<Value> values;
        values.reserve(plan_ ->OutputSchema() ->GetColumnCount());
        for (const auto &column : plan_ ->OutputSchema() ->GetColumns()) {
          auto column_expr = dynamic_cast<const ColumnValueExpression *>(column.GetExpr());
          if (column_expr ->GetTupleIdx() == 0) {
            values.push_back(left_child_tuple.GetValue(plan_ ->GetLeftPlan() ->OutputSchema(),column_expr ->GetColIdx()));
          } else {
            values.push_back(right_child_tuple.GetValue(plan_ ->GetRightPlan() ->OutputSchema(),column_expr ->GetColIdx()));
          }
        }
        join_tuples_.emplace_back(Tuple(values,plan_ ->OutputSchema()),left_child_tuple.GetRid());
      }
    }
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cur_ < join_tuples_.size()) {
    *tuple = join_tuples_[cur_].first;
    *rid = join_tuples_[cur_].second;
    cur_++;
    return true;
  }
  return false;
}

}  // namespace bustub
