//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),plan_(plan),left_child_(std::move(left_child)),
      right_child_(std::move(right_child)),hash_page_(nullptr),is_scanned_(false) {}

void HashJoinExecutor::Init() {
  left_child_ ->Init();
  right_child_ ->Init();
  // build hash table
  Tuple left_tuple;
  RID left_rid;
  while (left_child_ ->Next(&left_tuple,&left_rid)) {
    auto value = plan_ ->LeftJoinKeyExpression() ->Evaluate(&left_tuple,plan_ ->GetLeftPlan() ->OutputSchema());
    HashJoinKey hash_join_key{value}; //可以直接这样初始化？
    if (hash_table_.count(hash_join_key) > 0) {
      hash_table_[hash_join_key].emplace_back(std::move(left_tuple));
    } else {
      hash_table_.insert({hash_join_key,{left_tuple}});
    }
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!is_scanned_) {
    is_scanned_ = true;
    if (!right_child_ ->Next(&unhashed_tuple_,rid)) {
      return false;
    }
    unhashed_tuple_key_ = HashJoinKey{plan_ ->RightJoinKeyExpression() ->Evaluate(&unhashed_tuple_,plan_ ->GetRightPlan() ->OutputSchema())};
    hash_page_ = &hash_table_[unhashed_tuple_key_];
    cur_ = hash_page_ ->begin();
  }

  while (true) {
    Tuple left_tuple;
    while (cur_ != hash_page_ ->end()) {
      auto left_join_key = HashJoinKey{plan_ ->LeftJoinKeyExpression() ->Evaluate(&*cur_,plan_ ->GetLeftPlan() ->OutputSchema())};
      left_tuple = *cur_;
      cur_++;
      if (left_join_key == unhashed_tuple_key_) {
        std::vector<Value> values;
        values.reserve(plan_ ->OutputSchema() ->GetColumnCount());
        for (const auto &column : plan_ ->OutputSchema() ->GetColumns()) {
          auto column_expr = dynamic_cast<const ColumnValueExpression*>(column.GetExpr());
          if (column_expr ->GetTupleIdx() == 0) {
            values.push_back(left_tuple.GetValue(plan_ ->GetLeftPlan() ->OutputSchema(),column_expr ->GetColIdx()));
          } else {
            values.push_back(unhashed_tuple_.GetValue(plan_ ->GetRightPlan() ->OutputSchema(),column_expr ->GetColIdx()));
          }
        }
        *tuple = Tuple(values,plan_ ->OutputSchema());
        *rid = left_tuple.GetRid();
        return true;
      }
    }
    if (!right_child_ ->Next(&unhashed_tuple_,rid)) {
      return false;
    }
    unhashed_tuple_key_ = HashJoinKey{plan_ ->RightJoinKeyExpression() ->Evaluate(&unhashed_tuple_,plan_ ->GetRightPlan() ->OutputSchema())};
    hash_page_ = &hash_table_[unhashed_tuple_key_];
    cur_ = hash_page_ ->begin();
  }
}

}  // namespace bustub
