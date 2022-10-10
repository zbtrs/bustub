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
    : AbstractExecutor(exec_ctx),plan_(plan),left_child_(std::move(left_child)),right_child_(std::move(right_child)) {}

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

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { return false; }

}  // namespace bustub
