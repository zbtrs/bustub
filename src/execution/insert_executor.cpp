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
    : AbstractExecutor(exec_ctx), plan_(plan) {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan->TableOid());
  index_info_ = exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_ = std::move(child_executor);
}

void InsertExecutor::Init() {
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  RID tuple_rid;
  if (plan_->IsRawInsert()) {
    auto tuples = plan_->RawValues();
    for (auto &value : tuples) {
      auto made_tuple = Tuple(value, &(table_info_->schema_));
      // insert into table
      if (!table_info_->table_->InsertTuple(made_tuple, &tuple_rid, exec_ctx_->GetTransaction())) {
        return false;
      }
      for (auto index_info : index_info_) {
        const auto index_tuple =
            made_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        // 这里注意看参数名
        index_info->index_->InsertEntry(index_tuple, tuple_rid, exec_ctx_->GetTransaction());
      }
    }
  } else {
    Tuple child_tuple;
    while (child_executor_->Next(&child_tuple, &tuple_rid)) {
      if (!table_info_->table_->InsertTuple(child_tuple, &tuple_rid, exec_ctx_->GetTransaction())) {
        return false;
      }
      for (auto index_info : index_info_) {
        const auto index_tuple =
            child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(index_tuple, tuple_rid, exec_ctx_->GetTransaction());
      }
    }
  }
  return false;
}

}  // namespace bustub
