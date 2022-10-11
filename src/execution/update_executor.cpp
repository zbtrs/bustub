//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  child_executor_ = std::move(child_executor);
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan->TableOid());
  index_info_ = exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple;
  RID child_tuple_rid;
  std::vector<std::pair<Tuple, RID>> vec;
  while (child_executor_->Next(&child_tuple, &child_tuple_rid)) {
    table_info_->table_->MarkDelete(child_tuple_rid, exec_ctx_->GetTransaction());
    for (auto index_info : index_info_) {
      const auto index_tuple =
          child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(index_tuple, child_tuple_rid, exec_ctx_->GetTransaction());
    }
    vec.emplace_back(child_tuple, child_tuple_rid);
  }
  for (auto &tuple_pair : vec) {
    child_tuple = tuple_pair.first;
    child_tuple_rid = tuple_pair.second;
    RID new_tupld_rid;
    auto new_tuple = GenerateUpdatedTuple(child_tuple);
    table_info_->table_->InsertTuple(new_tuple, &new_tupld_rid, exec_ctx_->GetTransaction());
    for (auto index_info : index_info_) {
      const auto index_tuple =
          new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(index_tuple, new_tupld_rid, exec_ctx_->GetTransaction());
    }
  }

  return false;
}

auto UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) -> Tuple {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
