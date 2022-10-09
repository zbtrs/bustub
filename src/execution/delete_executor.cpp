//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  child_executor_ = std::move(child_executor);
  table_info_ = exec_ctx ->GetCatalog() ->GetTable(plan ->TableOid());
  index_info_ = exec_ctx ->GetCatalog() ->GetTableIndexes(table_info_ ->name_);
}

void DeleteExecutor::Init() {
  child_executor_ ->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple;
  RID child_tuple_rid;
  if (!child_executor_ ->Next(&child_tuple,&child_tuple_rid)) {
    return false;
  }
  for (auto index_info : index_info_) {
    auto index_tuple = child_tuple.KeyFromTuple(table_info_ ->schema_, index_info->key_schema_,
                                                index_info->index_->GetKeyAttrs());
    std::vector<RID> rids{};
    index_info ->index_ ->ScanKey(index_tuple,&rids,exec_ctx_ ->GetTransaction());
    index_info ->index_ ->DeleteEntry(index_tuple,rids[0],exec_ctx_ ->GetTransaction());
  }
  return table_info_ ->table_ ->MarkDelete(child_tuple_rid,exec_ctx_ ->GetTransaction());
}

}  // namespace bustub
