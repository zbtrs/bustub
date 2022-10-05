//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) :
                                                                                           AbstractExecutor(exec_ctx),plan_(plan),table_info_(exec_ctx ->GetCatalog() ->GetTable(plan ->GetTableOid())),
                                                                                           iter_(table_info_ ->table_ ->Begin(exec_ctx ->GetTransaction())) {}

void SeqScanExecutor::Init() {}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto end_iter = table_info_ ->table_ ->End();
  while (iter_ != end_iter) {
    if (plan_ ->GetPredicate()) {
      if (plan_->GetPredicate()->Evaluate(&(*iter_), GetOutputSchema()).GetAs<bool>()) {
        *tuple = *iter_;
        iter_++;
        *rid = tuple->GetRid();
        return true;
      }
    } else {
      *tuple = *iter_;
      iter_++;
      return true;
    }
    iter_++;
  }
  return false;
}

}  // namespace bustub
