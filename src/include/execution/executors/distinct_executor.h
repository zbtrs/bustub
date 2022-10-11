//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.h
//
// Identification: src/include/execution/executors/distinct_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>
#include <unordered_map>

#include "common/util/hash_util.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/distinct_plan.h"

namespace bustub {
struct DistinctKey {
  std::vector<Value> distinct_keys_;
  bool operator==(const DistinctKey &other) const {
    for (uint32_t i = 0; i < other.distinct_keys_.size(); i++) {
      if (distinct_keys_[i].CompareEquals(other.distinct_keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace bustub

namespace std {

template <>
struct hash<bustub::DistinctKey> {
  auto operator()(const bustub::DistinctKey &dis_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : dis_key.distinct_keys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

/**
 * DistinctExecutor removes duplicate rows from child ouput.
 */
class DistinctExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DistinctExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The limit plan to be executed
   * @param child_executor The child executor from which tuples are pulled
   */
  DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the distinct */
  void Init() override;

  /**
   * Yield the next tuple from the distinct.
   * @param[out] tuple The next tuple produced by the distinct
   * @param[out] rid The next tuple RID produced by the distinct
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the distinct */
  auto GetOutputSchema() -> const Schema * override { return plan_->OutputSchema(); };

 private:
  /** The distinct plan node to be executed */
  const DistinctPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::unordered_map<DistinctKey, Tuple> hash_table_;
};
}  // namespace bustub
