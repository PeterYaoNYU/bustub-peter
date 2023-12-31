//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

#include "catalog/schema.h"

#include <unordered_map>
#include <queue>

// do not include common/hash_util.h, because it can lead to redefiniton error
#include "common/util/hash_util.h"
#include "type/value_factory.h"

namespace bustub {

// this is the key used in the hash table for hash join
struct HashJoinKey {
  Value key_;

  auto operator==(const HashJoinKey &other) const -> bool { return key_.CompareEquals(other.key_) == CmpBool::CmpTrue; }
};

struct CompositeJoinKey {
  std::vector<HashJoinKey> keys;

  bool operator==(const CompositeJoinKey &other) const {
    if (keys.size() != other.keys.size()) {
      return false;
    }
    for (size_t i = 0; i < keys.size(); ++i) {
      if (!(keys[i] == other.keys[i])) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace bustub

// creating a hash function for HashJoinKey
namespace std{
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &key) const -> size_t {
    return bustub::HashUtil::HashValue(&key.key_);
  }
};


template <>
struct hash<bustub::CompositeJoinKey> {
  size_t operator()(const bustub::CompositeJoinKey &compositeKey) const {
    size_t curr_hash = 0;
    for (const auto &key : compositeKey.keys) {
      if (!key.key_.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key.key_));
      }
    }
    return curr_hash;
  }
};
}

namespace bustub {
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:

  auto MakeHashJoinLeftKey (const Tuple *tuple, const Schema &schema) -> std::vector<HashJoinKey>  {
    std::vector<HashJoinKey> keys;

    auto left_expr = plan_->LeftJoinKeyExpressions();
    keys.reserve(left_expr.size());

    for (auto &expr : left_expr) {
      keys.push_back({expr->Evaluate(tuple, schema)});
    }
    return keys;
  }

  auto MakeHashJoinRightKey (const Tuple *tuple, const Schema &schema) -> std::vector<HashJoinKey>  {
    std::vector<HashJoinKey> keys;

    auto right_expr = plan_->RightJoinKeyExpressions();
    keys.reserve(right_expr.size());

    for (auto &expr : right_expr) {
      keys.push_back({expr->Evaluate(tuple, schema)});
    }
    return keys;
  }

  auto InnerJoinOutput(const Tuple &left, const Tuple &right) -> Tuple;

  auto LeftOuterJoinOutput(const Tuple &left) -> Tuple;

  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  /** The child executor that produces tuples for the left side of join. */
  std::unique_ptr<AbstractExecutor> left_child_;

  /** The child executor that produces tuples for the right side of join. */
  std::unique_ptr<AbstractExecutor> right_child_;

  // a vector of hash table, one for each join key on the left side
  // the hashed value is a vector of tuples, because there can be multiple tuples with the same hash key
  std::vector<std::unordered_map<HashJoinKey, std::vector<Tuple>>> hash_tables_;

  // the htable for telling if a left join hashkey has a mathcing right tuple, used in left join
  // std::unordered_map<HashJoinKey, bool> left_done_;
  std::unordered_map<CompositeJoinKey, bool, std::hash<CompositeJoinKey>> left_done_;

  std::queue<Tuple> queue_;

  bool left_join_check_ = false;

  std::vector<Tuple> left_tuples_;
};

}  // namespace bustub
