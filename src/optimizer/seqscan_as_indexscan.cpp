#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule

  // 1. optimize all children node with this rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  // 2. check if the children is seqscan with filter predicate pushdown
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<SeqScanPlanNode &>(*optimized_plan);
    // if the seqscan has no filter predicate, return the original plan
    if (seq_scan_plan.filter_predicate_ == nullptr) {
      return optimized_plan;
    }
    auto cmp_expr = dynamic_cast<ComparisonExpression *>(seq_scan_plan.filter_predicate_.get());
    // check if the cmp_expr is an equi comparison
    if (cmp_expr == nullptr || cmp_expr->comp_type_ != ComparisonType::Equal) {
      return optimized_plan;
    }
    // check if there are matching index
    auto column_value_exprs = dynamic_cast<ColumnValueExpression *>(cmp_expr->GetChildAt(0).get());
    if (column_value_exprs == nullptr) {
      return optimized_plan;
    }

    // 3. get the index info
    auto indexes = catalog_.GetTableIndexes(seq_scan_plan.table_name_);
    if (indexes.empty()) {
      return optimized_plan;
    }

    // 4. check if the index is matching
    for (auto index_info : indexes) {
      if (index_info->index_->GetKeyAttrs().size() == 1 &&
          index_info->index_->GetKeyAttrs()[0] == column_value_exprs->GetColIdx()) {
        return std::make_shared<IndexScanPlanNode>(
            seq_scan_plan.output_schema_, seq_scan_plan.table_oid_, index_info->index_oid_,
            seq_scan_plan.filter_predicate_, dynamic_cast<ConstantValueExpression *>(cmp_expr->GetChildAt(1).get()));
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
