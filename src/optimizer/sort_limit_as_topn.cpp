#include "execution/plans/limit_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule()
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (plan->GetType() == PlanType::Limit) {
    auto limit_plan = std::static_pointer_cast<const LimitPlanNode>(plan);

    auto child_plan = limit_plan->GetChildPlan();
    if (child_plan != nullptr && child_plan->GetType() == PlanType::Sort) {
      // this is important to use the optimized plan, for double topn optimization
      auto sort_plan = dynamic_cast<const SortPlanNode &>(*optimized_plan->GetChildAt(0));

      auto output_schema = limit_plan->OutputSchema();
      auto schema_ref = std::make_shared<const Schema>(output_schema);
      return std::make_shared<TopNPlanNode>(schema_ref, sort_plan.GetChildPlan(), sort_plan.GetOrderBy(),
                                            limit_plan->GetLimit());
    }
  }
  return optimized_plan;
}

}  // namespace bustub
