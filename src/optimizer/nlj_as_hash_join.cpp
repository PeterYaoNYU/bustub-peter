#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.push_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (plan->GetType() != PlanType::NestedLoopJoin) {
    return optimized_plan;
  }

  const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);

  BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NestedLoopJoin should have two children");

  auto output_schema = std::make_shared<const bustub::Schema>(nlj_plan.OutputSchema());


  if (const auto *expr = dynamic_cast<const ComparisonExpression*>(nlj_plan.Predicate().get()); expr!= nullptr){
    if (expr->comp_type_ == ComparisonType::Equal){
      if (const auto * left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get()); left_expr != nullptr){
        if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->GetChildAt(1).get()); right_expr != nullptr) {
          auto left_expr_tuple_0 = std::make_shared<ColumnValueExpression>(left_expr->GetTupleIdx(), left_expr->GetColIdx(), left_expr->GetReturnType());
          auto right_expr_tuple_0 = std::make_shared<ColumnValueExpression>(right_expr->GetTupleIdx(), right_expr->GetColIdx(), right_expr->GetReturnType());
          auto left_expr_tuples = std::vector<AbstractExpressionRef>{left_expr_tuple_0};
          auto right_expr_tuples = std::vector<AbstractExpressionRef>{right_expr_tuple_0};
          auto join_type = nlj_plan.GetJoinType();

          if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0){
            return std::make_shared<HashJoinPlanNode>(output_schema, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(), right_expr_tuples, left_expr_tuples, join_type);
          }
          if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1){
            return std::make_shared<HashJoinPlanNode>(output_schema, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(), left_expr_tuples, right_expr_tuples, join_type);
          }
        }
      }
    }    
  } else if (const auto *expr = dynamic_cast<const LogicExpression*>(nlj_plan.Predicate().get()); expr != nullptr && expr->logic_type_ == LogicType::And){
    std::vector <AbstractExpressionRef> left_expr_tuples;
    std::vector <AbstractExpressionRef> right_expr_tuples;

    std::function<void(const LogicExpression *)> process_logic_expr;
    // for this lambda function, we are capturing by reference
    process_logic_expr = [&] (const LogicExpression *logic_expr) {
      // printf("processing logic expression\n");
      if (const auto *left_logic_expr = dynamic_cast<const LogicExpression *>(logic_expr->GetChildAt(0).get()); left_logic_expr != nullptr){
        process_logic_expr(left_logic_expr);
      } else if (const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(logic_expr->GetChildAt(0).get()); cmp_expr != nullptr){
        if (cmp_expr->comp_type_ == ComparisonType::Equal){
          // printf("entering cmp expression\n");
          const auto *left_col_expr = dynamic_cast<const ColumnValueExpression *>(cmp_expr->GetChildAt(0).get());
          const auto *right_col_expr = dynamic_cast<const ColumnValueExpression *>(cmp_expr->GetChildAt(1).get());

          if (left_col_expr != nullptr && right_col_expr != nullptr){
            auto left_expr_tuple = std::make_shared<ColumnValueExpression>(left_col_expr->GetTupleIdx(), left_col_expr->GetColIdx(), left_col_expr->GetReturnType());
            auto right_expr_tuple = std::make_shared<ColumnValueExpression>(right_col_expr->GetTupleIdx(), right_col_expr->GetColIdx(), right_col_expr->GetReturnType());

            if (left_col_expr->GetTupleIdx() == 0 && right_col_expr->GetTupleIdx() == 1){
              left_expr_tuples.push_back(left_expr_tuple);
              right_expr_tuples.push_back(right_expr_tuple);
            } else if (left_col_expr->GetTupleIdx() == 1 && right_col_expr->GetTupleIdx() == 0){
              left_expr_tuples.push_back(right_expr_tuple);
              right_expr_tuples.push_back(left_expr_tuple);
            }
          }
        }
      }

      // now process the right child 
      if (const auto * right_logic_expr = dynamic_cast<const LogicExpression *>(logic_expr->GetChildAt(1).get()); right_logic_expr != nullptr){
        process_logic_expr(right_logic_expr);
      } else if (const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(logic_expr->GetChildAt(1).get()); cmp_expr != nullptr){
        if (cmp_expr->comp_type_ == ComparisonType::Equal){
          // printf("entering cmp expression again\n");
          const auto *left_col_expr = dynamic_cast<const ColumnValueExpression *>(cmp_expr->GetChildAt(0).get());
          const auto *right_col_expr = dynamic_cast<const ColumnValueExpression *>(cmp_expr->GetChildAt(1).get());

          if (left_col_expr != nullptr && right_col_expr != nullptr){
            auto left_expr_tuple = std::make_shared<ColumnValueExpression>(left_col_expr->GetTupleIdx(), left_col_expr->GetColIdx(), left_col_expr->GetReturnType());
            auto right_expr_tuple = std::make_shared<ColumnValueExpression>(right_col_expr->GetTupleIdx(), right_col_expr->GetColIdx(), right_col_expr->GetReturnType());

            if (left_col_expr->GetTupleIdx() == 0 && right_col_expr->GetTupleIdx() == 1){
              left_expr_tuples.push_back(left_expr_tuple);
              right_expr_tuples.push_back(right_expr_tuple);
            } else if (left_col_expr->GetTupleIdx() == 1 && right_col_expr->GetTupleIdx() == 0){
              left_expr_tuples.push_back(right_expr_tuple);
              right_expr_tuples.push_back(left_expr_tuple);
            }
          }
        }
      }

      // printf("finished processing logic expression\n Left tuple size: %lu\n Right tuple size: %lu\n", left_expr_tuples.size(), right_expr_tuples.size());
    };

    process_logic_expr(expr);

    // now that we have processed everything, we can create the plan node
    auto join_type = nlj_plan.GetJoinType();
    return std::make_shared<HashJoinPlanNode>(output_schema, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(), left_expr_tuples, right_expr_tuples, join_type);
  }
  return optimized_plan;
}

}  // namespace bustub
