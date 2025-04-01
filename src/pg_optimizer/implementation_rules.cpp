#include "pg_optimizer/implementation_rules.h"

#include <memory>

#include "pg_catalog/relation_info.h"
#include "pg_operator/item_expr.h"
#include "pg_operator/logical_operator.h"
#include "pg_operator/operator.h"
#include "pg_operator/operator_node.h"
#include "pg_operator/operator_utils.h"
#include "pg_operator/physical_operator.h"
#include "pg_optimizer/colref.h"
#include "pg_optimizer/group_expression.h"
#include "pg_optimizer/optimization_context.h"
#include "pg_optimizer/pattern.h"
#include "pg_optimizer/properties.h"
#include "pg_optimizer/property.h"
#include "pg_optimizer/rule.h"

extern "C" {
#include <nodes/nodes.h>
}

namespace pgp {

Get2TableScan::Get2TableScan() {
  match_pattern_ = new Pattern(OperatorType::LogicalGet);
  rule_type_ = ExfGet2TableScan;
}

void Get2TableScan::Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr,
                              OptimizationContext *context) const {
  const auto &get = pexpr->Cast<LogicalGet>();

  pxfres.emplace_back(MakeOperatorNode(std::make_shared<PhysicalScan>(get.table_desc, get.relation_info, get.filter)));
}

Get2IndexScan::Get2IndexScan() {
  match_pattern_ = new Pattern(OperatorType::LogicalGet);
  rule_type_ = ExfGet2IndexScan;
}

bool Get2IndexScan::Check(GroupExpression *gexpr) const {
  const auto &get = gexpr->Pop()->Cast<LogicalGet>();
  return !get.relation_info->index_list.empty();
}

// TODO: support more index type, check create_index_paths for more info
// 1. btree   sorted index
// 2. hash    eq index
// 3. gin
// 4. gist
// 5. spgist
// bitmap index
void Get2IndexScan::Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr,
                              OptimizationContext *context) const {
  const auto &get = pexpr->Cast<LogicalGet>();
  const auto &index_list = get.relation_info->index_list;
  auto sort = context->GetRequiredProperties()->GetPropertyOfType(PropertyType::SORT);
  // 1. if no condition, check required sort property for btree index
  if (sort) {
    const auto *sort_prop = sort->As<PropertySort>();
    for (auto index : index_list) {
      auto path_key = index.GetScanDirection(sort_prop->GetSortSpec());
      // TODO: get lower cost path
      // index (a, b), index(a) select order by (a)
      //  shoulde choose index (a)
      //  INDEX: create index tx on ta(a desc nulls first)
      if (path_key != NoMovementScanDirection) {
        pxfres.emplace_back(MakeOperatorNode(std::make_shared<PhysicalIndexScan>(
            get.table_desc, get.relation_info, get.filter, index.index, path_key, sort_prop->GetSortSpec())));
      }
    }
  }

  // 2. if has condition, check if it can be pushed down to index
}

ImplementLimit::ImplementLimit() {
  match_pattern_ = new Pattern(OperatorType::LogicalLimit);
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));

  rule_type_ = ExfImplementLimit;
}

void ImplementLimit::Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr,
                               OptimizationContext *context) const {
  const auto &pop_limit = pexpr->Cast<LogicalLimit>();

  pxfres.emplace_back(MakeOperatorNode(
      std::make_shared<PhysicalLimit>(pop_limit.order_spec, pop_limit.limit, pop_limit.offset), pexpr->children));
}

Select2Filter::Select2Filter() {
  match_pattern_ = new Pattern(OperatorType::LogicalFilter);
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  rule_type_ = ExfSelect2Filter;
}

void Select2Filter::Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr,
                              OptimizationContext *context) const {
  const auto &pexpr_select = pexpr->Cast<LogicalFilter>();

  pxfres.emplace_back(MakeOperatorNode(std::make_shared<PhysicalFilter>(pexpr_select.filter), pexpr->children));
}

Project2ComputeScalarRule::Project2ComputeScalarRule() {
  match_pattern_ = new Pattern(OperatorType::LogicalProject);
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  rule_type_ = ExfProject2ComputeScalar;
}

void Project2ComputeScalarRule::Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr,
                                          OptimizationContext *context) const {
  const auto &project = pexpr->Cast<LogicalProject>();

  pxfres.emplace_back(
      MakeOperatorNode(std::make_shared<PhysicalComputeScalar>(project.project_exprs), pexpr->children));
}

static void ImplementHashJoin(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr, JoinType join_type) {
  ExprArray pdrgpexpr_outer;

  const auto &logical_join = pexpr->Cast<LogicalJoin>();

  auto pexpr_outer = pexpr->GetChild(0);
  auto pexpr_inner = pexpr->GetChild(1);
  auto pexpr_scalar = logical_join.filter;

  for (const auto &pexpr_pred : OperatorUtils::PdrgpexprConjuncts(pexpr_scalar)) {
    if (PhysicalJoin::FHashJoinCompatible(pexpr_pred, pexpr_outer, pexpr_inner)) {
      ItemExprPtr pexpr_pred_inner;
      ItemExprPtr pexpr_pred_outer;
      PhysicalJoin::AlignJoinKeyOuterInner(pexpr_pred, pexpr_outer, pexpr_inner, pexpr_pred_outer, pexpr_pred_inner);

      pdrgpexpr_outer.emplace_back(pexpr_pred_outer);
    }
  }

  // Add an alternative only if we found at least one hash-joinable predicate
  if (0 != pdrgpexpr_outer.size()) {
    pxfres.emplace_back(
        MakeOperatorNode(std::make_shared<PhysicalHashJoin>(join_type, pexpr_scalar), {pexpr_outer, pexpr_inner}));
  }
}

ImplementInnerJoin::ImplementInnerJoin() {
  match_pattern_ = new Pattern(OperatorType::LogicalJoin);
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  rule_type_ = ExfImplementInnerJoin;
}

bool ImplementInnerJoin::Check(GroupExpression *gexpr) const {
  const auto &logical_join = gexpr->Pop()->Cast<LogicalJoin>();
  return logical_join.join_type == JOIN_INNER;
}

void ImplementInnerJoin::Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr,
                                   OptimizationContext *context) const {
  const auto &logical_join = pexpr->Cast<LogicalJoin>();
  ImplementHashJoin(pxfres, pexpr, JOIN_INNER);

  pxfres.emplace_back(
      MakeOperatorNode(std::make_shared<PhysicalNLJoin>(JOIN_INNER, logical_join.filter), pexpr->children));
}

GbAgg2HashAgg::GbAgg2HashAgg() {
  match_pattern_ = new Pattern(OperatorType::LogicalGbAgg);
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  rule_type_ = ExfGbAgg2HashAgg;
}

bool GbAgg2HashAgg::Check(GroupExpression *gexpr) const {
  const auto &pop_agg = gexpr->Pop()->Cast<LogicalGbAgg>();
  return !pop_agg.group_columns.empty();
}

void GbAgg2HashAgg::Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr,
                              OptimizationContext *context) const {
  const auto &pop_agg = pexpr->Cast<LogicalGbAgg>();
  if (pop_agg.project_exprs.empty())
    return;

  pxfres.emplace_back(MakeOperatorNode(std::make_shared<PhysicalHashAgg>(pop_agg.group_columns, pop_agg.project_exprs),
                                       pexpr->children));
}

GbAgg2ScalarAgg::GbAgg2ScalarAgg() {
  match_pattern_ = new Pattern(OperatorType::LogicalGbAgg);
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  rule_type_ = ExfGbAgg2ScalarAgg;
}

bool GbAgg2ScalarAgg::Check(GroupExpression *gexpr) const {
  return 0 >= gexpr->Pop()->Cast<LogicalGbAgg>().group_columns.size();
}

void GbAgg2ScalarAgg::Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr,
                                OptimizationContext *context) const {
  const auto &pop_agg = pexpr->Cast<LogicalGbAgg>();

  pxfres.emplace_back(MakeOperatorNode(
      std::make_shared<PhysicalScalarAgg>(pop_agg.group_columns, pop_agg.project_exprs), pexpr->children));
}

GbAgg2StreamAgg::GbAgg2StreamAgg() {
  match_pattern_ = new Pattern(OperatorType::LogicalGbAgg);
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  rule_type_ = ExfGbAgg2StreamAgg;
}

bool GbAgg2StreamAgg::Check(GroupExpression *gexpr) const {
  const auto &pop_agg = gexpr->Pop()->Cast<LogicalGbAgg>();
  return 0 != pop_agg.group_columns.size();
}

void GbAgg2StreamAgg::Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr,
                                OptimizationContext *context) const {
  const auto &pop_agg = pexpr->Cast<LogicalGbAgg>();

  pxfres.emplace_back(MakeOperatorNode(
      std::make_shared<PhysicalStreamAgg>(pop_agg.group_columns, pop_agg.project_exprs), pexpr->children));
}

Join2NestedLoopJoin::Join2NestedLoopJoin() {
  match_pattern_ = new Pattern(OperatorType::LogicalJoin);
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  rule_type_ = ExfJoin2NLJoin;
}

bool Join2NestedLoopJoin::Check(GroupExpression *gexpr) const {
  const auto &pop_join = gexpr->Pop()->Cast<LogicalJoin>();

  if (auto join_type = pop_join.join_type;
      join_type == JOIN_FULL || join_type == JOIN_RIGHT || join_type == JOIN_RIGHT_ANTI)
    return false;

  return true;
}

void Join2NestedLoopJoin::Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr,
                                    OptimizationContext *context) const {
  const auto &logical_join = pexpr->Cast<LogicalJoin>();

  pxfres.emplace_back(
      MakeOperatorNode(std::make_shared<PhysicalNLJoin>(logical_join.join_type, logical_join.filter), pexpr->children));
}

Join2HashJoin::Join2HashJoin() {
  match_pattern_ = new Pattern(OperatorType::LogicalJoin);
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  rule_type_ = ExfJoin2HashJoin;
}

void Join2HashJoin::Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr,
                              OptimizationContext *context) const {
  const auto &logical_join = pexpr->Cast<LogicalJoin>();

  ExprArray pdrgpexpr_outer;

  auto pexpr_outer = pexpr->GetChild(0);
  auto pexpr_inner = pexpr->GetChild(1);
  auto pexpr_scalar = logical_join.filter;

  for (const auto &pexpr_pred : OperatorUtils::PdrgpexprConjuncts(pexpr_scalar)) {
    if (PhysicalJoin::FHashJoinCompatible(pexpr_pred, pexpr_outer, pexpr_inner)) {
      ItemExprPtr pexpr_pred_inner;
      ItemExprPtr pexpr_pred_outer;
      PhysicalJoin::AlignJoinKeyOuterInner(pexpr_pred, pexpr_outer, pexpr_inner, pexpr_pred_outer, pexpr_pred_inner);

      pdrgpexpr_outer.emplace_back(pexpr_pred_outer);
    }
  }

  // construct new HashJoin expression using explicit casting, if needed

  // Add an alternative only if we found at least one hash-joinable predicate
  if (0 != pdrgpexpr_outer.size()) {
    pxfres.emplace_back(MakeOperatorNode(std::make_shared<PhysicalHashJoin>(logical_join.join_type, pexpr_scalar),
                                         {pexpr_outer, pexpr_inner}));
  }
}

ImplementApply::ImplementApply() {
  match_pattern_ = new Pattern(OperatorType::LogicalApply);
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));

  rule_type_ = ExfImplementApply;
}

void ImplementApply::Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr,
                               OptimizationContext *context) const {
  const auto &pop_apply = pexpr->Cast<LogicalApply>();

  pxfres.emplace_back(MakeOperatorNode(std::make_shared<PhysicalApply>(pop_apply.expr_refs, pop_apply.subquery_type,
                                                                       pop_apply.is_not_subquery, pop_apply.filter),
                                       pexpr->children));
}

}  // namespace pgp