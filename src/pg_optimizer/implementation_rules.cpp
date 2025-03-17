#include "pg_optimizer/implementation_rules.h"

#include <memory>

#include "pg_operator/item_expr.h"
#include "pg_operator/logical_operator.h"
#include "pg_operator/operator.h"
#include "pg_operator/operator_node.h"
#include "pg_operator/operator_utils.h"
#include "pg_operator/physical_operator.h"
#include "pg_optimizer/colref.h"
#include "pg_optimizer/group_expression.h"
#include "pg_optimizer/pattern.h"
#include "pg_optimizer/rule.h"

extern "C" {
#include "nodes/nodes.h"
}

namespace pgp {

CXformGet2TableScan::CXformGet2TableScan() {
  match_pattern_ = new Pattern(OperatorType::LogicalGet);
  rule_type_ = ExfGet2TableScan;
}

void CXformGet2TableScan::Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const {
  const auto &pop_get = pexpr->Cast<LogicalGet>();

  pxfres.emplace_back(new OperatorNode(
      std::make_shared<PhysicalScan>(pop_get.table_desc, pop_get.output_columns, pop_get.colid2attno, pop_get.filter)));
}

CXformImplementLimit::CXformImplementLimit() {
  match_pattern_ = new Pattern(OperatorType::LogicalLimit);
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));

  rule_type_ = ExfImplementLimit;
}

void CXformImplementLimit::Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const {
  const auto &pop_limit = pexpr->Cast<LogicalLimit>();

  auto *pexpr_limit = new OperatorNode(
      std::make_shared<PhysicalLimit>(pop_limit.order_spec, pop_limit.limit, pop_limit.offset), pexpr->children);

  pxfres.emplace_back(pexpr_limit);
}

CXformSelect2Filter::CXformSelect2Filter() {
  match_pattern_ = new Pattern(OperatorType::LogicalSelect);
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  rule_type_ = ExfSelect2Filter;
}

void CXformSelect2Filter::Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const {
  const auto &pexpr_select = pexpr->Cast<LogicalFilter>();

  auto *pexpr_filter = new OperatorNode(std::make_shared<PhysicalFilter>(pexpr_select.filter), pexpr->children);

  pxfres.emplace_back(pexpr_filter);
}

CXformProject2ComputeScalar::CXformProject2ComputeScalar() {
  match_pattern_ = new Pattern(OperatorType::LogicalProject);
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  rule_type_ = ExfProject2ComputeScalar;
}

void CXformProject2ComputeScalar::Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const {
  const auto &project = pexpr->Cast<LogicalProject>();
  auto *pexpr_compute_scalar =
      new OperatorNode(std::make_shared<PhysicalComputeScalar>(project.project_exprs), pexpr->children);

  pxfres.emplace_back(pexpr_compute_scalar);
}

static void ImplementHashJoin(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr, JoinType join_type) {
  ExprArray pdrgpexpr_outer;

  const auto &logical_join = pexpr->Cast<LogicalJoin>();

  OperatorNode *pexpr_outer = pexpr->GetChild(0);
  OperatorNode *pexpr_inner = pexpr->GetChild(1);
  auto pexpr_scalar = logical_join.filter;

  for (const auto &pexpr_pred : CUtils::PdrgpexprConjuncts(pexpr_scalar)) {
    if (PhysicalJoin::FHashJoinCompatible(pexpr_pred, pexpr_outer, pexpr_inner)) {
      ItemExprPtr pexpr_pred_inner;
      ItemExprPtr pexpr_pred_outer;
      PhysicalJoin::AlignJoinKeyOuterInner(pexpr_pred, pexpr_outer, pexpr_inner, &pexpr_pred_outer, &pexpr_pred_inner);

      pdrgpexpr_outer.emplace_back(pexpr_pred_outer);
    }
  }

  // Add an alternative only if we found at least one hash-joinable predicate
  if (0 != pdrgpexpr_outer.size()) {
    auto *pexpr_result =
        new OperatorNode(std::make_shared<PhysicalHashJoin>(join_type, pexpr_scalar), {pexpr_outer, pexpr_inner});

    pxfres.emplace_back(pexpr_result);
  }
}

CXformImplementInnerJoin::CXformImplementInnerJoin() {
  match_pattern_ = new Pattern(OperatorType::LogicalJoin);
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  rule_type_ = ExfImplementInnerJoin;
}

bool CXformImplementInnerJoin::Check(GroupExpression *gexpr) const {
  const auto &logical_join = gexpr->Pop()->Cast<LogicalJoin>();
  return logical_join.join_type == JOIN_INNER;
}

void CXformImplementInnerJoin::Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const {
  const auto &logical_join = pexpr->Cast<LogicalJoin>();
  ImplementHashJoin(pxfres, pexpr, JOIN_INNER);

  auto *pexpr_binary =
      new OperatorNode(std::make_shared<PhysicalNLJoin>(JOIN_INNER, logical_join.filter), pexpr->children);
  pxfres.emplace_back(pexpr_binary);
}

CXformGbAgg2HashAgg::CXformGbAgg2HashAgg() {
  match_pattern_ = new Pattern(OperatorType::LogicalGbAgg);
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  rule_type_ = ExfGbAgg2HashAgg;
}

bool CXformGbAgg2HashAgg::Check(GroupExpression *gexpr) const {
  const auto &pop_agg = gexpr->Pop()->Cast<LogicalGbAgg>();
  return !pop_agg.group_columns.empty();
}

void CXformGbAgg2HashAgg::Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const {
  const auto &pop_agg = pexpr->Cast<LogicalGbAgg>();
  if (pop_agg.project_exprs.empty())
    return;

  auto *pexpr_alt = new OperatorNode(std::make_shared<PhysicalHashAgg>(pop_agg.group_columns, pop_agg.project_exprs),
                                     pexpr->children);

  pxfres.emplace_back(pexpr_alt);
}

CXformGbAgg2ScalarAgg::CXformGbAgg2ScalarAgg() {
  match_pattern_ = new Pattern(OperatorType::LogicalGbAgg);
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  rule_type_ = ExfGbAgg2ScalarAgg;
}

bool CXformGbAgg2ScalarAgg::Check(GroupExpression *gexpr) const {
  return 0 >= gexpr->Pop()->Cast<LogicalGbAgg>().group_columns.size();
}

void CXformGbAgg2ScalarAgg::Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const {
  const auto &pop_agg = pexpr->Cast<LogicalGbAgg>();

  auto *pexpr_alt = new OperatorNode(std::make_shared<PhysicalScalarAgg>(pop_agg.group_columns, pop_agg.project_exprs),
                                     pexpr->children);

  pxfres.emplace_back(pexpr_alt);
}

CXformGbAgg2StreamAgg::CXformGbAgg2StreamAgg() {
  match_pattern_ = new Pattern(OperatorType::LogicalGbAgg);
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  rule_type_ = ExfGbAgg2StreamAgg;
}

bool CXformGbAgg2StreamAgg::Check(GroupExpression *gexpr) const {
  const auto &pop_agg = gexpr->Pop()->Cast<LogicalGbAgg>();
  return 0 != pop_agg.group_columns.size();
}

void CXformGbAgg2StreamAgg::Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const {
  const auto &pop_agg = pexpr->Cast<LogicalGbAgg>();

  auto *pexpr_alt = new OperatorNode(std::make_shared<PhysicalStreamAgg>(pop_agg.group_columns, pop_agg.project_exprs),
                                     pexpr->children);

  pxfres.emplace_back(pexpr_alt);
}

CXformJoin2NLJoin::CXformJoin2NLJoin() {
  match_pattern_ = new Pattern(OperatorType::LogicalJoin);
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  rule_type_ = ExfJoin2NLJoin;
}

bool CXformJoin2NLJoin::Check(GroupExpression *gexpr) const {
  const auto &pop_join = gexpr->Pop()->Cast<LogicalJoin>();

  if (auto join_type = pop_join.join_type;
      join_type == JOIN_FULL || join_type == JOIN_RIGHT || join_type == JOIN_RIGHT_ANTI)
    return false;

  return true;
}

void CXformJoin2NLJoin::Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const {
  const auto &logical_join = pexpr->Cast<LogicalJoin>();

  auto *pexpr_binary =
      new OperatorNode(std::make_shared<PhysicalNLJoin>(logical_join.join_type, logical_join.filter), pexpr->children);

  pxfres.emplace_back(pexpr_binary);
}

CXformJoin2HashJoin::CXformJoin2HashJoin() {
  match_pattern_ = new Pattern(OperatorType::LogicalJoin);
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  rule_type_ = ExfJoin2HashJoin;
}

void CXformJoin2HashJoin::Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const {
  const auto &logical_join = pexpr->Cast<LogicalJoin>();

  ExprArray pdrgpexpr_outer;

  OperatorNode *pexpr_outer = pexpr->GetChild(0);
  OperatorNode *pexpr_inner = pexpr->GetChild(1);
  auto pexpr_scalar = logical_join.filter;

  for (const auto &pexpr_pred : CUtils::PdrgpexprConjuncts(pexpr_scalar)) {
    if (PhysicalJoin::FHashJoinCompatible(pexpr_pred, pexpr_outer, pexpr_inner)) {
      ItemExprPtr pexpr_pred_inner;
      ItemExprPtr pexpr_pred_outer;
      PhysicalJoin::AlignJoinKeyOuterInner(pexpr_pred, pexpr_outer, pexpr_inner, &pexpr_pred_outer, &pexpr_pred_inner);

      pdrgpexpr_outer.emplace_back(pexpr_pred_outer);
    }
  }

  // construct new HashJoin expression using explicit casting, if needed

  // Add an alternative only if we found at least one hash-joinable predicate
  if (0 != pdrgpexpr_outer.size()) {
    auto *pexpr_result = new OperatorNode(std::make_shared<PhysicalHashJoin>(logical_join.join_type, pexpr_scalar),
                                          {pexpr_outer, pexpr_inner});

    pxfres.emplace_back(pexpr_result);
  }
}

CXformImplementApply::CXformImplementApply() {
  match_pattern_ = new Pattern(OperatorType::LogicalApply);
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));

  rule_type_ = ExfImplementApply;
}

void CXformImplementApply::Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const {
  const auto &pop_apply = pexpr->Cast<LogicalApply>();

  auto *pexpr_physical_apply =
      new OperatorNode(std::make_shared<PhysicalApply>(pop_apply.expr_refs, pop_apply.subquery_type,
                                                       pop_apply.is_not_subquery, pop_apply.filter),
                       pexpr->children);

  pxfres.emplace_back(pexpr_physical_apply);
}

}  // namespace pgp