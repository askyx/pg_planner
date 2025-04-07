#include "pg_optimizer/implementation_rules.h"

#include <memory>

#include "common/exception.h"
#include "common/macros.h"
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
#include <access/sdir.h>
#include <catalog/pg_am.h>
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
  return !get.relation_info->relation_indexes.empty();
}

std::pair<ExprArray, ExprArray> Get2IndexScan::ExtractIndexPredicates(const ExprArray &predicates,
                                                                      const IndexInfo *index_info) const {
  ExprArray match_clause;
  ExprArray non_match_clause;
  for (const auto &predicate : predicates) {
    auto col_used = predicate->DeriveUsedColumns();
    /*
     index(a, b)
      1. a = c  x
      2. a = b  maybe index scan , but pg not, test which is faster
      3. a = 5
    */
    if (!col_used.empty() && ContainsAll(ColRefArrayToSet(index_info->index_cols), col_used)) {
      // if is boolean type and `where a`, transform to `a = true`, `not a` to `a = false`
      // `where a is true` -> `a = true`
      // TODO: what if is a expr `where a | false` ?
      if (predicate->NodeIs<ItemIdent>() && predicate->Cast<ItemIdent>().colref->type == BOOLOID) {
        throw OptException("TODO: support boolean type index scan");
      }
      if (IsNotExpr(predicate)) {
        // TODO: support boolean type index scan
        throw OptException("TODO: support boolean type index scan");
      }

      // `x=y`
      // TODO: function index support
      if (predicate->NodeIs<ItemOpExpr>()) {
        if (predicate->children.size() < 2)
          non_match_clause.emplace_back(predicate);
        else {
          const auto &op_expr = predicate->Cast<ItemOpExpr>();
          const auto &left_op = op_expr.GetChild(0);
          const auto &right_op = op_expr.GetChild(1);
          bool matched = false;
          if (left_op->NodeIs<ItemIdent>()) {
            const auto &left_col = left_op->Cast<ItemIdent>();
            for (const auto *index_col : index_info->index_cols) {
              if (*left_col.colref == *index_col) {
                match_clause.emplace_back(predicate);
                matched = true;
                break;
              }
            }
          } else if (right_op->NodeIs<ItemIdent>()) {
            const auto &right_col = right_op->Cast<ItemIdent>();
            for (const auto *index_col : index_info->index_cols) {
              if (*right_col.colref == *index_col) {
                match_clause.emplace_back(predicate);
                matched = true;
                break;
              }
            }
          }

          if (!matched)
            non_match_clause.emplace_back(predicate);
        }
      }
    } else {
      non_match_clause.emplace_back(predicate);
    }
  }
  return {match_clause, non_match_clause};
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
  const auto &relation_indexes = get.relation_info->relation_indexes;
  // 1. get sort property
  std::shared_ptr<OrderSpec> order_spec = nullptr;
  if (auto sort = context->GetRequiredProperties()->GetPropertyOfType(PropertyType::SORT); sort)
    order_spec = sort->As<PropertySort>()->GetSortSpec();

  // 2. if has condition, check if it can be pushed down to index
  if (get.filter) {
    for (const auto &[index_oid, index] : relation_indexes) {
      ScanDirection scan_direction = ForwardScanDirection;
      if (index.relam == BTREE_AM_OID && order_spec &&
          ColRefSetIntersects(order_spec->GetUsedColumns(), ColRefArrayToSet(index.index_cols)))
        scan_direction = index.GetScanDirection(order_spec);
      // support more expression type and index type
      // 1. distrubuted condition into match and non-match part
      auto condition_array = OperatorUtils::PdrgpexprConjuncts(get.filter);
      auto [match_clause, non_match_clause] = ExtractIndexPredicates(condition_array, &index);
      if (match_clause.empty())
        continue;

      const auto &index_cols = index.GetIndexOutCols();
      const auto &output = pexpr->DeriveOutputColumns();
      const auto &order = index.ConstructOrderSpec();
      if (ContainsAll(index_cols, output))
        pxfres.emplace_back(MakeOperatorNode(std::make_shared<PhysicalIndexOnlyScan>(
            get.table_desc, get.relation_info,
            non_match_clause.empty() ? nullptr : OperatorUtils::PexprConjunction(non_match_clause), index_oid,
            scan_direction, order, OperatorUtils::PexprConjunction(match_clause))));
      else
        pxfres.emplace_back(MakeOperatorNode(std::make_shared<PhysicalIndexScan>(
            get.table_desc, get.relation_info,
            non_match_clause.empty() ? nullptr : OperatorUtils::PexprConjunction(non_match_clause), index_oid,
            scan_direction, order, OperatorUtils::PexprConjunction(match_clause))));
    }
  } else if (order_spec) {  // 3. if no condition, check if sort is match
    for (const auto &[index_oid, index] : relation_indexes) {
      // sort only support btree index
      if (index.relam == BTREE_AM_OID &&
          ColRefSetIntersects(order_spec->GetUsedColumns(), ColRefArrayToSet(index.index_cols))) {
        // TODO: get lower cost path
        // index (a, b), index(a) select order by (a)
        //  shoulde choose index (a)
        //  INDEX: create index tx on ta(a desc nulls first)
        // TODO: pg support define one index for multiple time, only choose one index for one time
        if (auto scan_direction = index.GetScanDirection(order_spec); scan_direction != NoMovementScanDirection) {
          const auto &index_cols = index.GetIndexOutCols();
          const auto &output = pexpr->DeriveOutputColumns();
          const auto &order = index.ConstructOrderSpec();
          OLOG("index def: {}\noutput: {}\n", ColRefContainerToString(index_cols), ColRefContainerToString(output));
          if (ContainsAll(index_cols, output))
            pxfres.emplace_back(MakeOperatorNode(std::make_shared<PhysicalIndexOnlyScan>(
                get.table_desc, get.relation_info, nullptr, index_oid, scan_direction, order, nullptr)));
          else
            pxfres.emplace_back(MakeOperatorNode(std::make_shared<PhysicalIndexScan>(
                get.table_desc, get.relation_info, nullptr, index_oid, scan_direction, order, nullptr)));
        }
      }
    }
  }
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