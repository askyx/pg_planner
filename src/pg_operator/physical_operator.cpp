#include "pg_operator/physical_operator.h"

#include <memory>
#include <utility>

#include "common/hash_util.h"
#include "pg_operator/item_expr.h"
#include "pg_operator/operator.h"
#include "pg_operator/operator_node.h"
#include "pg_operator/operator_utils.h"
#include "pg_optimizer/colref.h"

extern "C" {
#include "utils/typcache.h"
}

namespace pgp {

hash_t PhysicalSort::Hash() const {
  auto hash = Operator::Hash();
  hash = HashUtil::CombineHashes(hash, order_spec->Hash());
  return hash;
}

bool PhysicalSort::operator==(const Operator &other) const {
  if (Operator::operator==(other)) {
    const auto &sort_node = Cast<PhysicalSort>();
    return *order_spec == *sort_node.order_spec;
  }
  return false;
}

hash_t PhysicalScan::Hash() const {
  auto hash = HashUtil::CombineHashes(Operator::Hash(), HashUtil::Hash(table_desc));
  hash = HashUtil::CombineHashes(hash, ColRefContainerHash(output_columns));
  if (filter != nullptr)
    hash = HashUtil::CombineHashes(hash, filter->Hash());

  return hash;
}

bool PhysicalScan::operator==(const Operator &other) const {
  if (Operator::operator==(other)) {
    const auto &scan_node = Cast<PhysicalScan>();
    return table_desc->relid == scan_node.table_desc->relid && output_columns == scan_node.output_columns &&
           ((filter == nullptr && scan_node.filter == nullptr) || (filter != nullptr && *filter == *scan_node.filter));
  }
  return false;
}

hash_t PhysicalLimit::Hash() const {
  auto hash = Operator::Hash();
  hash = HashUtil::CombineHashes(hash, order_spec->Hash());
  if (limit != nullptr)
    hash = HashUtil::CombineHashes(hash, limit->Hash());
  if (offset != nullptr)
    hash = HashUtil::CombineHashes(hash, offset->Hash());
  return hash;
}

bool PhysicalLimit::operator==(const Operator &other) const {
  if (Operator::operator==(other)) {
    const auto &limit_node = Cast<PhysicalLimit>();

    return limit_node.order_spec == order_spec && *limit == *(limit_node.limit) && *offset == *(limit_node.offset);
  }

  return false;
}

hash_t PhysicalApply::Hash() const {
  auto hash = Operator::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(subquery_type));
  hash = HashUtil::CombineHashes(hash, ColRefContainerHash(expr_refs));
  return hash;
}

bool PhysicalApply::operator==(const Operator &other) const {
  if (Operator::operator==(other)) {
    const auto &apply_node = Cast<PhysicalApply>();
    return subquery_type == apply_node.subquery_type && expr_refs == apply_node.expr_refs;
  }

  return false;
}

hash_t PhysicalFilter::Hash() const {
  auto hash = Operator::Hash();
  hash = HashUtil::CombineHashes(hash, filter->Hash());
  return hash;
}

bool PhysicalFilter::operator==(const Operator &other) const {
  if (Operator::operator==(other)) {
    const auto &filter_node = Cast<PhysicalFilter>();
    return *filter == *filter_node.filter;
  }

  return false;
}

hash_t PhysicalComputeScalar::Hash() const {
  auto hash = Operator::Hash();
  for (const auto &project : project_exprs)
    hash = HashUtil::CombineHashes(hash, project->Hash());
  return hash;
}

bool PhysicalComputeScalar::operator==(const Operator &other) const {
  if (Operator::operator==(other)) {
    const auto &compute_scalar_node = Cast<PhysicalComputeScalar>();
    return project_exprs == compute_scalar_node.project_exprs;
  }

  return false;
}

hash_t PhysicalAgg::Hash() const {
  auto hash = Operator::Hash();
  for (auto *colref : group_columns)
    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(colref->Id()));

  for (const auto &project : project_exprs)
    hash = HashUtil::CombineHashes(hash, project->Hash());
  return hash;
}

bool PhysicalAgg::operator==(const Operator &other) const {
  if (Operator::operator==(other)) {
    const auto &agg_node = Cast<PhysicalAgg>();
    return group_columns == agg_node.group_columns && project_exprs == agg_node.project_exprs;
  }

  return false;
}

PhysicalStreamAgg::PhysicalStreamAgg(ColRefArray colref_array, ExprArray project_exprs)
    : PhysicalAgg(OperatorType::PhysicalStreamAgg, std::move(colref_array), std::move(project_exprs)) {
  order_spec = std::make_shared<OrderSpec>();
  for (auto *colref : group_columns) {
    auto *type_entry = lookup_type_cache(colref->RetrieveType(), TYPECACHE_LT_OPR);

    order_spec->AddSortElement({type_entry->lt_opr, colref, NullsOrder::EnullsLast});
  }
}

std::shared_ptr<OrderSpec> PhysicalStreamAgg::PosCovering(const std::shared_ptr<OrderSpec> &pos_required,
                                                          const ColRefArray &pdrgpcr_grp) {
  if (0 == pos_required->SortSize()) {
    // required order must be non-empty
    return nullptr;
  }

  // create a set of required sort columns
  auto pcrs_reqd = pos_required->GetUsedColumns();

  std::shared_ptr<OrderSpec> pos = nullptr;

  ColRefSet pcrs_grp_cols;
  AddColRef(pcrs_grp_cols, pdrgpcr_grp);
  if (ContainsAll(pcrs_grp_cols, pcrs_reqd)) {
    // required order columns are included in grouping columns, we can
    // construct a covering order spec
    pos = pos_required->Copy();

    // augment order with remaining grouping columns
    for (auto *colref : pdrgpcr_grp) {
      if (!pcrs_reqd.contains(colref)) {
        auto *type_entry = lookup_type_cache(colref->RetrieveType(), TYPECACHE_LT_OPR);
        auto mdid = type_entry->lt_opr;

        pos->AddSortElement({mdid, colref, NullsOrder::EnullsLast});
      }
    }
  }

  return pos;
}

hash_t PhysicalStreamAgg::Hash() const {
  auto hash = PhysicalAgg::Hash();
  hash = HashUtil::CombineHashes(hash, order_spec->Hash());
  return hash;
}

bool PhysicalStreamAgg::operator==(const Operator &other) const {
  if (PhysicalAgg::operator==(other)) {
    const auto &stream_agg_node = Cast<PhysicalStreamAgg>();
    return *order_spec == *stream_agg_node.order_spec;
  }

  return false;
}

bool PhysicalJoin::FPredKeysSeparated(OperatorNode *pexpr_inner, OperatorNode *pexpr_outer,
                                      const ItemExprPtr &pexpr_pred_inner, const ItemExprPtr &pexpr_pred_outer) {
  auto pcrs_used_pred_outer = pexpr_pred_outer->DeriveUsedColumns();
  auto pcrs_used_pred_inner = pexpr_pred_inner->DeriveUsedColumns();

  auto outer_refs = pexpr_outer->DeriveOutputColumns();
  auto pcrs_inner = pexpr_inner->DeriveOutputColumns();

  // make sure that each predicate child uses columns from a different join child
  // in order to reject predicates of the form 'X Join Y on f(X.a, Y.b) = 5'
  bool f_pred_outer_uses_join_outer_child =
      (0 < pcrs_used_pred_outer.size()) && ContainsAll(outer_refs, pcrs_used_pred_outer);
  bool f_pred_outer_uses_join_inner_child =
      (0 < pcrs_used_pred_outer.size()) && ContainsAll(pcrs_inner, pcrs_used_pred_outer);
  bool f_pred_inner_uses_join_outer_child =
      (0 < pcrs_used_pred_inner.size()) && ContainsAll(outer_refs, pcrs_used_pred_inner);
  bool f_pred_inner_uses_join_inner_child =
      (0 < pcrs_used_pred_inner.size()) && ContainsAll(pcrs_inner, pcrs_used_pred_inner);

  return (f_pred_outer_uses_join_outer_child && f_pred_inner_uses_join_inner_child) ||
         (f_pred_outer_uses_join_inner_child && f_pred_inner_uses_join_outer_child);
}

bool PhysicalJoin::FHashJoinCompatible(const ItemExprPtr &pexpr_pred, OperatorNode *pexpr_outer,
                                       OperatorNode *pexpr_inner) {
  ItemExprPtr pexpr_pred_outer = nullptr;
  ItemExprPtr pexpr_pred_inner = nullptr;
  if (OperatorUtils::FINDF(pexpr_pred)) {
    auto pexpr = pexpr_pred->GetChild(0);
    pexpr_pred_outer = pexpr->GetChild(0);
    pexpr_pred_inner = pexpr->GetChild(1);
  } else {
    return false;
  }

  return FPredKeysSeparated(pexpr_inner, pexpr_outer, pexpr_pred_inner, pexpr_pred_outer);
}

// Check for equality and INDFs in the predicates, and also aligns the expressions inner and outer keys with the
// predicates For example foo (a int, b int) and bar (c int, d int), will need to be aligned properly if the predicate
// is d = a)
void PhysicalJoin::AlignJoinKeyOuterInner(const ItemExprPtr &pexpr_pred, OperatorNode *pexpr_outer,
                                          OperatorNode *pexpr_inner, ItemExprPtr *ppexpr_key_outer,
                                          ItemExprPtr *ppexpr_key_inner) {
  ItemExprPtr pexpr_pred_outer = nullptr;
  ItemExprPtr pexpr_pred_inner = nullptr;

  // extract left & right children from pexpr_pred for all supported ops
  if (OperatorUtils::FINDF(pexpr_pred)) {
    auto pexpr = pexpr_pred->GetChild(0);
    pexpr_pred_outer = pexpr->GetChild(0);
    pexpr_pred_inner = pexpr->GetChild(1);
  } else {
    throw OptException("Invalid join expression in AlignJoinKeyOuterInner");
  }

  auto pcrs_outer = pexpr_outer->DeriveOutputColumns();
  auto pcrs_pred_outer = pexpr_pred_outer->DeriveUsedColumns();

  if (ContainsAll(pcrs_outer, pcrs_pred_outer)) {
    *ppexpr_key_outer = pexpr_pred_outer;

    *ppexpr_key_inner = pexpr_pred_inner;
  } else {
    *ppexpr_key_outer = pexpr_pred_inner;

    *ppexpr_key_inner = pexpr_pred_outer;
  }
}

hash_t PhysicalJoin::Hash() const {
  auto hash = Operator::Hash();
  hash = HashUtil::CombineHashes(hash, join_type);
  hash = HashUtil::CombineHashes(hash, filter->Hash());
  return hash;
}

bool PhysicalJoin::operator==(const Operator &other) const {
  if (Operator::operator==(other)) {
    const auto &join_node = Cast<PhysicalJoin>();
    return join_type == join_node.join_type && *filter == *join_node.filter;
  }

  return false;
}

hash_t PhysicalFullMergeJoin::Hash() const {
  auto hash = PhysicalJoin::Hash();
  for (const auto &clause : outer_merge_clauses)
    hash = HashUtil::CombineHashes(hash, clause->Hash());
  for (const auto &clause : inner_merge_clauses)
    hash = HashUtil::CombineHashes(hash, clause->Hash());

  return hash;
}

bool PhysicalFullMergeJoin::operator==(const Operator &other) const {
  if (PhysicalJoin::operator==(other)) {
    const auto &full_merge_join_node = Cast<PhysicalFullMergeJoin>();
    return outer_merge_clauses == full_merge_join_node.outer_merge_clauses &&
           inner_merge_clauses == full_merge_join_node.inner_merge_clauses;
  }

  return false;
}

}  // namespace pgp