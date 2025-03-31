#pragma once

#include <utility>

#include "pg_catalog/relation_info.h"
#include "pg_operator/item_expr.h"
#include "pg_operator/operator.h"
#include "pg_operator/operator_node.h"
#include "pg_optimizer/colref.h"
#include "pg_optimizer/order_spec.h"
#include "postgres_ext.h"

extern "C" {
#include <nodes/nodes.h>

#include "nodes/parsenodes.h"
}
namespace pgp {

class PhysicalOperator : public Operator {
 public:
  explicit PhysicalOperator(OperatorType type) : Operator(type) {}

  bool Physical() const override { return true; }
};

class PhysicalSort : public PhysicalOperator {
 public:
  constexpr static OperatorType TYPE = OperatorType::PhysicalSort;

  std::shared_ptr<OrderSpec> order_spec;

  explicit PhysicalSort(std::shared_ptr<OrderSpec> pos)
      : PhysicalOperator(OperatorType::PhysicalSort), order_spec(std::move(pos)) {}

  hash_t Hash() const override;

  bool operator==(const Operator &other) const override;
};

class PhysicalScan : public PhysicalOperator {
 public:
  constexpr static OperatorType TYPE = OperatorType::PhysicalScan;

  RangeTblEntry *table_desc;

  RelationInfoPtr relation_info;

  ItemExprPtr filter;

  PhysicalScan(RangeTblEntry *table_desc, RelationInfoPtr relation_info, ItemExprPtr filter)
      : PhysicalOperator(OperatorType::PhysicalScan),
        table_desc(table_desc),
        relation_info(std::move(relation_info)),
        filter(std::move(filter)) {}

  hash_t Hash() const override;

  bool operator==(const Operator &other) const override;
};

class PhysicalIndexScan : public PhysicalScan {
 public:
  constexpr static OperatorType TYPE = OperatorType::PhysicalIndexScan;

  Oid index_id;

  ScanDirection scan_direction;

  std::shared_ptr<OrderSpec> order_spec;

  PhysicalIndexScan(RangeTblEntry *table_desc, RelationInfoPtr relation_info, ItemExprPtr filter, Oid index_oid,
                    ScanDirection scan_direction, std::shared_ptr<OrderSpec> order_spec)
      : PhysicalScan(table_desc, std::move(relation_info), std::move(filter)),
        index_id(index_oid),
        scan_direction(scan_direction),
        order_spec(std::move(order_spec)) {
    kind = OperatorType::PhysicalIndexScan;
  }

  hash_t Hash() const override;

  bool operator==(const Operator &other) const override;
};

class PhysicalLimit : public PhysicalOperator {
 public:
  constexpr static OperatorType TYPE = OperatorType::PhysicalLimit;

  std::shared_ptr<OrderSpec> order_spec;

  ItemExprPtr limit;
  ItemExprPtr offset;

  explicit PhysicalLimit(std::shared_ptr<OrderSpec> pos, ItemExprPtr limit, ItemExprPtr offset)
      : PhysicalOperator(OperatorType::PhysicalLimit),
        order_spec(std::move(pos)),
        limit(std::move(limit)),
        offset(std::move(offset)) {}

  hash_t Hash() const override;

  bool operator==(const Operator &other) const override;
};

class PhysicalApply : public PhysicalOperator {
 public:
  constexpr static OperatorType TYPE = OperatorType::PhysicalApply;

  ColRefArray expr_refs;

  SubLinkType subquery_type;

  bool is_not_subquery;

  ItemExprPtr filter;

  PhysicalApply(ColRefArray expr_refs, SubLinkType subquery_type, bool is_not_subquery, ItemExprPtr filter)
      : PhysicalOperator(OperatorType::PhysicalApply),
        expr_refs(std::move(expr_refs)),
        subquery_type(subquery_type),
        is_not_subquery(is_not_subquery),
        filter(std::move(filter)) {}

  hash_t Hash() const override;

  bool operator==(const Operator &other) const override;
};

class PhysicalFilter : public PhysicalOperator {
 public:
  constexpr static OperatorType TYPE = OperatorType::PhysicalFilter;

  ItemExprPtr filter;

  explicit PhysicalFilter(ItemExprPtr filter)
      : PhysicalOperator(OperatorType::PhysicalFilter), filter(std::move(filter)) {}

  hash_t Hash() const override;

  bool operator==(const Operator &other) const override;
};

class PhysicalComputeScalar : public PhysicalOperator {
 public:
  constexpr static OperatorType TYPE = OperatorType::PhysicalComputeScalar;

  ExprArray project_exprs;

  explicit PhysicalComputeScalar(ExprArray project_exprs)
      : PhysicalOperator(OperatorType::PhysicalComputeScalar), project_exprs(std::move(project_exprs)) {}

  hash_t Hash() const override;

  bool operator==(const Operator &other) const override;
};

class PhysicalAgg : public PhysicalOperator {
 public:
  constexpr static OperatorType TYPE = OperatorType::Invalid;

  ColRefArray group_columns;

  ExprArray project_exprs;

  PhysicalAgg(OperatorType type, ColRefArray group_columns, ExprArray project_exprs)
      : PhysicalOperator(type), group_columns(std::move(group_columns)), project_exprs(std::move(project_exprs)) {}

  hash_t Hash() const override;

  bool operator==(const Operator &other) const override;
};

class PhysicalHashAgg : public PhysicalAgg {
 public:
  constexpr static OperatorType TYPE = OperatorType::PhysicalHashAgg;

  PhysicalHashAgg(ColRefArray colref_array, ExprArray project_exprs)
      : PhysicalAgg(OperatorType::PhysicalHashAgg, std::move(colref_array), std::move(project_exprs)) {}
};

class PhysicalScalarAgg : public PhysicalAgg {
 public:
  constexpr static OperatorType TYPE = OperatorType::PhysicalScalarAgg;

  PhysicalScalarAgg(ColRefArray colref_array, ExprArray project_exprs)
      : PhysicalAgg(OperatorType::PhysicalScalarAgg, std::move(colref_array), std::move(project_exprs)) {}
};

class PhysicalStreamAgg : public PhysicalAgg {
 public:
  constexpr static OperatorType TYPE = OperatorType::PhysicalStreamAgg;

  std::shared_ptr<OrderSpec> order_spec;

  static std::shared_ptr<OrderSpec> PosCovering(const std::shared_ptr<OrderSpec> &pos_required,
                                                const ColRefArray &pdrgpcr_grp);

  PhysicalStreamAgg(ColRefArray colref_array, ExprArray project_exprs);

  hash_t Hash() const override;

  bool operator==(const Operator &other) const override;
};

class PhysicalJoin : public PhysicalOperator {
 public:
  constexpr static OperatorType TYPE = OperatorType::Invalid;

  JoinType join_type;

  ItemExprPtr filter;

  static bool FHashJoinCompatible(const ItemExprPtr &pexpr_pred, const OperatorNodePtr &pexpr_outer,
                                  const OperatorNodePtr &pexpr_inner);

  static void AlignJoinKeyOuterInner(const ItemExprPtr &pexpr_pred, const OperatorNodePtr &pexpr_outer,
                                     const OperatorNodePtr &pexpr_inner, ItemExprPtr &ppexpr_key_outer,
                                     ItemExprPtr &ppexpr_key_inner);

  PhysicalJoin(OperatorType type, JoinType join_type, ItemExprPtr filter)
      : PhysicalOperator(type), join_type(join_type), filter(std::move(filter)) {}

  hash_t Hash() const override;

  bool operator==(const Operator &other) const override;
};

class PhysicalNLJoin : public PhysicalJoin {
 public:
  constexpr static OperatorType TYPE = OperatorType::PhysicalNLJoin;

  PhysicalNLJoin(enum JoinType join_type, ItemExprPtr filter)
      : PhysicalJoin(OperatorType::PhysicalNLJoin, join_type, std::move(filter)) {}
};

class PhysicalHashJoin : public PhysicalJoin {
 public:
  constexpr static OperatorType TYPE = OperatorType::PhysicalHashJoin;

  PhysicalHashJoin(enum JoinType join_type, ItemExprPtr filter)
      : PhysicalJoin(OperatorType::PhysicalHashJoin, join_type, std::move(filter)) {}
};

class PhysicalFullMergeJoin : public PhysicalJoin {
 public:
  constexpr static OperatorType TYPE = OperatorType::PhysicalFullMergeJoin;

  ExprArray outer_merge_clauses;

  ExprArray inner_merge_clauses;

  PhysicalFullMergeJoin(ExprArray outer_merge_clauses, ExprArray inner_merge_clauses, ItemExprPtr filter)
      : PhysicalJoin(OperatorType::PhysicalFullMergeJoin, JOIN_INNER, std::move(filter)),
        outer_merge_clauses(std::move(outer_merge_clauses)),
        inner_merge_clauses(std::move(inner_merge_clauses)) {}

  hash_t Hash() const override;

  bool operator==(const Operator &other) const override;
};

}  // namespace pgp