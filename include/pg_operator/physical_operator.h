#pragma once

#include <unordered_map>
#include <utility>

#include "pg_operator/item_expr.h"
#include "pg_operator/operator.h"
#include "pg_operator/operator_node.h"
#include "pg_optimizer/colref.h"
#include "pg_optimizer/order_spec.h"

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

  OrderSpec *order_spec;

  explicit PhysicalSort(OrderSpec *pos) : PhysicalOperator(OperatorType::PhysicalSort), order_spec(pos) {}

  hash_t Hash() const override;

  bool operator==(const Operator &other) const override;
};

class PhysicalScan : public PhysicalOperator {
 public:
  constexpr static OperatorType TYPE = OperatorType::PhysicalScan;

  RangeTblEntry *table_desc;

  ColRefArray output_columns;

  std::unordered_map<uint32_t, int> colid2attno;

  ItemExprPtr filter;

  PhysicalScan(RangeTblEntry *table_desc, ColRefArray output_columns, std::unordered_map<uint32_t, int> colid2attno,
               ItemExprPtr filter)
      : PhysicalOperator(OperatorType::PhysicalScan),
        table_desc(table_desc),
        output_columns(std::move(output_columns)),
        colid2attno(std::move(colid2attno)),
        filter(std::move(filter)) {}

  hash_t Hash() const override;

  bool operator==(const Operator &other) const override;
};

class PhysicalLimit : public PhysicalOperator {
 public:
  constexpr static OperatorType TYPE = OperatorType::PhysicalLimit;

  OrderSpec *order_spec;

  ItemExprPtr limit;
  ItemExprPtr offset;

  explicit PhysicalLimit(OrderSpec *pos, ItemExprPtr limit, ItemExprPtr offset)
      : PhysicalOperator(OperatorType::PhysicalLimit),
        order_spec(pos),
        limit(std::move(limit)),
        offset(std::move(offset)) {}

  hash_t Hash() const override;

  bool operator==(const Operator &other) const override;
};

class PhysicalApply : public PhysicalOperator {
 public:
  constexpr static OperatorType TYPE = OperatorType::PhysicalApply;

  ColRefArray expr_refs;

  SubQueryType subquery_type;

  bool is_not_subquery;

  ItemExprPtr filter;

  PhysicalApply(ColRefArray expr_refs, SubQueryType subquery_type, bool is_not_subquery, ItemExprPtr filter)
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

  OrderSpec *order_spec;

  static OrderSpec *PosCovering(OrderSpec *pos_required, const ColRefArray &pdrgpcr_grp);

  PhysicalStreamAgg(ColRefArray colref_array, ExprArray project_exprs);

  hash_t Hash() const override;

  bool operator==(const Operator &other) const override;
};

class PhysicalJoin : public PhysicalOperator {
 public:
  constexpr static OperatorType TYPE = OperatorType::Invalid;

  JoinType join_type;

  ItemExprPtr filter;

  static bool FPredKeysSeparated(OperatorNode *pexpr_inner, OperatorNode *pexpr_outer,
                                 const ItemExprPtr &pexpr_pred_inner, const ItemExprPtr &pexpr_pred_outer);

  static bool FHashJoinCompatible(const ItemExprPtr &pexpr_pred, OperatorNode *pexpr_outer, OperatorNode *pexpr_inner);

  static void AlignJoinKeyOuterInner(const ItemExprPtr &pexpr_pred, OperatorNode *pexpr_outer,
                                     OperatorNode *pexpr_inner, ItemExprPtr *ppexpr_key_outer,
                                     ItemExprPtr *ppexpr_key_inner);

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