#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <utility>

#include "common/hash_util.h"
#include "pg_operator/item_expr.h"
#include "pg_operator/operator.h"
#include "pg_operator/operator_node.h"
#include "pg_optimizer/colref.h"
#include "pg_optimizer/order_spec.h"

extern "C" {
#include <nodes/nodes.h>
#include <nodes/parsenodes.h>
#include <nodes/pathnodes.h>
}

namespace pgp {

class LogicalOperator : public Operator {
 public:
  constexpr static OperatorType TYPE = OperatorType::Invalid;

  explicit LogicalOperator(OperatorType type) : Operator(type) {}

  bool Logical() const override { return true; }
};

class LogicalLimit : public LogicalOperator {
 public:
  constexpr static OperatorType TYPE = OperatorType::LogicalLimit;

  std::shared_ptr<OrderSpec> order_spec;

  ItemExprPtr limit;

  ItemExprPtr offset;

  LogicalLimit(std::shared_ptr<OrderSpec> order_spec, ItemExprPtr limit, ItemExprPtr offset)
      : LogicalOperator(OperatorType::LogicalLimit),
        order_spec(std::move(order_spec)),
        limit(std::move(limit)),
        offset(std::move(offset)) {}

  hash_t Hash() const override;

  bool operator==(const Operator &other) const override;
};

class LogicalJoin : public LogicalOperator {
 public:
  constexpr static OperatorType TYPE = OperatorType::LogicalJoin;

  JoinType join_type;

  ItemExprPtr filter;

  LogicalJoin(JoinType join_type, ItemExprPtr filter)
      : LogicalOperator(OperatorType::LogicalJoin), join_type(join_type), filter(std::move(filter)) {}

  hash_t Hash() const override;

  bool operator==(const Operator &other) const override;
};

class LogicalGet : public LogicalOperator {
 public:
  constexpr static OperatorType TYPE = OperatorType::LogicalGet;

  RangeTblEntry *table_desc;

  ColRefArray output_columns;

  ItemExprPtr filter{nullptr};

  LogicalGet(RangeTblEntry *table_desc, ColRefArray output_columns)
      : LogicalOperator(OperatorType::LogicalGet), table_desc(table_desc), output_columns(std::move(output_columns)) {}

  hash_t Hash() const override;

  bool operator==(const Operator &other) const override;
};

class LogicalApply : public LogicalOperator {
 public:
  constexpr static OperatorType TYPE = OperatorType::LogicalApply;

  ColRefArray expr_refs;

  SubLinkType subquery_type;

  bool is_not_subquery{false};

  ItemExprPtr filter;

  LogicalApply(ColRefArray expr_refs, SubLinkType subquery_type, ItemExprPtr filter)
      : LogicalOperator(OperatorType::LogicalApply),
        expr_refs(std::move(expr_refs)),
        subquery_type(subquery_type),
        filter(std::move(filter)) {}

  hash_t Hash() const override;

  bool operator==(const Operator &other) const override;
};

class LogicalGbAgg : public LogicalOperator {
 public:
  constexpr static OperatorType TYPE = OperatorType::LogicalGbAgg;

  ColRefArray group_columns;

  ExprArray project_exprs;

  LogicalGbAgg(ColRefArray group_columns, ExprArray project_exprs)
      : LogicalOperator(OperatorType::LogicalGbAgg),
        group_columns(std::move(group_columns)),
        project_exprs(std::move(project_exprs)) {}

  hash_t Hash() const override;

  bool operator==(const Operator &other) const override;
};

class LogicalFilter : public LogicalOperator {
 public:
  constexpr static OperatorType TYPE = OperatorType::LogicalFilter;

  ItemExprPtr filter;

  explicit LogicalFilter(ItemExprPtr filter)
      : LogicalOperator(OperatorType::LogicalFilter), filter(std::move(filter)) {}

  hash_t Hash() const override;

  bool operator==(const Operator &other) const override;
};

class LogicalProject : public LogicalOperator {
 public:
  constexpr static OperatorType TYPE = OperatorType::LogicalProject;

  ExprArray project_exprs;

  explicit LogicalProject(ExprArray project_exprs)
      : LogicalOperator(OperatorType::LogicalProject), project_exprs(std::move(project_exprs)){};

  hash_t Hash() const override;

  bool operator==(const Operator &other) const override;
};

}  // namespace pgp