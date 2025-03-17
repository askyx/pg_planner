#include "pg_operator/logical_operator.h"

#include "common/hash_util.h"
#include "pg_operator/operator.h"
#include "pg_operator/operator_node.h"
#include "pg_optimizer/colref.h"

namespace pgp {

hash_t LogicalLimit::Hash() const {
  auto hash = Operator::Hash();

  if (limit != nullptr)
    hash = HashUtil::CombineHashes(hash, limit->Hash());
  if (offset != nullptr)
    hash = HashUtil::CombineHashes(hash, offset->Hash());
  hash = HashUtil::CombineHashes(hash, order_spec->Hash());

  return hash;
}

bool LogicalLimit::operator==(const Operator &other) const {
  if (Operator::operator==(other)) {
    const auto &limit_node = other.Cast<LogicalLimit>();

    return *limit_node.limit == *limit && *limit_node.offset == *offset && *limit_node.order_spec == *order_spec;
  }

  return false;
}

hash_t LogicalJoin::Hash() const {
  auto hash = Operator::Hash();
  hash = HashUtil::CombineHashes(hash, join_type);
  if (filter != nullptr)
    hash = HashUtil::CombineHashes(hash, filter->Hash());
  return hash;
}

bool LogicalJoin::operator==(const Operator &other) const {
  if (Operator::operator==(other)) {
    const auto &join_node = other.Cast<LogicalJoin>();

    return join_type == join_node.join_type && *join_node.filter == *filter;
  }

  return false;
}

hash_t LogicalGet::Hash() const {
  auto hash = Operator::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(table_desc->relid));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(table_desc->eref->aliasname));
  hash = HashUtil::CombineHashes(hash, ColRefContainerHash(output_columns));
  if (filter != nullptr)
    hash = HashUtil::CombineHashes(hash, filter->Hash());

  return hash;
}

bool LogicalGet::operator==(const Operator &other) const {
  if (Operator::operator==(other)) {
    const auto &get_node = other.Cast<LogicalGet>();

    return get_node.table_desc->relid == table_desc->relid && get_node.output_columns == output_columns;
  }
  return false;
}

hash_t LogicalApply::Hash() const {
  auto hash = Operator::Hash();
  hash = HashUtil::CombineHashes(hash, ColRefContainerHash(expr_refs));
  hash = HashUtil::CombineHashes(hash, (int)subquery_type);
  if (filter != nullptr)
    hash = HashUtil::CombineHashes(hash, filter->Hash());

  return hash;
}

bool LogicalApply::operator==(const Operator &other) const {
  if (Operator::operator==(other)) {
    const auto &apply_node = other.Cast<LogicalApply>();

    return apply_node.expr_refs == expr_refs && apply_node.subquery_type == subquery_type;
  }

  return false;
}

hash_t LogicalGbAgg::Hash() const {
  auto hash = Operator::Hash();
  hash = HashUtil::CombineHashes(hash, ColRefContainerHash(group_columns));
  for (const auto &expr : project_exprs)
    hash = HashUtil::CombineHashes(hash, expr->Hash());
  return hash;
}

bool LogicalGbAgg::operator==(const Operator &other) const {
  if (Operator::operator==(other)) {
    const auto &gb_agg_node = other.Cast<LogicalGbAgg>();

    return group_columns == gb_agg_node.group_columns && project_exprs == gb_agg_node.project_exprs;
  }
  return false;
}

hash_t LogicalFilter::Hash() const {
  auto hash = Operator::Hash();
  hash = HashUtil::CombineHashes(hash, filter->Hash());
  return hash;
}

bool LogicalFilter::operator==(const Operator &other) const {
  if (Operator::operator==(other)) {
    const auto &filter_node = other.Cast<LogicalFilter>();

    return *filter_node.filter == *filter;
  }

  return false;
}

hash_t LogicalProject::Hash() const {
  auto hash = Operator::Hash();
  for (const auto &expr : project_exprs)
    hash = HashUtil::CombineHashes(hash, expr->Hash());
  return hash;
}

bool LogicalProject::operator==(const Operator &other) const {
  if (Operator::operator==(other)) {
    const auto &project_node = other.Cast<LogicalProject>();

    return project_exprs == project_node.project_exprs;
  }

  return false;
}

}  // namespace pgp