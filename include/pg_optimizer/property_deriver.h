#pragma once

#include "pg_operator/item_expr.h"
#include "pg_optimizer/colref.h"
#include "pg_optimizer/group_expression.h"

namespace pgp {

class PropertiesDriver {
 public:
  PropertiesDriver() = default;

  static ColRef2DArray PcrsRequired(GroupExpression *gexpr, const ColRefArray &pcrs_required);

  static ColRefSet DeriveOutputColumns(OperatorNode *expr);

  static KeyCollection DeriveKeyCollection(OperatorNode *expr);

  static ColRefSet DeriveNotNullColumns(OperatorNode *expr);

  static Cardinality DeriveMaxCard(OperatorNode *expr);

  static ColRefSet DeriveOuterReferences(OperatorNode *expr);
};

}  // namespace pgp