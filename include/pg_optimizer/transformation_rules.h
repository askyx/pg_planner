#pragma once

#include "pg_operator/operator_node.h"
#include "pg_optimizer/rule.h"

namespace pgp {

class InnerJoinCommutativityRule : public Rule {
 public:
  InnerJoinCommutativityRule();

  bool Check(GroupExpression *gexpr) const override;

  void Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr, OptimizationContext *context) const override;
};

}  // namespace pgp