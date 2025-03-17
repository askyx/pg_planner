#pragma once

#include "pg_operator/operator_node.h"
#include "pg_optimizer/rule.h"

namespace pgp {

class InnerJoinCommutativityRule : public Rule {
 public:
  InnerJoinCommutativityRule();

  bool Check(GroupExpression *gexpr) const override;

  void Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const override;
};

}  // namespace pgp