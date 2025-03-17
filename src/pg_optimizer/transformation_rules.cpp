#include "pg_optimizer/transformation_rules.h"

#include "pg_operator/logical_operator.h"
#include "pg_operator/operator.h"
#include "pg_operator/operator_node.h"
#include "pg_optimizer/group_expression.h"
#include "pg_optimizer/pattern.h"
#include "pg_optimizer/rule.h"

extern "C" {
#include "nodes/nodes.h"
}

namespace pgp {

InnerJoinCommutativityRule::InnerJoinCommutativityRule() {
  match_pattern_ = new Pattern(OperatorType::LogicalJoin);
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  match_pattern_->AddChild(new Pattern(OperatorType::LEAF));
  rule_type_ = ExfInnerJoinCommutativity;
}

bool InnerJoinCommutativityRule::Check(GroupExpression *gexpr) const {
  const auto &join_op = gexpr->Pop()->Cast<LogicalJoin>();
  return join_op.join_type == JOIN_INNER;
}

void InnerJoinCommutativityRule::Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const {
  OperatorNode *pexpr_left = pexpr->GetChild(0);
  OperatorNode *pexpr_right = pexpr->GetChild(1);

  auto *pexpr_alt = new OperatorNode(pexpr->content, {pexpr_right, pexpr_left});

  // add alternative to transformation result
  pxfres.emplace_back(pexpr_alt);
}

}  // namespace pgp