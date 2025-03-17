

#include "pg_optimizer/rule.h"

#include "pg_operator/operator.h"
#include "pg_optimizer/group_expression.h"
#include "pg_optimizer/implementation_rules.h"
#include "pg_optimizer/pattern.h"
#include "pg_optimizer/transformation_rules.h"

namespace pgp {

RulePromise Rule::Promise(GroupExpression* group_expr) const {
  auto root_type = match_pattern_->Type();
  // This rule is not applicable
  if (root_type != OperatorType::LEAF && root_type != group_expr->Pop()->kind) {
    return RulePromise::NO_PROMISE;
  }
  if (FImplementation())
    return RulePromise::PHYSICAL_PROMISE;
  return RulePromise::LOGICAL_PROMISE;
}

RuleSet::RuleSet() {
  Add(new CXformInnerJoinCommutativity());

  Add(new CXformProject2ComputeScalar());
  Add(new CXformGet2TableScan());
  Add(new CXformSelect2Filter());

  Add(new CXformJoin2NLJoin());
  Add(new CXformJoin2HashJoin());

  Add(new CXformGbAgg2HashAgg());
  Add(new CXformGbAgg2StreamAgg());
  Add(new CXformGbAgg2ScalarAgg());
  Add(new CXformImplementLimit());
  Add(new CXformImplementApply());
  Add(new CXformImplementInnerJoin());
}

RuleSet::~RuleSet() {
  for (const auto& [_, rules] : rule_set_) {
    for (auto* rule : rules)
      delete rule;
  }
}

void RuleSet::Add(Rule* pxform) {
  if (pxform->FImplementation())
    rule_set_[RuleCategory::Implementation].emplace_back(pxform);
  else
    rule_set_[RuleCategory::Exploration].emplace_back(pxform);
}

}  // namespace pgp