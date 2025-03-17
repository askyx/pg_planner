

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
  Add(new InnerJoinCommutativityRule());

  Add(new Project2ComputeScalarRule());
  Add(new Get2TableScan());
  Add(new Select2Filter());
  Add(new Join2NestedLoopJoin());
  Add(new Join2HashJoin());
  Add(new GbAgg2HashAgg());
  Add(new GbAgg2StreamAgg());
  Add(new GbAgg2ScalarAgg());
  Add(new ImplementLimit());
  Add(new ImplementApply());
  Add(new ImplementInnerJoin());
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