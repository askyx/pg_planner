

#include "pg_optimizer/group.h"

#include "pg_operator/operator.h"
#include "pg_optimizer/group_expression.h"
#include "pg_optimizer/optimization_context.h"

namespace pgp {

Group::~Group() {
  delete group_properties_;

  for (auto *pgexpr : logical_expressions_)
    delete pgexpr;

  for (auto *gexpr : physical_expressions_)
    delete gexpr;

  // TODO: delete enforced_expressions_
  // for (auto *enforced_expr : enforced_expressions_)
  //   delete enforced_expr;
}

void Group::AddExpression(GroupExpression *gexpr, bool force_gexpr) {
  gexpr->SetGroup(this);
  if (force_gexpr)
    enforced_expressions_.emplace_back(gexpr);
  if (gexpr->Pop()->Logical())
    logical_expressions_.emplace_back(gexpr);
  else
    physical_expressions_.emplace_back(gexpr);
}

GroupExpression *Group::GetBestExpression(PropertySet *properties) {
  auto it = lowest_cost_expressions_.find(properties);
  if (it != lowest_cost_expressions_.end()) {
    return std::get<1>(it->second);
  }

  return nullptr;
}

bool Group::SetExpressionCost(GroupExpression *expr, double cost, PropertySet *properties) {
  auto it = lowest_cost_expressions_.find(properties);
  if (it == lowest_cost_expressions_.end()) {
    // not exist so insert
    lowest_cost_expressions_[properties] = std::make_tuple(cost, expr);
    return true;
  }

  if (std::get<0>(it->second) > cost) {
    // this is lower cost
    lowest_cost_expressions_[properties] = std::make_tuple(cost, expr);
    delete properties;
    return true;
  }

  delete properties;
  return false;
}

std::vector<std::string> Group::ToString() const {
  std::vector<std::string> result;

  result.reserve(logical_expressions_.size());
  for (auto *pgexpr : logical_expressions_)
    result.emplace_back(pgexpr->ToString());

  for (auto *gexpr : physical_expressions_)
    result.emplace_back(gexpr->ToString());

  return result;
}

}  // namespace pgp