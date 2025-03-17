#pragma once

#include <cstdint>
#include <tuple>
#include <unordered_map>

#include "pg_operator/operator_node.h"
#include "pg_optimizer/property.h"

#define GPOPT_INVALID_GROUP_ID UINT32_MAX

namespace pgp {

class Group;
class GroupExpression;
class OperatorProperties;
class OperatorNode;
class OptimizationContext;

class Group {
 private:
  int32_t group_id_;

  OperatorProperties *group_properties_;

  bool has_explored_{false};

  std::vector<GroupExpression *> logical_expressions_;
  std::vector<GroupExpression *> physical_expressions_;
  std::vector<GroupExpression *> enforced_expressions_;

  std::unordered_map<PropertySet *, std::tuple<double, GroupExpression *>, PropertySetHasher, PropertySetEqer>
      lowest_cost_expressions_;

 public:
  Group(int32_t group_id, OperatorProperties *properties) : group_id_(group_id), group_properties_(properties) {}

  ~Group();

  bool SetExpressionCost(GroupExpression *expr, double cost, PropertySet *properties);

  void SetExplorationFlag() { has_explored_ = true; }

  bool HasExplored() const { return has_explored_; }

  bool HasExpressions(PropertySet *properties) const {
    const auto &it = lowest_cost_expressions_.find(properties);
    return (it != lowest_cost_expressions_.end());
  }

  const std::vector<GroupExpression *> &GetLogicalExpressions() const { return logical_expressions_; }

  const std::vector<GroupExpression *> &GetPhysicalExpressions() const { return physical_expressions_; }

  std::vector<std::string> ToString() const;

  void AddExpression(GroupExpression *gexpr, bool force_gexpr);

  int32_t GetGroupId() const { return group_id_; }

  OperatorProperties *GroupProperties() const { return group_properties_; }

  // lookup best expression under given optimization context
  GroupExpression *GetBestExpression(PropertySet *properties);

  bool operator==(const Group &other) const { return group_id_ == other.group_id_; }
};

}  // namespace pgp
