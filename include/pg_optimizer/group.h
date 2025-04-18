#pragma once

#include <cstdint>
#include <memory>
#include <tuple>
#include <unordered_map>
#include <utility>

#include "pg_optimizer/property.h"

namespace pgp {

class Group;
class GroupExpression;
class OperatorProperties;
class OperatorNode;
class OptimizationContext;

class Group {
 private:
  int32_t group_id_;

  std::shared_ptr<OperatorProperties> group_properties_;

  bool has_explored_{false};

  std::vector<GroupExpression *> logical_expressions_;
  std::vector<GroupExpression *> physical_expressions_;
  std::vector<GroupExpression *> enforced_expressions_;

  std::unordered_map<std::shared_ptr<PropertySet>, std::tuple<double, GroupExpression *>, PropertySetHasher,
                     PropertySetEqer>
      lowest_cost_expressions_;

 public:
  Group(int32_t group_id, std::shared_ptr<OperatorProperties> properties)
      : group_id_(group_id), group_properties_(std::move(properties)) {}

  ~Group();

  bool SetExpressionCost(GroupExpression *expr, double cost, const std::shared_ptr<PropertySet> &properties);

  void SetExplorationFlag() { has_explored_ = true; }

  bool HasExplored() const { return has_explored_; }

  bool HasExpressions(const std::shared_ptr<PropertySet> &properties) const {
    const auto &it = lowest_cost_expressions_.find(properties);
    return (it != lowest_cost_expressions_.end());
  }

  const std::vector<GroupExpression *> &GetLogicalExpressions() const { return logical_expressions_; }

  const std::vector<GroupExpression *> &GetPhysicalExpressions() const { return physical_expressions_; }

  std::vector<std::string> ToString() const;

  void AddExpression(GroupExpression *gexpr, bool force_gexpr);

  int32_t GetGroupId() const { return group_id_; }

  std::shared_ptr<OperatorProperties> GroupProperties() const { return group_properties_; }

  // lookup best expression under given optimization context
  GroupExpression *GetBestExpression(const std::shared_ptr<PropertySet> &properties);

  bool operator==(const Group &other) const { return group_id_ == other.group_id_; }
};

}  // namespace pgp
