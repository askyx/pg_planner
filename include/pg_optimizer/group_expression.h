#pragma once

#include <bitset>
#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/hash_util.h"
#include "common/macros.h"
#include "pg_operator/operator.h"
#include "pg_optimizer/group.h"
#include "pg_optimizer/property.h"
#include "pg_optimizer/rule.h"

namespace pgp {

class GroupExpression {
 private:
  std::shared_ptr<Operator> content;

  std::vector<Group *> children_;

  Group *group_{nullptr};

  std::bitset<static_cast<uint32_t>(RuleType::ExfSentinel)> rule_mask_;

  std::unordered_map<std::shared_ptr<PropertySet>, std::tuple<double, std::vector<std::shared_ptr<PropertySet>>>,
                     PropertySetHasher, PropertySetEqer>
      lowest_cost_table_;

 public:
  DISALLOW_COPY(GroupExpression)

  GroupExpression(std::shared_ptr<Operator> pop, const std::vector<Group *> &pdrgpgroup)
      : content(std::move(pop)), children_(pdrgpgroup) {}

  double GetCost(const std::shared_ptr<PropertySet> &requirements) const {
    return std::get<0>(lowest_cost_table_.find(requirements)->second);
  }

  std::vector<std::shared_ptr<PropertySet>> GetInputProperties(const std::shared_ptr<PropertySet> &required) const {
    return std::get<1>(lowest_cost_table_.find(required)->second);
  }

  void SetLocalHashTable(const std::shared_ptr<PropertySet> &output_properties,
                         const std::vector<std::shared_ptr<PropertySet>> &input_properties_list, double cost);

  const std::vector<Group *> &GetChildGroup() { return children_; }

  void SetRuleExplored(Rule *rule) { rule_mask_.set(rule->GetRuleType(), true); }

  bool HasRuleExplored(Rule *rule) { return rule_mask_.test(rule->GetRuleType()); }

  std::string ToString() const;

  void SetGroup(Group *pgroup) { group_ = pgroup; }

  Group *GetGroup() const { return group_; }

  std::shared_ptr<Operator> Pop() const { return content; }

  size_t ChildrenSize() const { return children_.size(); }

  Group *GetChild(size_t i) const { return children_[i]; }

  bool operator==(const GroupExpression &gexpr) const;

  hash_t Hash() const {
    auto hash = content->Hash();
    for (const auto &child : children_) {
      hash = HashUtil::CombineHashes(hash, child->GetGroupId());
    }
    return hash;
  }
};

struct GroupExpressionHasher {
  inline size_t operator()(const pgp::GroupExpression *oc) const { return oc->Hash(); }
};

struct GroupExpressionEqualer {
  inline bool operator()(const pgp::GroupExpression *poc1, const pgp::GroupExpression *poc2) const {
    return *poc1 == *poc2;
  }
};

}  // namespace pgp
