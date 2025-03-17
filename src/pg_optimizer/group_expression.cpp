

#include "pg_optimizer/group_expression.h"

#include <ranges>
#include <string>
#include <vector>

#include "pg_operator/operator_node.h"
#include "pg_optimizer/cost_model.h"
#include "pg_optimizer/optimization_context.h"

namespace pgp {

bool GroupExpression::operator==(const GroupExpression &gexpr) const {
  if (*content == *(gexpr.content) && children_.size() == gexpr.children_.size()) {
    for (auto [child1, child2] : std::views::zip(children_, gexpr.children_))
      if (child1->GetGroupId() != child2->GetGroupId())
        return false;
  }
  return true;
}

void GroupExpression::SetLocalHashTable(const std::shared_ptr<PropertySet> &output_properties,
                                        const std::vector<std::shared_ptr<PropertySet>> &input_properties_list,
                                        double cost) {
  auto it = lowest_cost_table_.find(output_properties);
  if (it == lowest_cost_table_.end()) {
    // No other cost to compare against
    lowest_cost_table_.insert(std::make_pair(output_properties, std::make_tuple(cost, input_properties_list)));
  } else {
    // Only insert if the cost is lower than the existing cost
    std::vector<std::shared_ptr<PropertySet>> pending_deletion = input_properties_list;
    if (std::get<0>(it->second) > cost) {
      pending_deletion = std::get<1>(it->second);

      // Insert
      lowest_cost_table_[output_properties] = std::make_tuple(cost, input_properties_list);
    }
  }
}

std::string GroupExpression::ToString() const {
  auto s = std::format("{:>3}", content->ToString());
  if (!children_.empty()) {
    s += "[";
    for (auto *child : children_) {
      s += std::format("G{},", child->GetGroupId());
    }
    s.pop_back();
    s += "]";
  }

  return s;
}

}  // namespace pgp