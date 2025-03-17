#pragma once

#include <memory>
namespace pgp {

class Property;
class PropertySort;
class GroupExpression;

class PropertyEnforcer {
 public:
  GroupExpression *EnforceProperty(GroupExpression *gexpr, const std::shared_ptr<Property> &property);

  void EnforceSortProperty(const PropertySort *prop_sort);

 private:
  GroupExpression *input_gexpr_{nullptr};

  GroupExpression *output_gexpr_{nullptr};
};

}  // namespace pgp