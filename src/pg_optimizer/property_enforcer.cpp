#include "pg_optimizer/property_enforcer.h"

#include "pg_operator/physical_operator.h"
#include "pg_optimizer/group_expression.h"
#include "pg_optimizer/properties.h"
#include "pg_optimizer/property.h"

namespace pgp {

GroupExpression *PropertyEnforcer::EnforceProperty(GroupExpression *gexpr, Property *property) {
  input_gexpr_ = gexpr;

  switch (property->Type()) {
    case PropertyType::SORT:
      EnforceSortProperty(property->As<PropertySort>());
      break;
    default:
      break;
  }

  return output_gexpr_;
}

void PropertyEnforcer::EnforceSortProperty(const PropertySort *prop_sort) {
  output_gexpr_ =
      new GroupExpression(std::make_shared<PhysicalSort>(prop_sort->GetSortSpec()->Copy()), {input_gexpr_->GetGroup()});
}

}  // namespace pgp