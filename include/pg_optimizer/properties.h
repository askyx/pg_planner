#pragma once

#include <memory>
#include <utility>

#include "pg_optimizer/property.h"

namespace pgp {

class OrderSpec;

class PropertySort : public Property {
 public:
  explicit PropertySort(std::shared_ptr<OrderSpec> order_spec) : order_spec_(std::move(order_spec)) {}

  PropertyType Type() const override { return PropertyType::SORT; }

  std::shared_ptr<Property> Copy() override;

  hash_t Hash() const override;

  bool operator>=(const Property &r) const override;

  std::shared_ptr<OrderSpec> GetSortSpec() const { return order_spec_; }

  std::string ToString() const override;

 private:
  std::shared_ptr<OrderSpec> order_spec_;
};

}  // namespace pgp
