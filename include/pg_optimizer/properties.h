#pragma once

#include "pg_optimizer/property.h"

namespace pgp {

class OrderSpec;

class PropertySort : public Property {
 public:
  explicit PropertySort(OrderSpec *order_spec) : order_spec_(order_spec) {}

  PropertyType Type() const override { return PropertyType::SORT; }

  PropertySort *Copy() override;

  hash_t Hash() const override;

  bool operator>=(const Property &r) const override;

  OrderSpec *GetSortSpec() const { return order_spec_; }

  std::string ToString() const override;

 private:
  OrderSpec *order_spec_;
};

}  // namespace pgp
