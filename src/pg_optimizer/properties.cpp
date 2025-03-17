#include "pg_optimizer/properties.h"

#include <cstdint>

#include "common/hash_util.h"
#include "pg_optimizer/order_spec.h"

namespace pgp {

PropertySort *PropertySort::Copy() {
  return new PropertySort(order_spec_->Copy());
}

hash_t PropertySort::Hash() const {
  return HashUtil::CombineHashes(HashUtil::Hash(static_cast<uint32_t>(PropertyType::SORT)), order_spec_->Hash());
}

bool PropertySort::operator>=(const Property &r) const {
  if (r.Type() != PropertyType::SORT) {
    return false;
  }

  const PropertySort &r_sort = *reinterpret_cast<const PropertySort *>(&r);
  return order_spec_->Satisfies(r_sort.order_spec_);
}

std::string PropertySort::ToString() const {
  std::string result = "SORT(";
  for (auto sort : order_spec_->GetSortArray()) {
    result += sort.colref->ToString() + ", ";
  }
  result.pop_back();
  result.pop_back();
  result += ")";
  return result;
}

}  // namespace pgp
