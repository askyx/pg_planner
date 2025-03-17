#include "pg_optimizer/property.h"

namespace pgp {

void PropertySet::AddProperty(Property *property) {
  auto iter = properties_.begin();
  for (; iter != properties_.end(); iter++) {
    // Iterate until point where preserve descending order
    if (property->Type() < (*iter)->Type()) {
      break;
    }
  }

  properties_.insert(iter, property);
}

const Property *PropertySet::GetPropertyOfType(PropertyType type) const {
  for (const auto &prop : properties_) {
    if (prop->Type() == type) {
      return prop;
    }
  }

  return nullptr;
}

bool PropertySet::HasProperty(const Property &r_property) const {
  for (auto *property : properties_) {
    if (*property >= r_property) {
      return true;
    }
  }

  return false;
}

bool PropertySet::operator>=(const PropertySet &r) const {
  for (auto *r_property : r.properties_) {
    if (!HasProperty(*r_property))
      return false;
  }
  return true;
}

bool PropertySet::operator==(const PropertySet &r) const {
  return *this >= r && r >= *this;
}

hash_t PropertySet::Hash() const {
  auto hash = HashUtil::Hash<size_t>(properties_.size());
  for (const auto &prop : properties_) {
    hash = HashUtil::CombineHashes(hash, prop->Hash());
  }
  return hash;
}

std::string PropertySet::ToString() const {
  std::string res = "PropertySet: ";
  for (auto *prop : properties_) {
    res += prop->ToString() + ", ";
  }
  res.pop_back();
  res.pop_back();
  return res;
}

}  // namespace pgp
