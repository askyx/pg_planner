#pragma once

#include <typeinfo>
#include <vector>

#include "common/hash_util.h"

namespace pgp {

enum class PropertyType {
  SORT,
};

class Property {
 public:
  virtual ~Property() = default;

  virtual PropertyType Type() const = 0;

  virtual Property *Copy() = 0;

  virtual hash_t Hash() const {
    PropertyType t = Type();
    return HashUtil::Hash(t);
  }

  virtual bool operator>=(const Property &r) const { return Type() == r.Type(); }

  virtual std::string ToString() const = 0;

  template <typename T>
  const T *As() const {
    if (typeid(*this) == typeid(T)) {
      return reinterpret_cast<const T *>(this);
    }
    return nullptr;
  }
};

class PropertySet {
 public:
  PropertySet() = default;

  ~PropertySet() {
    for (auto *prop : properties_) {
      delete prop;
    }
  }

  PropertySet *Copy() {
    std::vector<Property *> props;
    props.reserve(properties_.size());
    for (auto *prop : properties_) {
      props.push_back(prop->Copy());
    }

    return new PropertySet(props);
  }

  explicit PropertySet(std::vector<Property *> properties) : properties_(std::move(properties)) {}

  const std::vector<Property *> &Properties() const { return properties_; }

  void AddProperty(Property *property);

  const Property *GetPropertyOfType(PropertyType type) const;

  template <typename T>
  const T *GetPropertyOfTypeAs(PropertyType type) const {
    const auto *property = GetPropertyOfType(type);
    if (property)
      return property->As<T>();
    return nullptr;
  }

  hash_t Hash() const;

  bool HasProperty(const Property &r_property) const;

  bool operator>=(const PropertySet &r) const;

  bool operator==(const PropertySet &r) const;

  std::string ToString() const;

 private:
  std::vector<Property *> properties_;
};

struct PropertySetHasher {
  using argument_type = pgp::PropertySet;

  using result_type = std::size_t;

  result_type operator()(argument_type const *s) const { return s->Hash(); }
};

struct PropertySetEqer {
  using argument_type = pgp::PropertySet;

  bool operator()(argument_type const *s1, argument_type const *s2) const { return *s1 == *s2; }
};

}  // namespace pgp
