#pragma once

#include <cstddef>
#include <memory>
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

  virtual std::shared_ptr<Property> Copy() = 0;

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

  ~PropertySet() = default;

  std::shared_ptr<PropertySet> Copy() {
    std::vector<std::shared_ptr<Property>> props;
    props.reserve(properties_.size());
    for (const auto &prop : properties_) {
      props.push_back(prop->Copy());
    }

    return std::make_shared<PropertySet>(props);
  }

  explicit PropertySet(std::vector<std::shared_ptr<Property>> properties) : properties_(std::move(properties)) {}

  const std::vector<std::shared_ptr<Property>> &Properties() const { return properties_; }

  void AddProperty(const std::shared_ptr<Property> &property);

  const std::shared_ptr<Property> GetPropertyOfType(PropertyType type) const;

  template <typename T>
  const T &GetPropertyOfTypeAs(PropertyType type) const {
    const auto &property = GetPropertyOfType(type);
    if (property)
      return *property->As<T>();
    throw std::runtime_error("Property not found");
  }

  hash_t Hash() const;

  bool HasProperty(const Property &r_property) const;

  bool operator>=(const PropertySet &r) const;

  bool operator==(const PropertySet &r) const;

  std::string ToString() const;

 private:
  std::vector<std::shared_ptr<Property>> properties_;
};

struct PropertySetHasher {
  std::size_t operator()(std::shared_ptr<pgp::PropertySet> const &s) const { return s->Hash(); }
};

struct PropertySetEqer {
  bool operator()(std::shared_ptr<pgp::PropertySet> const &s1, std::shared_ptr<pgp::PropertySet> const &s2) const {
    return *s1 == *s2;
  }
};

}  // namespace pgp
