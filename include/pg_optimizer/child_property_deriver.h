#pragma once

#include <memory>
#include <vector>

namespace pgp {

class PropertySet;
class GroupExpression;
class Memo;

class ChildPropertyDeriver {
 public:
  std::vector<std::pair<std::shared_ptr<PropertySet>, std::vector<std::shared_ptr<PropertySet>>>> GetProperties(
      Memo *memo, const std::shared_ptr<PropertySet> &requirements, GroupExpression *gexpr);

 private:
};

}  // namespace pgp