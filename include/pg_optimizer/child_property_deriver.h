#pragma once

#include <vector>

namespace pgp {

class PropertySet;
class GroupExpression;
class Memo;

class ChildPropertyDeriver {
 public:
  std::vector<std::pair<PropertySet *, std::vector<PropertySet *>>> GetProperties(Memo *memo, PropertySet *requirements,
                                                                                  GroupExpression *gexpr);

 private:
};

}  // namespace pgp