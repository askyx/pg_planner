#pragma once

#include "pg_operator/operator_node.h"

namespace pgp {

class FilterPushDown {
 public:
  static OperatorNodePtr Process(OperatorNodePtr pexpr);
};
}  // namespace pgp
