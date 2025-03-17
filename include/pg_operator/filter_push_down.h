#pragma once

#include "pg_operator/operator_node.h"

namespace pgp {

class FilterPushDown {
 public:
  static OperatorNode *Process(OperatorNode *pexpr);
};
}  // namespace pgp
