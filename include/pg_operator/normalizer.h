#pragma once

#include "pg_operator/operator_node.h"
namespace pgp {

class Normalizer {
 public:
  static OperatorNodePtr NormalizerTree(OperatorNodePtr pexpr);
};
}  // namespace pgp
