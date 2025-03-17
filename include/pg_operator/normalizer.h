#pragma once

namespace pgp {

class OperatorNode;

class Normalizer {
 public:
  static OperatorNode *NormalizerTree(OperatorNode *pexpr);
};
}  // namespace pgp
