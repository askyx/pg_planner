#pragma once

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/hash_util.h"
#include "common/macros.h"
#include "pg_operator/operator.h"
#include "pg_optimizer/colref.h"
#include "pg_optimizer/operator_prop.h"

namespace pgp {

class OperatorNode;

class Operator;

using OperatorNodeArray = std::vector<OperatorNode *>;

class OperatorNode {
 public:
  std::shared_ptr<Operator> content;

  OperatorNodeArray children;

  OperatorProperties *operator_properties{new OperatorProperties()};

  DISALLOW_COPY(OperatorNode)

  explicit OperatorNode(std::shared_ptr<Operator> pop) : content(std::move(pop)) {}

  OperatorNode(std::shared_ptr<Operator> pop, OperatorNodeArray children)
      : content(std::move(pop)), children(std::move(children)) {}

  size_t ChildrenSize() const { return children.size(); }

  OperatorNode *GetChild(uint32_t ul_pos) const {
    PGP_ASSERT(ul_pos < children.size(), "index out of range");
    return children[ul_pos];
  }

  void AddChild(OperatorNode *pexpr) { children.emplace_back(pexpr); }

  std::string ToString() const;

  OperatorProperties *PdpDerive();

  bool operator==(const OperatorNode &other) const;

  hash_t Hash() const;

  ColRefSet DeriveOuterReferences();
  ColRefSet DeriveOutputColumns();
  ColRefSet DeriveNotNullColumns();
  KeyCollection DeriveKeyCollection();
  Cardinality DeriveMaxCard();
  CFunctionalDependencyArray DeriveFunctionalDependencies();
  ColRefSet DeriveDefinedColumns();

  template <class TARGET>
  TARGET &Cast() {
    return content->Cast<TARGET>();
  }

  template <class TARGET>
  const TARGET &Cast() const {
    return content->Cast<TARGET>();
  }
};

inline bool operator==(const OperatorNodeArray &op1, const OperatorNodeArray &op2) {
  if (op1.size() != op2.size()) {
    return false;
  }

  for (auto [child1, child2] : std::views::zip(op1, op2)) {
    if (*child1 != *(child2))
      return false;
  }

  return true;
}

}  // namespace pgp
