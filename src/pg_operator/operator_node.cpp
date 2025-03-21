

#include "pg_operator/operator_node.h"

#include "common/hash_util.h"
#include "pg_operator/operator.h"
#include "pg_optimizer/colref.h"
#include "pg_optimizer/operator_prop.h"

namespace pgp {

std::string OperatorNode::ToString() const {
  return content->ToString();
}

std::shared_ptr<OperatorProperties> OperatorNode::DeriveProp() {
  if (!operator_properties->IsComplete())
    operator_properties->Derive(this);

  return operator_properties;
}

bool OperatorNode::operator==(const OperatorNode &other) const {
  if (*content != *other.content) {
    return false;
  }

  return children == other.children;
}

hash_t OperatorNode::Hash() const {
  auto ul_hash = content->Hash();

  for (const auto &child : children)
    ul_hash = HashUtil::CombineHashes(ul_hash, child->Hash());

  return ul_hash;
}

ColRefSet OperatorNode::DeriveOuterReferences() {
  return operator_properties->DeriveOuterReferences(this);
}

ColRefSet OperatorNode::DeriveOutputColumns() {
  return operator_properties->DeriveOutputColumns(this);
}

ColRefSet OperatorNode::DeriveNotNullColumns() {
  return operator_properties->DeriveNotNullColumns(this);
}

Cardinality OperatorNode::DeriveMaxCard() {
  return operator_properties->DeriveMaxCard(this);
}

KeyCollection OperatorNode::DeriveKeyCollection() {
  return operator_properties->DeriveKeyCollection(this);
}

FunctionalDependencyArray OperatorNode::DeriveFunctionalDependencies() {
  return operator_properties->DeriveFunctionalDependencies(this);
}

// Scalar property accessors - derived as needed
ColRefSet OperatorNode::DeriveDefinedColumns() {
  return operator_properties->DeriveDefinedColumns(this);
}

}  // namespace pgp