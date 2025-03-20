#pragma once

#include "common/macros.h"
#include "pg_optimizer/functional_dependency.h"

extern "C" {
#include "nodes/bitmapset.h"
}
namespace pgp {

class OperatorNode;

using KeyCollection = std::vector<ColRefSet>;

class OperatorProperties {
  friend class OperatorNode;

  enum PropType {
    EdptPcrsOutput = 0,
    EdptPcrsOuter,
    EdptPcrsNotNull,
    EdptPkc,
    EdptPdrgpfd,
    EdptMaxCard,
    EdptPcrsDefined,
    EdptPcrsUsed,
    EdptSentinel
  };

 private:
  Bitmapset *prop_derive_flag_{nullptr};

  ColRefSet output_columns_;

  ColRefSet outer_references_;

  ColRefSet not_null_columns_;

  KeyCollection key_collection_;

  FunctionalDependencyArray functional_dependencies_;

  ColRefSet defined_columns_;

  Cardinality cardinality_;

  bool logical_derived_{false};

  ColRefSet DeriveOutputColumns(OperatorNode *expr);

  ColRefSet DeriveOuterReferences(OperatorNode *expr);

  ColRefSet DeriveNotNullColumns(OperatorNode *expr);

  KeyCollection DeriveKeyCollection(OperatorNode *expr);

  FunctionalDependencyArray DeriveFunctionalDependencies(OperatorNode *expr);

  Cardinality DeriveMaxCard(OperatorNode *expr);

  ColRefSet DeriveDefinedColumns(OperatorNode *expr);

 public:
  DISALLOW_COPY(OperatorProperties)

  OperatorProperties() = default;

  ~OperatorProperties();

  bool IsComplete() const { return logical_derived_; }

  void Derive(OperatorNode *expr);

  ColRefSet GetOutputColumns() const;

  ColRefSet GetOuterReferences() const;

  ColRefSet GetNotNullColumns() const;

  KeyCollection GetKeyCollection() const;

  FunctionalDependencyArray GetFunctionalDependencies() const;

  Cardinality GetMaxCard() const;

  ColRefSet GetDefinedColumns() const;
};

}  // namespace pgp
