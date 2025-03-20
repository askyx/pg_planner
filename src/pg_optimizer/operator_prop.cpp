

#include "pg_optimizer/operator_prop.h"

#include <memory>
#include <ranges>

#include "nodes/bitmapset.h"
#include "pg_operator/item_expr.h"
#include "pg_operator/logical_operator.h"
#include "pg_operator/operator.h"
#include "pg_optimizer/colref.h"
#include "pg_optimizer/group.h"
#include "pg_optimizer/property_deriver.h"

namespace pgp {

OperatorProperties::~OperatorProperties() {
  bms_free(prop_derive_flag_);
}

void OperatorProperties::Derive(OperatorNode *expr) {
  DeriveDefinedColumns(expr);

  DeriveOutputColumns(expr);

  DeriveOuterReferences(expr);

  DeriveNotNullColumns(expr);

  DeriveMaxCard(expr);

  DeriveKeyCollection(expr);

  DeriveFunctionalDependencies(expr);

  logical_derived_ = true;
}

FunctionalDependencyArray DeriveChildFunctionalDependencies(uint32_t child_index, OperatorNode *expr) {
  auto output_colums = expr->DeriveOutputColumns();

  FunctionalDependencyArray pdrgpfd;
  for (const auto &fd : expr->GetChild(child_index)->DeriveFunctionalDependencies()) {
    if (ContainsAll(output_colums, fd->Determinants())) {
      // decompose FD's RHS to extract the applicable part
      ColRefSet pcrs_determined;
      AddColRef(pcrs_determined, fd->Dependents());
      ColRefSetIntersection(pcrs_determined, output_colums);
      if (0 < pcrs_determined.size()) {
        // create a new FD and add it to the output array
        auto pfd_new = std::make_shared<FunctionalDependency>(fd->Determinants(), pcrs_determined);
        pdrgpfd.emplace_back(pfd_new);
      }
    }
  }

  return pdrgpfd;
}

FunctionalDependencyArray DeriveLocalFunctionalDependencies(OperatorNode *expr) {
  FunctionalDependencyArray pdrgpfd;

  auto output_colums = expr->DeriveOutputColumns();

  for (const auto &key : expr->DeriveKeyCollection()) {
    ColRefSet dependents;
    AddColRef(dependents, output_colums);
    ColRefSetDifference(dependents, key);

    if (0 < dependents.size()) {
      auto pfd_local = std::make_shared<FunctionalDependency>(key, dependents);
      pdrgpfd.emplace_back(pfd_local);
    }
  }

  return pdrgpfd;
}

// output columns
ColRefSet OperatorProperties::GetOutputColumns() const {
  PGP_ASSERT(IsComplete(), "property derivation is not complete");
  return output_columns_;
}

// output columns
ColRefSet OperatorProperties::DeriveOutputColumns(OperatorNode *expr) {
  if (!bms_is_member(EdptPcrsOutput, prop_derive_flag_)) {
    prop_derive_flag_ = bms_add_member(prop_derive_flag_, EdptPcrsOutput);

    output_columns_ = PropertiesDriver::DeriveOutputColumns(expr);
  }

  return output_columns_;
}

// outer references
ColRefSet OperatorProperties::GetOuterReferences() const {
  PGP_ASSERT(IsComplete(), "property derivation is not complete");
  return outer_references_;
}

// outer references
ColRefSet OperatorProperties::DeriveOuterReferences(OperatorNode *expr) {
  if (!bms_is_member(EdptPcrsOuter, prop_derive_flag_)) {
    prop_derive_flag_ = bms_add_member(prop_derive_flag_, EdptPcrsOuter);

    outer_references_ = PropertiesDriver::DeriveOuterReferences(expr);
  }

  return outer_references_;
}

// nullable columns
ColRefSet OperatorProperties::GetNotNullColumns() const {
  return not_null_columns_;
}

ColRefSet OperatorProperties::DeriveNotNullColumns(OperatorNode *expr) {
  if (!bms_is_member(EdptPcrsNotNull, prop_derive_flag_)) {
    prop_derive_flag_ = bms_add_member(prop_derive_flag_, EdptPcrsNotNull);

    not_null_columns_ = PropertiesDriver::DeriveNotNullColumns(expr);
  }

  return not_null_columns_;
}

// key collection
KeyCollection OperatorProperties::GetKeyCollection() const {
  return key_collection_;
}

KeyCollection OperatorProperties::DeriveKeyCollection(OperatorNode *expr) {
  if (!bms_is_member(EdptPkc, prop_derive_flag_)) {
    prop_derive_flag_ = bms_add_member(prop_derive_flag_, EdptPkc);

    key_collection_ = PropertiesDriver::DeriveKeyCollection(expr);

    if (key_collection_.empty() && 1 == DeriveMaxCard(expr)) {
      output_columns_ = DeriveOutputColumns(expr);

      if (0 < output_columns_.size()) {
        key_collection_.emplace_back(output_columns_);
      }
    }
  }

  return key_collection_;
}

FunctionalDependencyArray OperatorProperties::GetFunctionalDependencies() const {
  PGP_ASSERT(IsComplete(), "property derivation is not complete");
  return functional_dependencies_;
}

FunctionalDependencyArray OperatorProperties::DeriveFunctionalDependencies(OperatorNode *expr) {
  if (!bms_is_member(EdptPdrgpfd, prop_derive_flag_)) {
    prop_derive_flag_ = bms_add_member(prop_derive_flag_, EdptPdrgpfd);

    // collect applicable FD's from logical children
    for (auto [ul, child] : std::views::enumerate(expr->children)) {
      for (const auto &pfd : DeriveChildFunctionalDependencies(ul, expr))
        functional_dependencies_.emplace_back(pfd);
    }
    // add local FD's
    for (const auto &pfd : DeriveLocalFunctionalDependencies(expr))
      functional_dependencies_.emplace_back(pfd);
  }

  return functional_dependencies_;
}

// max cardinality
Cardinality OperatorProperties::GetMaxCard() const {
  PGP_ASSERT(IsComplete(), "property derivation is not complete");
  return cardinality_;
}

Cardinality OperatorProperties::DeriveMaxCard(OperatorNode *expr) {
  if (!bms_is_member(EdptMaxCard, prop_derive_flag_)) {
    prop_derive_flag_ = bms_add_member(prop_derive_flag_, EdptMaxCard);

    cardinality_ = PropertiesDriver::DeriveMaxCard(expr);
  }

  return cardinality_;
}

ColRefSet OperatorProperties::DeriveDefinedColumns(OperatorNode *expr) {
  if (!bms_is_member(EdptPcrsDefined, prop_derive_flag_)) {
    prop_derive_flag_ = bms_add_member(prop_derive_flag_, EdptPcrsDefined);

    if (expr->content->kind == OperatorType::LogicalProject) {
      auto &project_op = expr->Cast<LogicalProject>();
      for (const auto &child : project_op.project_exprs) {
        auto *project_element = child->Cast<ItemProjectElement>().colref;
        defined_columns_.insert(project_element);
      }
    }

    if (expr->content->kind == OperatorType::LogicalGbAgg) {
      auto &project_op = expr->Cast<LogicalGbAgg>();
      for (const auto &child : project_op.project_exprs) {
        auto *project_element = child->Cast<ItemProjectElement>().colref;
        defined_columns_.insert(project_element);
      }
    }
  }
  return defined_columns_;
}

ColRefSet OperatorProperties::GetDefinedColumns() const {
  PGP_ASSERT(IsComplete(), "property derivation is not complete");
  return defined_columns_;
}

}  // namespace pgp