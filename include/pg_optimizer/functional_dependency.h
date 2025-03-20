#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "common/macros.h"
#include "pg_optimizer/colref.h"

namespace pgp {

class FunctionalDependency {
 private:
  ColRefSet determinants_;

  ColRefSet dependents_;

 public:
  DISALLOW_COPY(FunctionalDependency)

  FunctionalDependency(ColRefSet determinants, ColRefSet dependents)
      : determinants_(std::move(determinants)), dependents_(std::move(dependents)) {}

  ColRefSet Determinants() const { return determinants_; }

  ColRefSet Dependents() const { return dependents_; }

  bool FIncluded(const ColRefSet& pcrs) const {
    return ContainsAll(pcrs, determinants_) && ContainsAll(pcrs, dependents_);
  }
};

using FunctionalDependencyArray = std::vector<std::shared_ptr<FunctionalDependency>>;

}  // namespace pgp
