#pragma once

#include <utility>
#include <vector>

#include "common/macros.h"
#include "pg_optimizer/colref.h"

namespace pgp {

class CFunctionalDependency {
 private:
  ColRefSet determinants_;

  ColRefSet dependents_;

 public:
  DISALLOW_COPY(CFunctionalDependency)

  CFunctionalDependency(ColRefSet determinants, ColRefSet dependents)
      : determinants_(std::move(determinants)), dependents_(std::move(dependents)) {}

  ColRefSet Determinants() const { return determinants_; }

  ColRefSet Dependents() const { return dependents_; }

  bool FIncluded(const ColRefSet& pcrs) const {
    return ContainsAll(pcrs, determinants_) && ContainsAll(pcrs, dependents_);
  }
};

using CFunctionalDependencyArray = std::vector<CFunctionalDependency*>;

}  // namespace pgp
