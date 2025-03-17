#pragma once

#include <utility>

#include "pg_optimizer/property.h"

namespace pgp {

class Group;
class GroupExpression;
class OptimizationContext;
class OptimizerContext;

class OptimizationContext {
 private:
  double cost_upper_bound_;

  OptimizerContext *context_;

  std::shared_ptr<PropertySet> property_set_;

 public:
  std::shared_ptr<PropertySet> GetRequiredProperties() const { return property_set_; }

  explicit OptimizationContext(std::shared_ptr<PropertySet> prpp) : property_set_(std::move(prpp)) {}

  double GetCostUpperBound() const { return cost_upper_bound_; }

  void SetCostUpperBound(double cost) { cost_upper_bound_ = cost; }

  void SetGlobalOptimizerContext(OptimizerContext *context) { context_ = context; }

  OptimizerContext *GetOptimizerContext() const { return context_; }
};

}  // namespace pgp
