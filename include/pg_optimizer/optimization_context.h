#pragma once

#include "pg_optimizer/property.h"

#define GPOPT_INVALID_OPTCTXT_ID UINT32_MAX

namespace pgp {

class Group;
class GroupExpression;
class OptimizationContext;
class OptimizerContext;

class OptimizationContext {
 private:
  double cost_upper_bound_;

  OptimizerContext *context_;

  PropertySet *property_set_;

 public:
  PropertySet *GetRequiredProperties() const { return property_set_; }

  explicit OptimizationContext(PropertySet *prpp) : property_set_(prpp) {}

  double GetCostUpperBound() const { return cost_upper_bound_; }

  void SetCostUpperBound(double cost) { cost_upper_bound_ = cost; }

  void SetGlobalOptimizerContext(OptimizerContext *context) { context_ = context; }

  OptimizerContext *GetOptimizerContext() const { return context_; }
};

}  // namespace pgp
