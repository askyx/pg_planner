#pragma once

#include "common/exception.h"
#include "pg_optimizer/optimization_context.h"

namespace pgp {

class Memo;
class GroupExpression;

class CostCalculator {
 public:
  Cost CalculateCost(Memo *memo, GroupExpression *gexpr);
};
}  // namespace pgp
