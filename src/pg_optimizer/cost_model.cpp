#include "pg_optimizer/cost_model.h"

#include <cmath>
#include <numeric>

#include "pg_operator/operator_node.h"
#include "pg_operator/physical_operator.h"
#include "pg_optimizer/group.h"
#include "pg_optimizer/group_expression.h"

extern "C" {
#include <access/table.h>
#include <optimizer/optimizer.h>
#include <utils/rel.h>
#include <utils/spccache.h>
}

namespace pgp {
Cost CostCalculator::CalculateCost(Memo *memo, GroupExpression *gexpr) {
  return 0;
}

}  // namespace pgp