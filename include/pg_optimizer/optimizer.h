#pragma once

#include "pg_optimizer/optimizer_context.h"
#include "pg_optimizer/property.h"
struct Query;
struct PlannedStmt;

namespace pgp {

struct QueryInfo {
  pgp::OperatorNode *expr;
  std::vector<std::string> output_col_names;
  pgp::ColRefArray output_array;
  std::shared_ptr<PropertySet> properties;

  QueryInfo &Normalizer();

  explicit operator bool() const { return expr != nullptr; }
};

// 主入口，所有静态变量都要从这里初始化
struct Optimizer {
  explicit Optimizer(Query *query);

  PlannedStmt *Planner();

  QueryInfo QueryToOperator();

  Group *OptimizePlan(QueryInfo &query_info);

  PlannedStmt *GeneratePlan(QueryInfo &query_info, Group *root_group);

  Query *query;
  OptimizerContext context;
};

}  // namespace pgp