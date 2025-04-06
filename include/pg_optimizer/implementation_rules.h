#pragma once

#include <utility>

#include "pg_operator/item_expr.h"
#include "pg_operator/operator_node.h"
#include "pg_optimizer/rule.h"

namespace pgp {

struct IndexInfo;

class Get2TableScan : public Rule {
 public:
  Get2TableScan();

  void Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr, OptimizationContext *context) const override;
};

class Get2IndexScan : public Rule {
 public:
  Get2IndexScan();

  bool Check(GroupExpression *gexpr) const override;

  void Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr, OptimizationContext *context) const override;

  std::pair<ExprArray, ExprArray> ExtractIndexPredicates(const ExprArray &predicates,
                                                         const IndexInfo *index_info) const;
};

class ImplementLimit : public Rule {
 public:
  ImplementLimit();

  void Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr, OptimizationContext *context) const override;
};

class Select2Filter : public Rule {
 public:
  Select2Filter();

  void Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr, OptimizationContext *context) const override;
};

class Project2ComputeScalarRule : public Rule {
 public:
  Project2ComputeScalarRule();

  void Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr, OptimizationContext *context) const override;
};

class ImplementInnerJoin : public Rule {
 public:
  ImplementInnerJoin();

  bool Check(GroupExpression *gexpr) const override;

  void Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr, OptimizationContext *context) const override;
};

// HashAggregate
class GbAgg2HashAgg : public Rule {
 public:
  GbAgg2HashAgg();

  bool Check(GroupExpression *gexpr) const override;

  void Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr, OptimizationContext *context) const override;
};

// Plain Aggregate
class GbAgg2ScalarAgg : public Rule {
 public:
  GbAgg2ScalarAgg();

  bool Check(GroupExpression *gexpr) const override;

  void Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr, OptimizationContext *context) const override;
};

// Sorted Aggregate
class GbAgg2StreamAgg : public Rule {
 public:
  GbAgg2StreamAgg();

  bool Check(GroupExpression *gexpr) const override;

  void Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr, OptimizationContext *context) const override;
};

class ImplementApply : public Rule {
 public:
  ImplementApply();

  void Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr, OptimizationContext *context) const override;
};

class Join2NestedLoopJoin : public Rule {
 public:
  Join2NestedLoopJoin();

  bool Check(GroupExpression *gexpr) const override;

  void Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr, OptimizationContext *context) const override;
};

class Join2HashJoin : public Rule {
 public:
  Join2HashJoin();

  void Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr, OptimizationContext *context) const override;
};

}  // namespace pgp