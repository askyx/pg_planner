#pragma once

#include "pg_optimizer/rule.h"

namespace pgp {

class Get2TableScan : public Rule {
 public:
  Get2TableScan();

  void Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const override;
};

class ImplementLimit : public Rule {
 public:
  ImplementLimit();

  void Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const override;
};

class Select2Filter : public Rule {
 public:
  Select2Filter();

  void Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const override;
};

class Project2ComputeScalarRule : public Rule {
 public:
  Project2ComputeScalarRule();

  void Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const override;
};

class ImplementInnerJoin : public Rule {
 public:
  ImplementInnerJoin();

  bool Check(GroupExpression *gexpr) const override;

  void Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const override;
};

// HashAggregate
class GbAgg2HashAgg : public Rule {
 public:
  GbAgg2HashAgg();

  bool Check(GroupExpression *gexpr) const override;

  void Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const override;
};

// Plain Aggregate
class GbAgg2ScalarAgg : public Rule {
 public:
  GbAgg2ScalarAgg();

  bool Check(GroupExpression *gexpr) const override;

  void Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const override;
};

// Sorted Aggregate
class GbAgg2StreamAgg : public Rule {
 public:
  GbAgg2StreamAgg();

  bool Check(GroupExpression *gexpr) const override;

  void Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const override;
};

class ImplementApply : public Rule {
 public:
  ImplementApply();

  void Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const override;
};

class Join2NestedLoopJoin : public Rule {
 public:
  Join2NestedLoopJoin();

  bool Check(GroupExpression *gexpr) const override;

  void Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const override;
};

class Join2HashJoin : public Rule {
 public:
  Join2HashJoin();

  void Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const override;
};

}  // namespace pgp