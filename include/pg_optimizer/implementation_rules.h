#pragma once

#include "pg_optimizer/rule.h"

namespace pgp {

class CXformGet2TableScan : public Rule {
 public:
  CXformGet2TableScan();

  void Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const override;
};

class CXformImplementLimit : public Rule {
 public:
  CXformImplementLimit();

  void Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const override;
};

class CXformSelect2Filter : public Rule {
 public:
  CXformSelect2Filter();

  void Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const override;
};

class CXformProject2ComputeScalar : public Rule {
 public:
  CXformProject2ComputeScalar();

  void Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const override;
};

class CXformImplementInnerJoin : public Rule {
 public:
  CXformImplementInnerJoin();

  bool Check(GroupExpression *gexpr) const override;

  void Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const override;
};

// HashAggregate
class CXformGbAgg2HashAgg : public Rule {
 public:
  CXformGbAgg2HashAgg();

  bool Check(GroupExpression *gexpr) const override;

  void Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const override;
};

// Plain Aggregate
class CXformGbAgg2ScalarAgg : public Rule {
 public:
  CXformGbAgg2ScalarAgg();

  bool Check(GroupExpression *gexpr) const override;

  void Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const override;
};

// Sorted Aggregate
class CXformGbAgg2StreamAgg : public Rule {
 public:
  CXformGbAgg2StreamAgg();

  bool Check(GroupExpression *gexpr) const override;

  void Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const override;
};

class CXformImplementApply : public Rule {
 public:
  CXformImplementApply();

  void Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const override;
};

class CXformJoin2NLJoin : public Rule {
 public:
  CXformJoin2NLJoin();

  bool Check(GroupExpression *gexpr) const override;

  void Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const override;
};

class CXformJoin2HashJoin : public Rule {
 public:
  CXformJoin2HashJoin();

  void Transform(std::vector<OperatorNode *> &pxfres, OperatorNode *pexpr) const override;
};

}  // namespace pgp