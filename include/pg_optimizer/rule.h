#pragma once

#include <unordered_map>
#include <vector>

#include "common/macros.h"
#include "pg_operator/operator_node.h"
#include "pg_optimizer/pattern.h"

extern "C" {
#include <nodes/bitmapset.h>
}

namespace pgp {

class Pattern;
class OperatorNode;
class GroupExpression;
class OptimizationContext;

enum class RulePromise : uint32_t {
  /**
   * NO promise. Rule cannot be used
   */
  NO_PROMISE = 0,

  /**
   * Logical rule/low priority unnest
   */
  LOGICAL_PROMISE = 1,

  /**
   * High priority unnest
   */
  UNNEST_PROMISE_HIGH = 2,

  /**
   * Physical rule
   */
  PHYSICAL_PROMISE = 3
};

enum RuleType : uint8_t {
  ExfInnerJoinCommutativity,

  EXP_IMP_DETERMI,

  ExfJoin2NLJoin,
  ExfJoin2HashJoin,

  ExfGet2TableScan,
  ExfImplementLimit,
  ExfSelect2Filter,
  ExfProject2ComputeScalar,
  ExfImplementInnerJoin,
  ExfGbAgg2HashAgg,
  ExfGbAgg2StreamAgg,
  ExfGbAgg2ScalarAgg,

  ExfImplementApply,

  ExfSentinel
};

class Rule {
 public:
  virtual ~Rule() { delete match_pattern_; }

  virtual RuleType GetRuleType() const { return rule_type_; }

  bool FExploration() const { return rule_type_ < RuleType::EXP_IMP_DETERMI; }

  bool FImplementation() const { return rule_type_ > RuleType::EXP_IMP_DETERMI; }

  // actual transformation
  virtual void Transform(OperatorNodeArray &pxfres, const OperatorNodePtr &pexpr) const = 0;

  virtual RulePromise Promise(GroupExpression *group_expr) const;

  virtual bool Check(GroupExpression *gexpr) const { return true; }

  virtual Pattern *GetMatchPattern() const { return match_pattern_; }

  virtual bool Check(const OperatorNodePtr &pexpr, OptimizationContext *context) const { return true; }

 protected:
  Pattern *match_pattern_;

  RuleType rule_type_;
};

enum class RuleCategory {
  Exploration,
  Implementation,
};

class RuleSet {
 private:
  std::unordered_map<RuleCategory, std::vector<Rule *>> rule_set_;

  void Add(Rule *pxform);

 public:
  DISALLOW_COPY_AND_MOVE(RuleSet)

  RuleSet();

  ~RuleSet();

  std::vector<Rule *> &GetRulesByName(RuleCategory type) { return rule_set_[type]; }
};

struct RuleWithPromise {
 public:
  RuleWithPromise(Rule *rule, RulePromise promise) : rule_(rule), promise_(promise) {}

  Rule *GetRule() { return rule_; }

  RulePromise GetPromise() { return promise_; }

  bool operator<(const RuleWithPromise &r) const { return promise_ < r.promise_; }

  bool operator>(const RuleWithPromise &r) const { return promise_ > r.promise_; }

 private:
  Rule *rule_;

  RulePromise promise_;
};

}  // namespace pgp
