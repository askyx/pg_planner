#pragma once

#include <stack>

#include "common/macros.h"
#include "pg_optimizer/property.h"
#include "pg_optimizer/rule.h"
namespace pgp {

class Memo;
class Group;
class Rule;
class RuleSet;
class PropertySet;
class GroupExpression;
class OptimizationContext;

enum class OptimizerTaskType {
  OPTIMIZE_GROUP,
  OPTIMIZE_EXPR,
  EXPLORE_GROUP,
  EXPLORE_EXPR,
  APPLY_RULE,
  OPTIMIZE_INPUTS,
};

class OptimizerTask {
 public:
  OptimizerTask(OptimizerTaskType type, OptimizationContext* context) : type_(type), context_(context) {}

  virtual ~OptimizerTask() = default;

  virtual void Execute() = 0;

  virtual void PushTask(OptimizerTask* task);

  virtual RuleSet& GetRuleSet() const;

  virtual Memo& GetMemo() const;

 protected:
  OptimizerTaskType type_;
  OptimizationContext* context_;
};

/**
 * OptimizeGroup optimize a group within a given context.
 * OptimizeGroup will generate tasks to optimize all logically equivalent
 * operator trees if not already explored. OptimizeGroup will then generate
 * tasks to cost all physical operator trees given the current OptimizationContext.
 */
class OptimizeGroup : public OptimizerTask {
 public:
  OptimizeGroup(Group* group, OptimizationContext* context)
      : OptimizerTask(OptimizerTaskType::OPTIMIZE_GROUP, context), group_(group) {}

  void Execute() override;

 private:
  Group* group_;
};

/**
 * OptimizeExpression optimizes a GroupExpression by constructing all logical
 * and physical transformations and applying those rules. The rules are sorted
 * by promise and applied in that order so a physical transformation rule is
 * applied before a logical transformation rule.
 */
class OptimizeExpression : public OptimizerTask {
 public:
  OptimizeExpression(GroupExpression* group_expr, OptimizationContext* context)
      : OptimizerTask(OptimizerTaskType::OPTIMIZE_EXPR, context), group_expr_(group_expr) {}

  void Execute() override;

 private:
  GroupExpression* group_expr_;
};

/**
 * ExploreGroup generates all logical transformation expressions by applying
 * logical transformation rules to logical operators until saturation.
 */
class ExploreGroup : public OptimizerTask {
 public:
  ExploreGroup(Group* group, OptimizationContext* context)
      : OptimizerTask(OptimizerTaskType::EXPLORE_GROUP, context), group_(group) {}

  void Execute() override;

 private:
  Group* group_;
};

/**
 * ExploreExpression applies logical transformation rules to a GroupExpression
 * until no more logical transformation rules can be applied. ExploreExpression
 * will also descend and explorenon-leaf children groups.
 */
class ExploreExpression : public OptimizerTask {
 public:
  ExploreExpression(GroupExpression* group_expr, OptimizationContext* context)
      : OptimizerTask(OptimizerTaskType::EXPLORE_EXPR, context), group_expr_(group_expr) {}

  void Execute() override;

 private:
  GroupExpression* group_expr_;
};

/**
 * ApplyRule applies a rule. If it is a logical transformation rule, we need to
 * explore (apply logical rules) or optimize (apply logical & physical rules)
 * the new group expression based on the explore flag. If the rule is a
 * physical implementation rule, we directly cost the physical expression
 */
class ApplyRule : public OptimizerTask {
 public:
  ApplyRule(GroupExpression* group_expr, Rule* rule, OptimizationContext* context, bool explore_only = false)
      : OptimizerTask(OptimizerTaskType::APPLY_RULE, context),
        group_expr_(group_expr),
        rule_(rule),
        explore_only_(explore_only) {}

  void Execute() override;

 private:
  GroupExpression* group_expr_;

  Rule* rule_;

  [[maybe_unused]] bool explore_only_;
};

/**
 * OptimizeExpressionCostWithEnforcedProperty costs a physical expression. The root operator is costed first
 * and the lowest cost of each child group is added. Finally, properties are
 * enforced to meet requirement in the context. We apply pruning by terminating if
 * the current expression's cost is larger than the upper bound of the current group
 */
class OptimizeExpressionCostWithEnforcedProperty : public OptimizerTask {
 public:
  OptimizeExpressionCostWithEnforcedProperty(GroupExpression* group_expr, OptimizationContext* context)
      : OptimizerTask(OptimizerTaskType::OPTIMIZE_INPUTS, context), group_expr_(group_expr) {}

  explicit OptimizeExpressionCostWithEnforcedProperty(OptimizeExpressionCostWithEnforcedProperty* task)
      : OptimizerTask(OptimizerTaskType::OPTIMIZE_INPUTS, task->context_),
        output_input_properties_(std::move(task->output_input_properties_)),
        group_expr_(task->group_expr_),
        cur_total_cost_(task->cur_total_cost_),
        cur_child_idx_(task->cur_child_idx_),
        cur_prop_pair_idx_(task->cur_prop_pair_idx_) {}

  void Execute() override;

  ~OptimizeExpressionCostWithEnforcedProperty() override {
    for (auto& pair : output_input_properties_) {
      delete pair.first;
      for (auto& prop : pair.second) {
        delete prop;
      }
    }
  }

 private:
  std::vector<std::pair<PropertySet*, std::vector<PropertySet*>>> output_input_properties_;

  GroupExpression* group_expr_;

  double cur_total_cost_;

  int cur_child_idx_ = -1;

  int prev_child_idx_ = -1;

  int cur_prop_pair_idx_ = 0;
};

class OptimizerTaskStack {
 public:
  DISALLOW_COPY(OptimizerTaskStack)

  OptimizerTaskStack() = default;

  OptimizerTask* Pop() {
    // ownership handed off to caller
    auto* task = task_stack_.top();
    task_stack_.pop();
    return task;
  }

  ~OptimizerTaskStack() {
    while (!task_stack_.empty()) {
      auto* task = task_stack_.top();
      task_stack_.pop();
      delete task;
    }
  }

  void Push(OptimizerTask* task) {
    // ownership trasnferred to stack
    task_stack_.push(task);
  }

  bool Empty() { return task_stack_.empty(); }

 private:
  std::stack<OptimizerTask*> task_stack_;
};

}  // namespace pgp