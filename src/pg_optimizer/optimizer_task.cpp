#include "pg_optimizer/optimizer_task.h"

#include <cstdint>
#include <vector>

#include "pg_optimizer/binding.h"
#include "pg_optimizer/child_property_deriver.h"
#include "pg_optimizer/cost_model.h"
#include "pg_optimizer/group.h"
#include "pg_optimizer/group_expression.h"
#include "pg_optimizer/memo.h"
#include "pg_optimizer/optimizer_context.h"
#include "pg_optimizer/pattern.h"
#include "pg_optimizer/property_enforcer.h"
#include "pg_optimizer/rule.h"

namespace pgp {

void OptimizerTask::PushTask(OptimizerTask *task) {
  context_->GetOptimizerContext()->PushTask(task);
}

Memo &OptimizerTask::GetMemo() const {
  return context_->GetOptimizerContext()->GetMemo();
}

RuleSet &OptimizerTask::GetRuleSet() const {
  return context_->GetOptimizerContext()->GetRuleSet();
}

void OptimizeGroup::Execute() {
  if (group_->GetBestExpression(context_->GetRequiredProperties()) != nullptr)
    return;

  // Push explore task first for logical expressions if the group has not been explored
  OLOGLN("OptimizeGroup::execute() for group #{}", group_->GetGroupId());
  if (!group_->HasExplored()) {
    for (const auto &logical_expr : group_->GetLogicalExpressions()) {
      PushTask(new OptimizeExpression(logical_expr, context_));
    }
  }

  // Push implement tasks to ensure that they are run first (for early pruning)
  for (const auto &physical_expr : group_->GetPhysicalExpressions()) {
    PushTask(new OptimizeExpressionCostWithEnforcedProperty(physical_expr, context_));
  }

  // Since there is no cycle in the tree, it is safe to set the flag even before
  // all expressions are explored
  group_->SetExplorationFlag();
}

void ConstructValidRules(GroupExpression *group_expr, const std::vector<Rule *> &rules,
                         std::vector<RuleWithPromise> *valid_rules) {
  for (auto *rule : rules) {
    // Check if we can apply the rule
    bool root_pattern_mismatch = group_expr->Pop()->kind != rule->GetMatchPattern()->Type();
    bool already_explored = group_expr->HasRuleExplored(rule);

    // This check exists only as an "early" reject. As is evident, we do not check
    // the full patern here. Checking the full pattern happens when actually trying to
    // apply the rule (via a GroupExprBindingIterator).
    bool child_pattern_mismatch = group_expr->ChildrenSize() != rule->GetMatchPattern()->GetChildPatternsSize() &&
                                  !rule->GetMatchPattern()->IsMultiLeaf();

    if (root_pattern_mismatch || already_explored || child_pattern_mismatch) {
      continue;
    }

    auto promise = rule->Promise(group_expr);
    if (promise != RulePromise::NO_PROMISE)
      valid_rules->emplace_back(rule, promise);
  }
}

void OptimizeExpression::Execute() {
  std::vector<RuleWithPromise> valid_rules;

  // Construct valid transformation rules from rule set
  auto logical_rules = GetRuleSet().GetRulesByName(RuleCategory::Exploration);
  auto phys_rules = GetRuleSet().GetRulesByName(RuleCategory::Implementation);
  ConstructValidRules(group_expr_, logical_rules, &valid_rules);
  ConstructValidRules(group_expr_, phys_rules, &valid_rules);

  std::sort(valid_rules.begin(), valid_rules.end());
  OLOGLN("OptimizeExpression::execute() op {0}, valid rules : {1}", group_expr_->Pop()->ToString(), valid_rules.size());

  // Apply rule
  for (auto &r : valid_rules) {
    PushTask(new ApplyRule(group_expr_, r.GetRule(), context_));
    int child_group_idx = 0;
    for (const auto &child_pattern : r.GetRule()->GetMatchPattern()->Children()) {
      // If child_pattern has any more children (i.e non-leaf), then we will explore the
      // child before applying the rule. (assumes task pool is effectively a stack)
      if (child_pattern->GetChildPatternsSize() > 0) {
        Group *group = group_expr_->GetChildGroup()[child_group_idx];
        PushTask(new ExploreGroup(group, context_));
      }

      child_group_idx++;
    }
  }
}

void ExploreGroup::Execute() {
  if (group_->HasExplored())
    return;

  OLOGLN("ExploreGroup::execute() for group {0}", group_->GetGroupId());

  for (const auto &logical_expr : group_->GetLogicalExpressions()) {
    PushTask(new ExploreExpression(logical_expr, context_));
  }

  group_->SetExplorationFlag();
}

void ExploreExpression::Execute() {
  std::vector<RuleWithPromise> valid_rules;

  // Construct valid transformation rules from rule set
  auto logical_rules = GetRuleSet().GetRulesByName(RuleCategory::Exploration);
  ConstructValidRules(group_expr_, logical_rules, &valid_rules);
  std::sort(valid_rules.begin(), valid_rules.end());

  OLOGLN("ExploreExpression::execute() for group_expr_ {0}", group_expr_->ToString());

  // Apply rule
  for (auto &r : valid_rules) {
    PushTask(new ApplyRule(group_expr_, r.GetRule(), context_, true));
    int child_group_idx = 0;
    for (const auto &child_pattern : r.GetRule()->GetMatchPattern()->Children()) {
      // Only need to explore non-leaf children before applying rule to the
      // current group. this condition is important for early-pruning
      if (child_pattern->GetChildPatternsSize() > 0) {
        Group *group = group_expr_->GetChildGroup()[child_group_idx];
        PushTask(new ExploreGroup(group, context_));
      }

      child_group_idx++;
    }
  }
}

void ApplyRule::Execute() {
  if (group_expr_->HasRuleExplored(rule_))
    return;

  OLOGLN("ApplyRule::execute() for group_expr_ {0} apply rule {1}", group_expr_->ToString(),
         (int8_t)rule_->GetRuleType());

  GroupExprBindingIterator iterator(GetMemo(), group_expr_, rule_->GetMatchPattern());
  while (iterator.HasNext()) {
    auto *before = iterator.Next();
    if (!rule_->Check(group_expr_)) {
      continue;
    }

    // Caller frees after
    std::vector<OperatorNode *> after;
    rule_->Transform(after, before);
    for (const auto &new_expr : after) {
      auto *group = group_expr_->GetGroup();
      GroupExpression *new_gexpr = nullptr;
      if (context_->GetOptimizerContext()->RecordOptimizerNodeIntoGroup(new_expr, &new_gexpr, group)) {
        // A new group expression is generated
        if (new_gexpr->Pop()->Logical()) {
          // Derive stats for the *logical expression*
          // PushTask(new DeriveStats(new_gexpr, context_));
          if (explore_only_) {
            // Explore this logical expression
            PushTask(new ExploreExpression(new_gexpr, context_));
          } else {
            // Optimize this logical expression
            PushTask(new OptimizeExpression(new_gexpr, context_));
          }
        } else {
          // Cost this physical expression and optimize its inputs
          PushTask(new OptimizeExpressionCostWithEnforcedProperty(new_gexpr, context_));
        }
      }
    }
  }
  group_expr_->SetRuleExplored(rule_);
}

void OptimizeExpressionCostWithEnforcedProperty::Execute() {
  // Init logic: only run once per task
  if (cur_child_idx_ == -1) {
    // TODO(patrick):
    // 1. We can init input cost using non-zero value for pruning
    // 2. We can calculate the current operator cost if we have maintain
    //    logical properties in group (e.g. stats, schema, cardinality)
    cur_total_cost_ = 0;

    // Pruning
    if (cur_total_cost_ > context_->GetCostUpperBound())
      return;

    // Derive output and input properties
    ChildPropertyDeriver prop_deriver;
    output_input_properties_ = prop_deriver.GetProperties(&context_->GetOptimizerContext()->GetMemo(),
                                                          context_->GetRequiredProperties(), group_expr_);
    cur_child_idx_ = 0;

    // TODO(patrick/boweic): If later on we support properties that may not be enforced in some
    // cases, we can check whether it is the case here to do the pruning
  }

  OLOGLN("OptimizeExpressionCostWithEnforcedProperty::execute() for group_expr_ {}", group_expr_->ToString());

  // Loop over (output prop, input props) pair for the GroupExpression being optimized
  // (1) Cost children (if needed); or pick the best child expression (in terms of cost)
  // (2) Enforce any missing properties as required
  // (3) Update Group/Context metadata of expression + cost
  for (; cur_prop_pair_idx_ < static_cast<int>(output_input_properties_.size()); cur_prop_pair_idx_++) {
    auto &output_prop = output_input_properties_[cur_prop_pair_idx_].first;
    auto &input_props = output_input_properties_[cur_prop_pair_idx_].second;

    // Calculate local cost and update total cost
    if (cur_child_idx_ == 0) {
      CostCalculator cost_calculator;
      cur_total_cost_ = cost_calculator.CalculateCost(&context_->GetOptimizerContext()->GetMemo(), group_expr_);
    }

    auto child_size = group_expr_->GetChildGroup().size();
    for (; cur_child_idx_ < static_cast<int>(child_size); cur_child_idx_++) {
      const auto &i_prop = input_props[cur_child_idx_];
      auto *child_group = group_expr_->GetChildGroup()[cur_child_idx_];
      // Check whether the child group is already optimized for the prop
      auto *child_best_expr = child_group->GetBestExpression(i_prop);
      if (child_best_expr != nullptr) {  // Directly get back the best expr if the child group is optimized
        cur_total_cost_ += child_best_expr->GetCost(i_prop);
        if (cur_total_cost_ > context_->GetCostUpperBound())
          break;
      } else if (prev_child_idx_ != cur_child_idx_) {  // We haven't optimized child group
        prev_child_idx_ = cur_child_idx_;
        PushTask(new OptimizeExpressionCostWithEnforcedProperty(this));

        // auto cost_high = context_->GetCostUpperBound() - cur_total_cost_;
        auto *ctx = new OptimizationContext(i_prop);
        ctx->SetGlobalOptimizerContext(context_->GetOptimizerContext());
        PushTask(new OptimizeGroup(child_group, ctx));
        // context_->GetOptimizerContext()->AddOptimizationContext(ctx);
        return;
      } else {  // If we return from OptimizeGroup, then there is no expr for the context
        break;
      }
    }

    // TODO(wz2): Can we reduce the amount of copying
    // Check whether we successfully optimize all child group
    if (cur_child_idx_ == static_cast<int>(child_size)) {
      // Not need to do pruning here because it has been done when we get the
      // best expr from the child group

      // Add this group expression to group expression hash table
      std::vector<std::shared_ptr<PropertySet>> input_props_copy;
      input_props_copy.reserve(input_props.size());
      for (const auto &i_prop : input_props) {
        input_props_copy.push_back(i_prop->Copy());
      }

      group_expr_->SetLocalHashTable(output_prop->Copy(), input_props_copy, cur_total_cost_);
      auto *cur_group = group_expr_->GetGroup();
      cur_group->SetExpressionCost(group_expr_, cur_total_cost_, output_prop->Copy());

      // Enforce property if the requirement does not meet
      PropertyEnforcer prop_enforcer;
      GroupExpression *memo_enforced_expr = nullptr;
      bool meet_requirement = true;

      // TODO(patrick/boweic): For now, we enforce the missing properties in the order of how we
      // find them. This may miss the opportunity to enforce them or may lead to
      // sub-optimal plan. This is fine now because we only have one physical
      // property (sort). If more properties are added, we should add some heuristics
      // to derive the optimal enforce order or perform a cost-based full enumeration.
      for (const auto &prop : context_->GetRequiredProperties()->Properties()) {
        if (!output_prop->HasProperty(*prop)) {
          auto *enforced_expr = prop_enforcer.EnforceProperty(group_expr_, prop);
          // Cannot enforce the missing property
          if (enforced_expr == nullptr) {
            meet_requirement = false;
            break;
          }

          GetMemo().InsertExpression(nullptr, enforced_expr, group_expr_->GetGroup(), true);

          // Extend the output properties after enforcement
          auto pre_output_prop_set = output_prop->Copy();

          // Cost the enforced expression
          auto extended_prop_set = output_prop->Copy();
          extended_prop_set->AddProperty(prop->Copy());

          CostCalculator cost_calculator;
          cur_total_cost_ += cost_calculator.CalculateCost(&context_->GetOptimizerContext()->GetMemo(), enforced_expr);

          enforced_expr->SetLocalHashTable(extended_prop_set, {pre_output_prop_set}, cur_total_cost_);
          cur_group->SetExpressionCost(enforced_expr, cur_total_cost_, extended_prop_set);
        }
      }

      // Can meet the requirement
      if (meet_requirement && cur_total_cost_ <= context_->GetCostUpperBound()) {
        // If the cost is smaller than the winner, update the context upper bound
        context_->SetCostUpperBound(context_->GetCostUpperBound() - cur_total_cost_);
        if (memo_enforced_expr != nullptr) {  // Enforcement takes place
          cur_group->SetExpressionCost(memo_enforced_expr, cur_total_cost_, context_->GetRequiredProperties()->Copy());
        } else if (output_prop->Properties().size() != context_->GetRequiredProperties()->Properties().size()) {
          // The original output property is a super set of the requirement
          cur_group->SetExpressionCost(group_expr_, cur_total_cost_, context_->GetRequiredProperties()->Copy());
        }
      }
    }

    // Reset child idx and total cost
    prev_child_idx_ = -1;
    cur_child_idx_ = 0;
    cur_total_cost_ = 0;
  }
}

}  // namespace pgp