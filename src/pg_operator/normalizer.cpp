#include "pg_operator/normalizer.h"

#include <concepts>
#include <iostream>

#include "common/formator.h"
#include "pg_operator/filter_push_down.h"
#include "pg_operator/logical_operator.h"
#include "pg_operator/operator.h"
#include "pg_operator/operator_node.h"
#include "pg_operator/operator_utils.h"

extern "C" {
#include <nodes/nodes.h>
}
namespace pgp {

template <typename T>
concept NormalizerRuleConcept = requires(const T &t) {
  { t.Match(nullptr) } -> std::same_as<bool>;
  { t.Transform(nullptr) } -> std::same_as<OperatorNode *>;
};

template <NormalizerRuleConcept T>
OperatorNode *NormalizerNodeByRule(OperatorNode *expr, const T &rule) {
  if (rule.Match(expr)) {
    expr = rule.Transform(expr);
  }

  OperatorNodeArray pdrgpexpr_children;
  for (auto *child : expr->children) {
    pdrgpexpr_children.emplace_back(NormalizerNodeByRule(child, rule));
  }

  return new OperatorNode(expr->content, pdrgpexpr_children);
}

struct RemoveEmptySelectRule {
  bool Match(OperatorNode *expr) const {
    if (expr->content->kind == OperatorType::LogicalFilter) {
      const auto &select = expr->Cast<LogicalFilter>();
      return OperatorUtils::FScalarConstTrue(select.filter);
    }
    return false;
  }

  OperatorNode *Transform(OperatorNode *pexpr) const {
    return NormalizerNodeByRule(pexpr->GetChild(0), RemoveEmptySelectRule{});
  }
};

// main driver, pre-processing of input logical expression
OperatorNode *Normalizer::NormalizerTree(OperatorNode *pexpr) {
  OLOGLN("Normalizer::NormalizerTree: {}", Format<OperatorNode>::ToString(pexpr));

  pexpr = FilterPushDown::Process(pexpr);

  OLOGLN("After FilterPushDown:\n{}", Format<OperatorNode>::ToString(pexpr));

  pexpr = NormalizerNodeByRule(pexpr, RemoveEmptySelectRule{});

  return pexpr;
}

}  // namespace pgp