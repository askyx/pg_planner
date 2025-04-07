#include "pg_operator/normalizer.h"

#include <concepts>

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
  { t.Transform(nullptr) } -> std::same_as<OperatorNodePtr>;
};

template <NormalizerRuleConcept T>
OperatorNodePtr NormalizerNodeByRule(OperatorNodePtr expr, const T &rule) {
  if (rule.Match(expr)) {
    expr = rule.Transform(expr);
  }

  OperatorNodeArray pdrgpexpr_children;
  for (const auto &child : expr->children) {
    pdrgpexpr_children.emplace_back(NormalizerNodeByRule(child, rule));
  }

  return MakeOperatorNode(expr->content, pdrgpexpr_children);
}

struct RemoveEmptySelectRule {
  bool Match(OperatorNodePtr expr) const {
    if (expr->content->kind == OperatorType::LogicalFilter) {
      const auto &select = expr->Cast<LogicalFilter>();
      return ConstIsTrue(select.filter);
    }
    return false;
  }

  OperatorNodePtr Transform(const OperatorNodePtr &pexpr) const {
    return NormalizerNodeByRule(pexpr->GetChild(0), RemoveEmptySelectRule{});
  }
};

// main driver, pre-processing of input logical expression
OperatorNodePtr Normalizer::NormalizerTree(OperatorNodePtr pexpr) {
  OLOGLN("Normalizer::NormalizerTree: {}", Format<OperatorNodePtr>::ToString(pexpr));

  pexpr = FilterPushDown::Process(pexpr);

  OLOGLN("After FilterPushDown:\n{}", Format<OperatorNodePtr>::ToString(pexpr));

  pexpr = NormalizerNodeByRule(pexpr, RemoveEmptySelectRule{});

  return pexpr;
}

}  // namespace pgp