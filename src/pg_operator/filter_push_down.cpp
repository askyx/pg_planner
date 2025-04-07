#include "pg_operator/filter_push_down.h"

#include <memory>

#include "pg_operator/item_expr.h"
#include "pg_operator/logical_operator.h"
#include "pg_operator/operator.h"
#include "pg_operator/operator_node.h"
#include "pg_operator/operator_utils.h"
#include "pg_optimizer/colref.h"

extern "C" {
#include <nodes/nodes.h>
#include <tcop/tcopprot.h>
}
namespace pgp {

// push scalar expression through logical expression
static OperatorNodePtr PushThrux(OperatorNodePtr pexpr_logical, const ItemExprPtr &pexpr_conj);

// push an array of conjuncts through logical expression, and compute an array of remaining conjuncts
static OperatorNodePtr PushThru(OperatorNodePtr pexpr_logical, const ExprArray &pdrgpexpr_conjuncts,
                                ExprArray &ppdrgpexpr_remaining);

bool PushThruApply(const OperatorNodePtr &pexpr_logical) {
  if (pexpr_logical->content->kind == OperatorType::LogicalApply) {
    const auto &apply = pexpr_logical->Cast<LogicalApply>();
    return apply.subquery_type == ANY_SUBLINK || apply.subquery_type == EXISTS_SUBLINK;
  }
  return false;
}

bool FPushThruOuterChild(const OperatorNodePtr &pexpr_logical) {
  auto bool_ret = PushThruApply(pexpr_logical);

  if (!bool_ret && pexpr_logical->content->kind == OperatorType::LogicalJoin) {
    return pexpr_logical->Cast<LogicalJoin>().join_type == JOIN_LEFT;
  }

  return bool_ret;
}

static bool FPushable(const OperatorNodePtr &pexpr_logical, const ItemExprPtr &pexpr_pred) {
  ColRefSet pcrs_used = pexpr_pred->DeriveUsedColumns();
  ColRefSet pcrs_output = pexpr_logical->DeriveOutputColumns();

  //	In case of a Union or UnionAll the predicate might get pushed
  //	to multiple children In such cases we will end up with duplicate
  //	CTEProducers having the same cte_id.
  return ContainsAll(pcrs_output, pcrs_used);
}

static OperatorNodePtr PexprRecursiveNormalize(const OperatorNodePtr &pexpr) {
  OperatorNodeArray pdrgpexpr;

  for (const auto &child : pexpr->children)
    pdrgpexpr.emplace_back(FilterPushDown::Process(child));

  return MakeOperatorNode(pexpr->content, pdrgpexpr);
}

static void SplitConjunct(const OperatorNodePtr &pexpr, const ItemExprPtr &pexpr_conj, ExprArray &ppdrgpexpr_pushable,
                          ExprArray &ppdrgpexpr_unpushable) {
  // collect pushable predicates from given conjunct
  auto pdrgpexpr_conjuncts = OperatorUtils::PdrgpexprConjuncts(pexpr_conj);
  for (const auto &pexpr_scalar : pdrgpexpr_conjuncts) {
    if (FPushable(pexpr, pexpr_scalar)) {
      ppdrgpexpr_pushable.emplace_back(pexpr_scalar);
    } else {
      ppdrgpexpr_unpushable.emplace_back(pexpr_scalar);
    }
  }
}

OperatorNodePtr PexprSelect(OperatorNodePtr pexpr, const ExprArray &pdrgpexpr) {
  if (pdrgpexpr.empty())
    return pexpr;

  auto pexpr_conjunction = OperatorUtils::PexprConjunction(pdrgpexpr);
  return OperatorUtils::PexprSafeSelect(pexpr, pexpr_conjunction);
}

OperatorNodePtr PushThrux(OperatorNodePtr pexpr_logical, const ItemExprPtr &pexpr_conj) {
  switch (pexpr_logical->content->kind) {
    case OperatorType::LogicalGet: {
      auto &get = pexpr_logical->Cast<LogicalGet>();
      auto filter = OperatorUtils::PexprConjunction(get.filter, pexpr_conj);
      if (!ConstIsTrue(filter))
        get.filter = filter;
      return pexpr_logical;
    }
    case OperatorType::LogicalFilter: {
      auto &select = pexpr_logical->Cast<LogicalFilter>();
      OperatorNodePtr child_node = pexpr_logical->GetChild(0);
      auto pexpr_pred = OperatorUtils::PexprConjunction(select.filter, pexpr_conj);

      auto pdrgpexpr_conjuncts = OperatorUtils::PdrgpexprConjuncts(pexpr_pred);
      ExprArray pdrgpexpr_remaining;
      OperatorNodePtr pexpr = PushThru(child_node, pdrgpexpr_conjuncts, pdrgpexpr_remaining);
      if (pdrgpexpr_remaining.empty())
        return pexpr;

      auto pexpr_conjunction = OperatorUtils::PexprConjunction(pdrgpexpr_remaining);
      if (pexpr->content->kind == OperatorType::LogicalGet) {
        ColRefSet pcrs_used = pexpr_conjunction->DeriveUsedColumns();
        ColRefSet pcrs_output = pexpr->DeriveOutputColumns();
        if (ColRefSetIntersects(pcrs_used, pcrs_output)) {
          auto &get = pexpr->Cast<LogicalGet>();
          auto filter = OperatorUtils::PexprConjunction(get.filter, pexpr_conjunction);
          get.filter = filter;
          return pexpr;
        }
      }
      return OperatorUtils::PexprSafeSelect(pexpr, pexpr_conjunction);
    } break;

    case OperatorType::LogicalJoin:
    case OperatorType::LogicalApply: {
      auto f_outer_join = FPushThruOuterChild(pexpr_logical);

      ItemExprPtr pexpr_scalar;
      if (pexpr_logical->content->kind == OperatorType::LogicalJoin) {
        pexpr_scalar = pexpr_logical->Cast<LogicalJoin>().filter;
      } else {
        pexpr_scalar = pexpr_logical->Cast<LogicalApply>().filter;
      }

      auto pexpr_pred = OperatorUtils::PexprConjunction(pexpr_scalar, pexpr_conj);
      auto pdrgpexpr_conjuncts = OperatorUtils::PdrgpexprConjuncts(pexpr_pred);
      OperatorNodeArray pdrgpexpr_children;

      for (uint32_t ul = 0; ul < pexpr_logical->ChildrenSize(); ul++) {
        OperatorNodePtr pexpr_child = pexpr_logical->GetChild(ul);

        if (0 == ul && f_outer_join) {
          pdrgpexpr_children.emplace_back(FilterPushDown::Process(pexpr_child));
          continue;
        }

        ExprArray pdrgpexpr_remaining;
        pdrgpexpr_children.emplace_back(PushThru(pexpr_child, pdrgpexpr_conjuncts, pdrgpexpr_remaining));

        pdrgpexpr_conjuncts = pdrgpexpr_remaining;
      }

      // remaining conjuncts become the new join predicate
      auto pexpr_new_scalar = OperatorUtils::PexprConjunction(pdrgpexpr_conjuncts);

      if (pexpr_logical->content->kind == OperatorType::LogicalJoin) {
        auto &join = pexpr_logical->Cast<LogicalJoin>();
        join.filter = pexpr_new_scalar;
      } else {
        auto &apply = pexpr_logical->Cast<LogicalApply>();
        apply.filter = pexpr_new_scalar;
      }

      return MakeOperatorNode(pexpr_logical->content, pdrgpexpr_children);
    }

    default: {
      OperatorNodePtr pexpr_normalized = PexprRecursiveNormalize(pexpr_logical);
      return OperatorUtils::PexprSafeSelect(pexpr_normalized, pexpr_conj);
    }
  }
  return nullptr;
}

OperatorNodePtr PushThru(OperatorNodePtr pexpr_logical, const ExprArray &pdrgpexpr_conjuncts,
                         ExprArray &ppdrgpexpr_remaining) {
  ExprArray pdrgpexpr_pushable;
  ExprArray pdrgpexpr_unpushable;

  for (const auto &pexpr_conj : pdrgpexpr_conjuncts) {
    if (FPushable(pexpr_logical, pexpr_conj)) {
      pdrgpexpr_pushable.emplace_back(pexpr_conj);
    } else {
      pdrgpexpr_unpushable.emplace_back(pexpr_conj);
    }
  }

  ppdrgpexpr_remaining = pdrgpexpr_unpushable;

  // push through a conjunction of all pushable predicates
  auto pexpr_pred = OperatorUtils::PexprConjunction(pdrgpexpr_pushable);
  if (FPushThruOuterChild(pexpr_logical)) {
    if (pexpr_logical->children.empty())
      return pexpr_logical;

    OperatorNodePtr pexpr_outer = pexpr_logical->GetChild(0);
    OperatorNodePtr pexpr_inner = pexpr_logical->GetChild(1);

    ExprArray pdrgpexpr_pushable;
    ExprArray pdrgpexpr_unpushable;
    SplitConjunct(pexpr_outer, pexpr_pred, pdrgpexpr_pushable, pdrgpexpr_unpushable);

    auto ppexpr_result = pexpr_logical;

    if (!pdrgpexpr_pushable.empty()) {
      auto pexpr_new_conj = OperatorUtils::PexprConjunction(pdrgpexpr_pushable);

      auto pexpr_new_select = MakeOperatorNode(std::make_shared<LogicalFilter>(pexpr_new_conj), {pexpr_outer});

      auto pexpr_new_outer = PushThrux(pexpr_new_select, pexpr_new_conj);

      auto pexpr_new = MakeOperatorNode(pexpr_logical->content, {pexpr_new_outer, pexpr_inner});

      ppexpr_result = PushThrux(pexpr_new, OperatorUtils::PexprScalarConstBool(true));
    }

    if (!pdrgpexpr_unpushable.empty()) {
      OperatorNodePtr pexpr_outer_join = pexpr_logical;

      if (!pdrgpexpr_pushable.empty()) {
        pexpr_outer_join = ppexpr_result;
      }

      auto pexpr_new = PushThrux(pexpr_outer_join, OperatorUtils::PexprScalarConstBool(true));

      return PexprSelect(pexpr_new, pdrgpexpr_unpushable);
    }
    return ppexpr_result;
  }
  return PushThrux(pexpr_logical, pexpr_pred);
}

OperatorNodePtr FilterPushDown::Process(OperatorNodePtr pexpr) {
  if (pexpr->children.empty())
    return pexpr;

  if (auto opid = pexpr->content->kind;
      opid == OperatorType::LogicalJoin || opid == OperatorType::LogicalApply || opid == OperatorType::LogicalFilter) {
    ItemExprPtr filter;
    if (FPushThruOuterChild(pexpr))
      filter = OperatorUtils::PexprScalarConstBool(true);
    else {
      if (opid == OperatorType::LogicalFilter) {
        filter = pexpr->Cast<LogicalFilter>().filter;
      } else if (opid == OperatorType::LogicalJoin) {
        filter = pexpr->Cast<LogicalJoin>().filter;
      } else {
        filter = pexpr->Cast<LogicalApply>().filter;
      }
    }

    return PushThrux(pexpr, filter);
  }

  return PexprRecursiveNormalize(pexpr);
}

}  // namespace pgp