#include "pg_operator/operator_utils.h"

#include <cstring>
#include <memory>

#include "pg_operator/item_expr.h"
#include "pg_operator/logical_operator.h"
#include "pg_operator/operator_node.h"

extern "C" {
#include <catalog/pg_type_d.h>
#include <nodes/makefuncs.h>
#include <nodes/nodes.h>
#include <nodes/pg_list.h>
#include <utils/lsyscache.h>
}

namespace pgp {

// Generate a comparison expression over an expression and a column
ItemExprPtr CUtils::PexprScalarCmp(const ItemExprPtr &pexprLeft, ColRef *pcrRight, Oid mdid_op) {
  auto scalar_op = std::make_shared<ItemCmpExpr>(mdid_op);
  scalar_op->AddChild(pexprLeft);
  scalar_op->AddChild(std::make_shared<ItemIdent>(pcrRight));
  return scalar_op;
}

// generate a boolean scalar constant expression
ItemExprPtr CUtils::PexprScalarConstBool(bool value, bool is_null) {
  // create a bool datum
  auto *datum = makeBoolConst(value, is_null);

  return std::make_shared<ItemConst>((Const *)datum);
}

// if predicate is True return logical expression, otherwise return a new select node
OperatorNode *CUtils::PexprSafeSelect(OperatorNode *pexpr_logical, const ItemExprPtr &pexpr_pred) {
  if (FScalarConstTrue(pexpr_pred)) {
    // caller must have add-refed the predicate before coming here
    return pexpr_logical;
  }

  return new OperatorNode(std::make_shared<LogicalFilter>(pexpr_pred), {pexpr_logical});
}

// check if the expression is a scalar boolean const
bool FScalarConstBool(const ItemExprPtr &pexpr, bool value) {
  if (ExpressionKind::EopScalarConst == pexpr->kind) {
    auto *cvalue = pexpr->Cast<ItemConst>().value;
    if (BOOLOID == cvalue->consttype) {
      return !cvalue->constisnull && (bool)cvalue->constvalue == value;
    }
  }

  return false;
}

// checks to see if the expression is a scalar const TRUE
bool CUtils::FScalarConstTrue(const ItemExprPtr &pexpr) {
  return FScalarConstBool(pexpr, true);
}

// checks to see if the expression is a scalar const FALSE
bool CUtils::FScalarConstFalse(const ItemExprPtr &pexpr) {
  return FScalarConstBool(pexpr, false);
}

// is the given expression a scalar bool op of the passed type?
bool CUtils::FScalarBoolOp(const ItemExprPtr &pexpr, BoolExprType eboolop) {
  return ExpressionKind::EopScalarBoolOp == pexpr->kind && eboolop == pexpr->Cast<ItemBoolExpr>().boolop;
}

// recursively collect conjuncts
void CollectConjuncts(const ItemExprPtr &pexpr, ExprArray &pdrgpexpr) {
  if (CUtils::FAnd(pexpr)) {
    for (const auto &arg : pexpr->GetChildren())
      CollectConjuncts(arg, pdrgpexpr);
  } else {
    pdrgpexpr.emplace_back(pexpr);
  }
}

// extract conjuncts from a predicate
ExprArray CUtils::PdrgpexprConjuncts(const ItemExprPtr &pexpr) {
  ExprArray pdrgpexpr;

  CollectConjuncts(pexpr, pdrgpexpr);

  return pdrgpexpr;
}

// check if a conjunct/disjunct can be skipped
bool FSkippable(const ItemExprPtr &pexpr, bool fConjunction) {
  return ((fConjunction && CUtils::FScalarConstTrue(pexpr)) || (!fConjunction && CUtils::FScalarConstFalse(pexpr)));
}

// check if a conjunction/disjunction can be reduced to a constant
// True/False based on the given conjunct/disjunct
bool FReducible(const ItemExprPtr &pexpr, bool fConjunction) {
  return ((fConjunction && CUtils::FScalarConstFalse(pexpr)) || (!fConjunction && CUtils::FScalarConstTrue(pexpr)));
}

// create conjunction/disjunction from array of components; Takes ownership over the given array of expressions
ItemExprPtr CUtils::PexprConjDisj(const ExprArray &pdrgpexpr, bool fConjunction) {
  auto eboolop = AND_EXPR;
  if (!fConjunction) {
    eboolop = OR_EXPR;
  }

  ExprArray pdrgpexprFinal;

  for (const auto &pexpr : pdrgpexpr) {
    if (FSkippable(pexpr, fConjunction)) {
      // skip current conjunct/disjunct
      continue;
    }

    if (FReducible(pexpr, fConjunction)) {
      // a False (True) conjunct (disjunct) yields the whole conjunction (disjunction) False (True)

      return CUtils::PexprScalarConstBool(!fConjunction);
    }

    // add conjunct/disjunct to result array
    pdrgpexprFinal.emplace_back(pexpr);
  }

  // assemble result
  ItemExprPtr pexpr_result = nullptr;
  if (0 < pdrgpexprFinal.size()) {
    if (1 == pdrgpexprFinal.size()) {
      pexpr_result = pdrgpexprFinal[0];

      return pexpr_result;
    }

    auto bool_op = std::make_shared<ItemBoolExpr>(eboolop);
    bool_op->children = (pdrgpexprFinal);
    return bool_op;
  }

  return CUtils::PexprScalarConstBool(fConjunction);
}

ItemExprPtr CUtils::PexprConjunction(const ExprArray &pdrgpexpr) {
  return PexprConjDisj(pdrgpexpr, true);
}

// create a conjunction/disjunction of two components; Does *not* take ownership over given expressions
ItemExprPtr CUtils::PexprConjDisj(const ItemExprPtr &pexprOne, const ItemExprPtr &pexprTwo, bool fConjunction) {
  if (pexprOne == nullptr)
    return pexprTwo;
  if (pexprTwo == nullptr)
    return pexprOne;

  if (pexprOne == pexprTwo) {
    return pexprOne;
  }

  auto pdrgpexpr = PdrgpexprConjuncts(pexprOne);
  auto pdrgpexpr_two = PdrgpexprConjuncts(pexprTwo);

  pdrgpexpr.insert(pdrgpexpr.begin(), pdrgpexpr_two.begin(), pdrgpexpr_two.end());

  return PexprConjDisj(pdrgpexpr, fConjunction);
}

// create a conjunction of two components;
ItemExprPtr CUtils::PexprConjunction(const ItemExprPtr &pexprOne, const ItemExprPtr &pexprTwo) {
  return PexprConjDisj(pexprOne, pexprTwo, true);
}

// is the given expression in the form (expr Is NOT DISTINCT FROM expr)
bool CUtils::FINDF(const ItemExprPtr &pexpr) {
  if (FNot(pexpr)) {
    return pexpr->GetChild(0)->kind == ExpressionKind::EopScalarIsDistinctFrom;
  }
  return false;
}

}  // namespace pgp