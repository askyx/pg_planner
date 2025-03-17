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
ItemExprPtr OperatorUtils::PexprScalarCmp(const ItemExprPtr &pexpr_left, ColRef *pcr_right, Oid mdid_op) {
  auto scalar_op = std::make_shared<ItemCmpExpr>(mdid_op);
  scalar_op->AddChild(pexpr_left);
  scalar_op->AddChild(std::make_shared<ItemIdent>(pcr_right));
  return scalar_op;
}

// generate a boolean scalar constant expression
ItemExprPtr OperatorUtils::PexprScalarConstBool(bool value, bool is_null) {
  // create a bool datum
  auto *datum = makeBoolConst(value, is_null);

  return std::make_shared<ItemConst>((Const *)datum);
}

// if predicate is True return logical expression, otherwise return a new select node
OperatorNode *OperatorUtils::PexprSafeSelect(OperatorNode *pexpr_logical, const ItemExprPtr &pexpr_pred) {
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
bool OperatorUtils::FScalarConstTrue(const ItemExprPtr &pexpr) {
  return FScalarConstBool(pexpr, true);
}

// checks to see if the expression is a scalar const FALSE
bool OperatorUtils::FScalarConstFalse(const ItemExprPtr &pexpr) {
  return FScalarConstBool(pexpr, false);
}

// is the given expression a scalar bool op of the passed type?
bool OperatorUtils::FScalarBoolOp(const ItemExprPtr &pexpr, BoolExprType eboolop) {
  return ExpressionKind::EopScalarBoolOp == pexpr->kind && eboolop == pexpr->Cast<ItemBoolExpr>().boolop;
}

// recursively collect conjuncts
void CollectConjuncts(const ItemExprPtr &pexpr, ExprArray &pdrgpexpr) {
  if (OperatorUtils::FAnd(pexpr)) {
    for (const auto &arg : pexpr->GetChildren())
      CollectConjuncts(arg, pdrgpexpr);
  } else {
    pdrgpexpr.emplace_back(pexpr);
  }
}

// extract conjuncts from a predicate
ExprArray OperatorUtils::PdrgpexprConjuncts(const ItemExprPtr &pexpr) {
  ExprArray pdrgpexpr;

  CollectConjuncts(pexpr, pdrgpexpr);

  return pdrgpexpr;
}

// check if a conjunct/disjunct can be skipped
bool FSkippable(const ItemExprPtr &pexpr, bool f_conjunction) {
  return ((f_conjunction && OperatorUtils::FScalarConstTrue(pexpr)) ||
          (!f_conjunction && OperatorUtils::FScalarConstFalse(pexpr)));
}

// check if a conjunction/disjunction can be reduced to a constant
// True/False based on the given conjunct/disjunct
bool FReducible(const ItemExprPtr &pexpr, bool f_conjunction) {
  return ((f_conjunction && OperatorUtils::FScalarConstFalse(pexpr)) ||
          (!f_conjunction && OperatorUtils::FScalarConstTrue(pexpr)));
}

// create conjunction/disjunction from array of components; Takes ownership over the given array of expressions
ItemExprPtr OperatorUtils::PexprConjDisj(const ExprArray &pdrgpexpr, bool f_conjunction) {
  auto eboolop = AND_EXPR;
  if (!f_conjunction) {
    eboolop = OR_EXPR;
  }

  ExprArray pdrgpexpr_final;

  for (const auto &pexpr : pdrgpexpr) {
    if (FSkippable(pexpr, f_conjunction)) {
      // skip current conjunct/disjunct
      continue;
    }

    if (FReducible(pexpr, f_conjunction)) {
      // a False (True) conjunct (disjunct) yields the whole conjunction (disjunction) False (True)

      return OperatorUtils::PexprScalarConstBool(!f_conjunction);
    }

    // add conjunct/disjunct to result array
    pdrgpexpr_final.emplace_back(pexpr);
  }

  // assemble result
  ItemExprPtr pexpr_result = nullptr;
  if (0 < pdrgpexpr_final.size()) {
    if (1 == pdrgpexpr_final.size()) {
      pexpr_result = pdrgpexpr_final[0];

      return pexpr_result;
    }

    auto bool_op = std::make_shared<ItemBoolExpr>(eboolop);
    bool_op->children = (pdrgpexpr_final);
    return bool_op;
  }

  return OperatorUtils::PexprScalarConstBool(f_conjunction);
}

ItemExprPtr OperatorUtils::PexprConjunction(const ExprArray &pdrgpexpr) {
  return PexprConjDisj(pdrgpexpr, true);
}

// create a conjunction/disjunction of two components; Does *not* take ownership over given expressions
ItemExprPtr OperatorUtils::PexprConjDisj(const ItemExprPtr &pexpr_one, const ItemExprPtr &pexpr_two,
                                         bool f_conjunction) {
  if (pexpr_one == nullptr)
    return pexpr_two;
  if (pexpr_two == nullptr)
    return pexpr_one;

  if (pexpr_one == pexpr_two) {
    return pexpr_one;
  }

  auto pdrgpexpr = PdrgpexprConjuncts(pexpr_one);
  auto pdrgpexpr_two = PdrgpexprConjuncts(pexpr_two);

  pdrgpexpr.insert(pdrgpexpr.begin(), pdrgpexpr_two.begin(), pdrgpexpr_two.end());

  return PexprConjDisj(pdrgpexpr, f_conjunction);
}

// create a conjunction of two components;
ItemExprPtr OperatorUtils::PexprConjunction(const ItemExprPtr &pexpr_one, const ItemExprPtr &pexpr_two) {
  return PexprConjDisj(pexpr_one, pexpr_two, true);
}

// is the given expression in the form (expr Is NOT DISTINCT FROM expr)
bool OperatorUtils::FINDF(const ItemExprPtr &pexpr) {
  if (FNot(pexpr)) {
    return pexpr->GetChild(0)->kind == ExpressionKind::EopScalarIsDistinctFrom;
  }
  return false;
}

}  // namespace pgp