#pragma once

#include "catalog/pg_type_d.h"
#include "pg_operator/item_expr.h"
#include "pg_operator/operator_node.h"
#include "pg_optimizer/colref.h"
extern "C" {
#include "nodes/primnodes.h"
}
namespace pgp {

class Memo;
class LogicalOperator;
class LogicalGbAgg;

template <BoolExprType T>
inline bool BoolExprTypeIsX(const ItemExprPtr &expr) {
  if (expr->NodeIs<ItemBoolExpr>()) {
    return expr->Cast<ItemBoolExpr>().boolop == T;
  }
  return false;
}

inline bool IsNotExpr(const ItemExprPtr &expr) {
  return BoolExprTypeIsX<NOT_EXPR>(expr);
}

inline bool IsOrExpr(const ItemExprPtr &expr) {
  return BoolExprTypeIsX<OR_EXPR>(expr);
}

inline bool IsAndExpr(const ItemExprPtr &expr) {
  return BoolExprTypeIsX<AND_EXPR>(expr);
}

template <bool V>
inline bool BoolConstIsX(const ItemExprPtr &expr) {
  if (expr->NodeIs<ItemConst>()) {
    const auto &value = expr->Cast<ItemConst>();
    return value.value->consttype == BOOLOID && !value.value->constisnull && value.value->constvalue == V;
  }
  return false;
}

inline bool ConstIsTrue(const ItemExprPtr &expr) {
  return BoolConstIsX<true>(expr);
}

inline bool ConstIsFalse(const ItemExprPtr &expr) {
  return BoolConstIsX<false>(expr);
}

class OperatorUtils {
 public:
  // generate a comparison expression for an expression and a column reference
  static ItemExprPtr PexprScalarCmp(const ItemExprPtr &pexpr_left, ColRef *pcr_right, Oid mdid_op);

  // if predicate is True return logical expression, otherwise return a new select node
  static OperatorNodePtr PexprSafeSelect(OperatorNodePtr pexpr_logical, const ItemExprPtr &pexpr_predicate);

  // generate a bool expression
  static ItemExprPtr PexprScalarConstBool(bool value, bool is_null = false);

  // is the given expression in the form (expr IS NOT DISTINCT FROM expr)
  static bool FINDF(const ItemExprPtr &pexpr);

  // extract conjuncts from a scalar tree
  static ExprArray PdrgpexprConjuncts(const ItemExprPtr &pexpr);

  // create conjunction/disjunction
  static ItemExprPtr PexprConjDisj(const ExprArray &pdrgpexpr, bool f_conjunction);

  // create conjunction/disjunction of two expressions
  static ItemExprPtr PexprConjDisj(const ItemExprPtr &pexpr_one, const ItemExprPtr &pexpr_two, bool f_conjunction);

  // create conjunction
  static ItemExprPtr PexprConjunction(const ExprArray &pdrgpexpr);

  // create conjunction of two expressions
  static ItemExprPtr PexprConjunction(const ItemExprPtr &pexpr_one, const ItemExprPtr &pexpr_two);
};

}  // namespace pgp
