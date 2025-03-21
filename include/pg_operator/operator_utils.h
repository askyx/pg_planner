#pragma once

#include "pg_operator/item_expr.h"
#include "pg_operator/operator_node.h"
#include "pg_optimizer/colref.h"

namespace pgp {

class Memo;
class LogicalOperator;
class LogicalGbAgg;

class OperatorUtils {
 public:
  // generate a comparison expression for an expression and a column reference
  static ItemExprPtr PexprScalarCmp(const ItemExprPtr &pexpr_left, ColRef *pcr_right, Oid mdid_op);

  // if predicate is True return logical expression, otherwise return a new select node
  static OperatorNodePtr PexprSafeSelect(OperatorNodePtr pexpr_logical, const ItemExprPtr &pexpr_predicate);

  // generate a bool expression
  static ItemExprPtr PexprScalarConstBool(bool value, bool is_null = false);

  // check to see if the expression is a scalar const TRUE
  static bool FScalarConstTrue(const ItemExprPtr &pexpr);

  // check to see if the expression is a scalar const FALSE
  static bool FScalarConstFalse(const ItemExprPtr &pexpr);

  // is the given expression a scalar bool op of the passed type?
  static bool FScalarBoolOp(const ItemExprPtr &pexpr, BoolExprType eboolop);

  // is the given expression in the form (expr IS NOT DISTINCT FROM expr)
  static bool FINDF(const ItemExprPtr &pexpr);

  // is the given expression an AND
  static bool FAnd(const ItemExprPtr &pexpr) { return OperatorUtils::FScalarBoolOp(pexpr, AND_EXPR); }

  // is the given expression an OR
  static bool FOr(const ItemExprPtr &pexpr) { return OperatorUtils::FScalarBoolOp(pexpr, OR_EXPR); }

  // does the given expression have any NOT children?
  // is the given expression a NOT
  static bool FNot(const ItemExprPtr &pexpr) { return OperatorUtils::FScalarBoolOp(pexpr, NOT_EXPR); }

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
