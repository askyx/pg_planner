# Subquery


SELECT (1, 'a') = (SELECT 1, 'a')

SELECT 1 IN (SELECT 1)

SELECT (1, 'a') IN (SELECT 1, 'a')


$10 = SubPlan {subLinkType: ANY_SUBLINK, plan_id: 1, plan_name: 'SubPlan 1', firstColType: 16, firstColTypmod: -1, firstColCollation: 0, useHashTable: true, unknownEqFalse: false, parallel_safe: true, startup_cost: 40, per_call_cost: 0.0025000000000000001} = {
  testexpr = OpExpr {opno: 91, opfuncid: 60, opresulttype: 16, opretset: false, opcollid: 0, inputcollid: 0} = {
    args = List with 2 elements = {
      0 = Var {varno: -2, varattno: 3, vartype: 16, vartypmod: -1, varcollid: 0, varlevelsup: 0, varnosyn: 1, varattnosyn: 3},
      1 = Param {paramkind: PARAM_EXEC, paramid: 0, paramtype: 16, paramtypmod: -1, paramcollid: 0}
    }
  },
  paramIds = IntList with 1 elements = {0}
}


```c++
	JOIN_INNER,					/* matching tuple pairs only */
	JOIN_LEFT,					/* pairs + unmatched LHS tuples */
	JOIN_FULL,					/* pairs + unmatched LHS + unmatched RHS */
	JOIN_RIGHT,					/* pairs + unmatched RHS tuples */

	/*
	 * Semijoins and anti-semijoins (as defined in relational theory) do not
	 * appear in the SQL JOIN syntax, but there are standard idioms for
	 * representing them (e.g., using EXISTS).  The planner recognizes these
	 * cases and converts them to joins.  So the planner and executor must
	 * support these codes.  NOTE: in JOIN_SEMI output, it is unspecified
	 * which matching RHS row is joined to.  In JOIN_ANTI output, the row is
	 * guaranteed to be null-extended.
	 */
	JOIN_SEMI,					/* 1 copy of each LHS row that has match(es) */
	JOIN_ANTI,					/* 1 copy of each LHS row that has no match */
	JOIN_RIGHT_ANTI,			/* 1 copy of each RHS row that has no match */

	/*
	 * These codes are used internally in the planner, but are not supported
	 * by the executor (nor, indeed, by most of the planner).
	 */
	JOIN_UNIQUE_OUTER,			/* LHS path must be made unique */
	JOIN_UNIQUE_INNER,			/* RHS path must be made unique */


typedef enum SubLinkType
{
	EXISTS_SUBLINK,                                           
	ALL_SUBLINK,                                              
	ANY_SUBLINK,                                              
	ROWCOMPARE_SUBLINK,
	EXPR_SUBLINK,                                             
	MULTIEXPR_SUBLINK,
	ARRAY_SUBLINK,            array(subquery)                
	CTE_SUBLINK,				/* for SubPlans only */
} SubLinkType;


```

expr [not] in (subquery)
(filter expr [not] in subquery child)
(filter expr [not] = any(subquery_out)
  (apply semi
    child
    subquery
  )
)

(filter [not] exists (subquery) child)
(apply semi if not anti 
  child
  subquery
)


in --> x = any (y)
not in --> NOT (x = any (y))
some --> x = any (y)
any --> x = any (y)
all --> x = all (y)
exists
not exists

apply
  JOIN_INNER                ->      scalar subquery
  JOIN_LEFT                 ->      scalar subquery
  JOIN_FULL
  JOIN_RIGHT
  JOIN_SEMI
  JOIN_ANTI
  JOIN_RIGHT_ANTI
  JOIN_UNIQUE_OUTER
  JOIN_UNIQUE_INNER


https://dl.gi.de/server/api/core/bitstreams/535a5d94-043d-4b1a-9062-fbaf8ed35468/content
  * dependent join，mark join， single join

