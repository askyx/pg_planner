#include "main/from_pg_query.h"

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/defer.h"
#include "common/exception.h"
#include "pg_operator/item_expr.h"
#include "pg_operator/logical_operator.h"
#include "pg_operator/operator.h"
#include "pg_operator/operator_node.h"
#include "pg_operator/operator_utils.h"
#include "pg_optimizer/colref.h"
#include "pg_optimizer/properties.h"

extern "C" {
#include <c.h>
#include <postgres.h>

#include <access/attnum.h>
#include <access/sysattr.h>
#include <catalog/heap.h>
#include <catalog/pg_class.h>
#include <catalog/pg_proc.h>
#include <catalog/pg_type_d.h>
#include <commands/defrem.h>
#include <funcapi.h>
#include <nodes/bitmapset.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/nodes.h>
#include <nodes/parsenodes.h>
#include <nodes/pathnodes.h>
#include <nodes/pg_list.h>
#include <nodes/plannodes.h>
#include <nodes/primnodes.h>
#include <parser/parsetree.h>
#include <tcop/tcopprot.h>
#include <utils/datum.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
}

namespace pgp {

OperatorNodePtr TranslatorQuery::TranslateQuery() {
  // TODO: other commands
  switch (query_->commandType) {
    case CMD_SELECT:
      return TranslateSelect();

    default:
      return nullptr;
  }
}

static bool IsSortGrouColumn(const TargetEntry *target_entry, List *group_clause) {
  foreach_node(SortGroupClause, group_clause_node, group_clause) {
    if (target_entry->ressortgroupref == group_clause_node->tleSortGroupRef)
      return true;
  }

  return false;
}

OperatorNodePtr TranslatorQuery::TranslateSelect() {
  std::unordered_map<Index, ColRef *> sort_group_attno_to_colid_mapping;

  if (nullptr != query_->setOperations)
    throw OptException("TODO setOperations");

  // from and where
  auto root = TranslateNode(query_->jointree);

  ExprArray project_exprs;

  // 1. agg 作为 project 元素保存在 group 中
  // 2. 非平凡var需要计算colref，保存在projectnode 中
  // 3. 有 aggnode 则保存 project 早agg中，没有则保存在projectnode中，后面尝试下推
  ColRefArray grouping_cols;
  foreach_node(TargetEntry, target_entry, query_->targetList) {
    auto project_elem = TranslateExprToProject(target_entry->expr, root, target_entry->resname);
    auto *colref = project_elem->Cast<ItemProjectElement>().colref;
    col_ref_.emplace_back(colref);

    if (IsSortGrouColumn(target_entry, query_->groupClause))
      grouping_cols.emplace_back(colref);

    if (target_entry->ressortgroupref > 0)
      sort_group_attno_to_colid_mapping[target_entry->ressortgroupref] = colref;

    if (!IsA(target_entry->expr, Var))
      project_exprs.emplace_back(project_elem);
  }

  if (query_->hasAggs)
    root = MakeOperatorNode(std::make_shared<LogicalGbAgg>(grouping_cols, project_exprs), {std::move(root)});
  else if (!project_exprs.empty())
    root = MakeOperatorNode(std::make_shared<LogicalProject>(project_exprs), {std::move(root)});

  std::shared_ptr<OrderSpec> pos = nullptr;

  if (query_->sortClause != nullptr) {
    pos = std::make_shared<OrderSpec>();
    foreach_node(SortGroupClause, sort_group_clause, query_->sortClause) {
      // get the colid of the sorting column
      auto *colref = sort_group_attno_to_colid_mapping[sort_group_clause->tleSortGroupRef];

      pos->AddSortElement({sort_group_clause->sortop, colref,
                           sort_group_clause->nulls_first ? NullsOrder::EnullsFirst : NullsOrder::EnullsLast});
    }
    property_set_->AddProperty(std::make_shared<PropertySort>(pos));
  }

  // TODO: check if limit 0, retrurn empty result set at once
  if ((query_->limitCount != nullptr) || (query_->limitOffset != nullptr)) {
    auto limit = TranslateExpr((Expr *)query_->limitCount, root);
    auto offset = TranslateExpr((Expr *)query_->limitOffset, root);
    root = MakeOperatorNode(std::make_shared<LogicalLimit>(pos, limit, offset), {std::move(root)});
  }

  return root;
}

OperatorNodePtr TranslatorQuery::TranslateNode(Node *node) {
  switch (nodeTag(node)) {
    case T_FromExpr:
      return TranslateNode((FromExpr *)node);

    case T_RangeTblRef:
      return TranslateNode((RangeTblRef *)node);

    case T_JoinExpr:
      return TranslateNode((JoinExpr *)node);

    default:
      throw OptException("TODO");
  }
  return nullptr;
}

OperatorNodePtr TranslatorQuery::TranslateNode(FromExpr *from_expr) {
  OperatorNodePtr root = nullptr;

  if (0 == list_length(from_expr->fromlist))
    throw OptException("TODO: empty fromlist");

  if (1 == list_length(from_expr->fromlist))
    root = TranslateNode(linitial_node(Node, from_expr->fromlist));
  else {
    auto outer = TranslateNode(linitial_node(Node, from_expr->fromlist));
    auto inner = TranslateNode(lsecond_node(Node, from_expr->fromlist));
    root = MakeOperatorNode(std::make_shared<LogicalJoin>(JOIN_INNER, OperatorUtils::PexprScalarConstBool(true)),
                            {outer, inner});

    ListCell *arg;
    for_each_from(arg, from_expr->fromlist, 2) {
      root = MakeOperatorNode(std::make_shared<LogicalJoin>(JOIN_INNER, OperatorUtils::PexprScalarConstBool(true)),
                              {root, TranslateNode(lfirst_node(Node, arg))});
    }
  }

  auto condition_node = TranslateExpr((Expr *)from_expr->quals, root);

  if (1 >= list_length(from_expr->fromlist)) {
    if (nullptr != condition_node) {
      root = MakeOperatorNode(std::make_shared<LogicalFilter>(condition_node), {root});
    }
  } else {
    if (nullptr == condition_node)
      condition_node = OperatorUtils::PexprScalarConstBool(true);

    auto cnode = root;
    while (cnode->content->kind == OperatorType::LogicalApply &&
           (cnode->Cast<LogicalApply>().subquery_type == ANY_SUBLINK ||
            cnode->Cast<LogicalApply>().subquery_type == ALL_SUBLINK)) {
      cnode = cnode->GetChild(0);
    }
    if (cnode->content->kind == OperatorType::LogicalApply) {
      auto &apply = cnode->Cast<LogicalApply>();
      apply.filter = condition_node;
    } else {
      auto &join = cnode->Cast<LogicalJoin>();
      join.filter = condition_node;
    }
  }

  return root;
}

OperatorNodePtr TranslatorQuery::TranslateNode(RangeTblRef *node) {
  auto rt_index = node->rtindex;

  auto *rte = rt_fetch(rt_index, query_->rtable);

  if (rte->lateral)
    throw OptException("TODO: LATERAL");

  switch (rte->rtekind) {
    case RTE_RELATION: {
      if (!rte->inh) {
        throw OptException("ONLY in the FROM clause");
      }

      auto *rel = RelationIdGetRelation(rte->relid);
      auto rel_close = pgp::ScopedDefer{[] {}, [&]() { RelationClose(rel); }};

      ColRefArray output_columns;

      for (int i = 0; i < RelationGetNumberOfAttributes(rel); i++) {
        Form_pg_attribute att = TupleDescAttr(rel->rd_att, i);
        auto item_width = get_attavgwidth(RelationGetRelid(rel), (AttrNumber)i);
        if (item_width <= 0) {
          item_width = get_typavgwidth(att->atttypid, att->atttypmod);
        }
        ColRef *colref = optimizer_context_->GetColumnFactory().PcrCreate(att->atttypid, att->atttypmod, false, true,
                                                                          NameStr(att->attname), item_width);
        output_columns.emplace_back(colref);
        var_to_colid_map_.Insert(query_level_, rt_index, att->attnum, colref);
        colref->attnum = att->attnum;
      }

      return MakeOperatorNode(std::make_shared<LogicalGet>(rte, output_columns));
    }

    case RTE_SUBQUERY: {
      Query *query_derived_tbl = rte->subquery;

      TranslatorQuery transformer(optimizer_context_, var_to_colid_map_, query_derived_tbl, query_level_ + 1);

      OperatorNodePtr subquery = transformer.TranslateSelect();

      uint32_t counter = 0;
      auto outputs = transformer.GetQueryOutputCols();
      foreach_node(TargetEntry, target_entry, transformer.query_->targetList) {
        if (!target_entry->resjunk) {
          auto *colref = outputs[counter];
          var_to_colid_map_.Insert(query_level_, rt_index, int32_t(target_entry->resno), colref);
          counter++;
        }
      }
      return subquery;
    }

    case RTE_JOIN:
    case RTE_TABLEFUNC:
    default:
      throw OptException("Unrecognized RTE kind");
  }

  return nullptr;
}

OperatorNodePtr TranslatorQuery::TranslateNode(JoinExpr *join_expr) {
  OperatorNodePtr left_child = TranslateNode(join_expr->larg);
  OperatorNodePtr right_child = TranslateNode(join_expr->rarg);
  OperatorNodePtr root;

  // TODO: subquery in join condition
  std::shared_ptr<LogicalJoin> join_node;
  OperatorNodeArray join_children;
  if (join_expr->jointype == JOIN_RIGHT) {
    join_node = std::make_shared<LogicalJoin>(JOIN_LEFT, nullptr);
    join_children = {right_child, left_child};
  } else {
    join_node = std::make_shared<LogicalJoin>(join_expr->jointype, nullptr);
    join_children = {left_child, right_child};
  }
  root = MakeOperatorNode(join_node, join_children);

  if (nullptr != join_expr->quals)
    join_node->filter = TranslateExpr((Expr *)join_expr->quals, root);
  else
    join_node->filter = OperatorUtils::PexprScalarConstBool(true);

  auto rtindex = join_expr->rtindex;
  auto *rte = rt_fetch(rtindex, query_->rtable);

  ExprArray project_exprs;

  ListCell *lc_node = nullptr;
  ListCell *lc_col_name = nullptr;
  int32_t i = 0;
  forboth(lc_node, rte->joinaliasvars, lc_col_name, rte->eref->colnames) {
    Node *join_alias_node = (Node *)lfirst(lc_node);

    auto project_elem = TranslateExprToProject((Expr *)join_alias_node, root, strVal(lfirst(lc_col_name)));
    auto *proj_colref = project_elem->Cast<ItemProjectElement>().colref;
    var_to_colid_map_.Insert(query_level_, rtindex, ++i, proj_colref);

    if (!IsA(join_alias_node, Var))
      project_exprs.emplace_back(project_elem);
  }

  if (project_exprs.empty()) {
    return root;
  }

  return MakeOperatorNode(std::make_shared<LogicalProject>(project_exprs), {root});
}

ItemExprPtr TranslatorQuery::TranslateExprToProject(Expr *expr, OperatorNodePtr &root, const char *alias_name) {
  auto expr_node = TranslateExpr(expr, root);

  ColRef *colref;

  if (IsA(expr, Var))
    colref = expr_node->Cast<ItemIdent>().colref;
  else {
    if (alias_name == nullptr)
      alias_name = "?column?";

    colref = optimizer_context_->GetColumnFactory().PcrCreate(expr_node->ExprReturnType(), -1, alias_name);
  }

  auto project = std::make_shared<ItemProjectElement>(colref);
  project->AddChild(expr_node);

  return project;
}

ItemExprPtr TranslatorQuery::TranslateExpr(const Expr *expr, OperatorNodePtr &root) {
  if (expr == nullptr)
    return nullptr;

  switch (nodeTag(expr)) {
    default:
      throw OptException("TODO");

    case T_Param: {
      auto *param = castNode(Param, expr);

      return ItemParam::FromPg(param);
    }
    case T_Var: {
      auto *var = castNode(Var, expr);

      if (var->varattno == 0)
        throw OptException("Whole-row variable");

      auto *colref = var_to_colid_map_.GetColRefByVar(query_level_, var);

      if (colref == nullptr)
        throw OptException("Unknown variable");

      return std::make_shared<ItemIdent>(colref);
    }
    case T_OpExpr: {
      auto *op_expr = castNode(OpExpr, expr);

      auto op = std::make_shared<ItemOpExpr>(op_expr->opno, op_expr->opresulttype);

      foreach_node(Expr, expr, op_expr->args) op->AddChild(TranslateExpr(expr, root));

      return op;
    }
    case T_DistinctExpr: {
      auto *distinct_expr = castNode(DistinctExpr, expr);

      auto distinct_node = std::make_shared<ItemIsDistinctFrom>(distinct_expr->opno);

      foreach_node(Expr, expr, distinct_expr->args) distinct_node->AddChild(TranslateExpr(expr, root));

      return distinct_node;
    }
    case T_ScalarArrayOpExpr: {
      auto *scalar_array_op_expr = castNode(ScalarArrayOpExpr, expr);

      auto array_node = std::make_shared<ItemArrayOpExpr>(scalar_array_op_expr->opno, scalar_array_op_expr->opfuncid,
                                                          scalar_array_op_expr->useOr);

      foreach_node(Expr, expr, scalar_array_op_expr->args) array_node->AddChild(TranslateExpr(expr, root));

      return array_node;
    }
    case T_Const: {
      return std::make_shared<ItemConst>((Const *)copyObject(expr));
    }
    case T_BoolExpr: {
      auto *bool_expr = castNode(BoolExpr, expr);

      if (bool_expr->boolop == NOT_EXPR) {
        if (IsA(linitial_node(Node, bool_expr->args), SubLink)) {
          auto *sublink = linitial_node(SubLink, bool_expr->args);
          return TranslateNode(sublink, root, true);
        }
      }

      auto bool_op = std::make_shared<ItemBoolExpr>(bool_expr->boolop);
      foreach_node(Expr, expr, bool_expr->args) bool_op->AddChild(TranslateExpr(expr, root));

      return bool_op;
    }

    case T_CaseExpr: {
      auto *case_expr = castNode(CaseExpr, expr);

      auto case_node = ItemCaseExpr::FromPg(case_expr);

      if (case_expr->arg != nullptr)
        case_node->AddChild(TranslateExpr(case_expr->arg, root));

      foreach_node(CaseWhen, expr, case_expr->args) {
        case_node->AddChild(TranslateExpr(expr->expr, root));
        case_node->AddChild(TranslateExpr(expr->result, root));
      }

      if (nullptr != case_expr->defresult)
        case_node->AddChild(TranslateExpr(case_expr->defresult, root));

      return case_node;
    }
    case T_CaseTestExpr: {
      auto *case_test_expr = castNode(CaseTestExpr, expr);

      return std::make_shared<ItemCaseTest>(case_test_expr->typeId, case_test_expr->typeMod, case_test_expr->collation);
    }
    case T_CoalesceExpr: {
      auto *coalesce_expr = castNode(CoalesceExpr, expr);

      auto coalesce_node = std::make_shared<ItemCoalesce>(coalesce_expr->coalescetype);
      foreach_node(Expr, expr, coalesce_expr->args) coalesce_node->AddChild(TranslateExpr(expr, root));

      return coalesce_node;
    }

    case T_FuncExpr: {
      auto *func_expr = castNode(FuncExpr, expr);

      auto func_node =
          std::make_shared<ItemFuncExpr>(func_expr->funcid, func_expr->funcresulttype, func_expr->funcvariadic);

      foreach_node(Expr, expr, func_expr->args) func_node->AddChild(TranslateExpr(expr, root));

      return func_node;
    }
    case T_Aggref: {
      auto *aggref = castNode(Aggref, expr);

      if (aggref->aggfilter != nullptr) {
        throw OptException("Aggregate functions with FILTER");
      }

      auto aggref_scalar =
          std::make_shared<ItemAggref>(aggref->aggfnoid, aggref->aggtype, aggref->aggdistinct != nullptr,
                                       aggref->aggkind, copyObject(aggref->aggargtypes));

      std::vector<int> indexes(list_length(aggref->args) + 1, -1);
      int i = 0;
      foreach_node(TargetEntry, tle, aggref->args) {
        i++;
        aggref_scalar->AddChild(TranslateExpr(tle->expr, root));

        if (tle->ressortgroupref != 0) {
          indexes[tle->ressortgroupref] = i;
        }
      }

      ExprArray aggdirectargs;
      foreach_node(Expr, expr, aggref->aggdirectargs) aggdirectargs.emplace_back(TranslateExpr(expr, root));
      aggref_scalar->aggdirectargs = std::move(aggdirectargs);

      ExprArray aggorder;
      foreach_node(SortGroupClause, expr, aggref->aggorder) {
        expr->tleSortGroupRef = indexes[expr->tleSortGroupRef];
        aggorder.emplace_back(TranslateExpr((Expr *)expr, root));
      }
      aggref_scalar->aggorder = std::move(aggorder);

      ExprArray aggdistinct;
      foreach_node(SortGroupClause, expr, aggref->aggdistinct) {
        expr->tleSortGroupRef = indexes[expr->tleSortGroupRef];
        aggdistinct.emplace_back(TranslateExpr((Expr *)expr, root));
      }
      aggref_scalar->aggdistinct = std::move(aggdistinct);

      return aggref_scalar;
    }

    case T_NullTest: {
      auto *null_test = castNode(NullTest, expr);

      auto null_testx = std::make_shared<ItemNullTest>();
      null_testx->AddChild(TranslateExpr(null_test->arg, root));

      return null_testx;
    }

    case T_RelabelType: {
      auto *relabel_type = castNode(RelabelType, expr);

      auto cast = std::make_shared<ItemCastExpr>(relabel_type->resulttype, 0);
      cast->AddChild(TranslateExpr(relabel_type->arg, root));

      return cast;
    }
    case T_CoerceToDomain: {
      auto *coerce = castNode(CoerceToDomain, expr);

      auto cast =
          std::make_shared<ItemCoerceToDomain>(coerce->resulttype, coerce->resulttypmod, coerce->coercionformat);
      cast->AddChild(TranslateExpr(coerce->arg, root));

      return cast;
    }
    case T_CoerceViaIO: {
      auto *coerce = castNode(CoerceViaIO, expr);

      auto cast = std::make_shared<ItemCoerceViaIO>(coerce->resulttype, -1, coerce->coerceformat);
      cast->AddChild(TranslateExpr(coerce->arg, root));

      return cast;
    }
    case T_ArrayCoerceExpr: {
      auto *array_coerce_expr = castNode(ArrayCoerceExpr, expr);

      auto cast = std::make_shared<ItemArrayCoerceExpr>(array_coerce_expr->resulttype, array_coerce_expr->resulttypmod,
                                                        array_coerce_expr->coerceformat);

      cast->AddChild(TranslateExpr(array_coerce_expr->arg, root));
      cast->AddChild(TranslateExpr(array_coerce_expr->elemexpr, root));

      return cast;
    }

    case T_ArrayExpr: {
      auto *parrayexpr = castNode(ArrayExpr, expr);

      auto pop_array =
          std::make_shared<ItemArrayExpr>(parrayexpr->element_typeid, parrayexpr->array_typeid, parrayexpr->multidims);

      foreach_node(Expr, expr, parrayexpr->elements) pop_array->AddChild(TranslateExpr(expr, root));

      return pop_array;
    }

    case T_SortGroupClause: {
      auto *sgc = castNode(SortGroupClause, expr);

      return std::make_shared<ItemSortGroupClause>((SortGroupClause *)copyObject(sgc));
    }
    case T_SubLink: {
      auto *sublink = castNode(SubLink, expr);

      return TranslateNode(sublink, root, false);
    }
  }
}

ItemExprPtr TranslatorQuery::TranslateNode(SubLink *sublink, OperatorNodePtr &root, bool under_not) {
  TranslatorQuery query_translator{optimizer_context_, var_to_colid_map_, (Query *)sublink->subselect,
                                   query_level_ + 1};

  OperatorNodePtr subquery_node = query_translator.TranslateSelect();
  auto output_array = query_translator.GetQueryOutputCols();

  // TODO: more subquery types
  switch (sublink->subLinkType) {
    case EXPR_SUBLINK: {
      auto *colref = output_array[0];

      auto apply =
          std::make_shared<LogicalApply>(output_array, EXPR_SUBLINK, pgp::OperatorUtils::PexprScalarConstBool(true));
      root = MakeOperatorNode(apply, {std::move(root), subquery_node});

      return std::make_shared<ItemIdent>(colref);
    }

    case ALL_SUBLINK:
    case ANY_SUBLINK: {
      if (1 != output_array.size())
        throw OptException("Non-Scalar Subquery");

      auto *op_expr = (OpExpr *)sublink->testexpr;

      Expr *lhs_expr = (Expr *)list_nth(op_expr->args, 0);

      auto pexpr_predicate =
          OperatorUtils::PexprScalarCmp(TranslateExpr(lhs_expr, root), output_array[0], op_expr->opno);

      auto apply = std::make_shared<LogicalApply>(output_array, sublink->subLinkType, pexpr_predicate);

      apply->is_not_subquery = under_not;

      root = MakeOperatorNode(apply, {std::move(root), subquery_node});

      return OperatorUtils::PexprScalarConstBool(true);
    }

    case EXISTS_SUBLINK: {
      ColRefArray colrefs = {output_array.front()};
      auto apply =
          std::make_shared<LogicalApply>(colrefs, EXISTS_SUBLINK, pgp::OperatorUtils::PexprScalarConstBool(true));
      apply->is_not_subquery = under_not;
      root = MakeOperatorNode(apply, {std::move(root), subquery_node});

      return OperatorUtils::PexprScalarConstBool(true);
    }

    default: {
      throw OptException("Non-Scalar Subquery");
    }
  }

  return nullptr;
}

}  // namespace pgp