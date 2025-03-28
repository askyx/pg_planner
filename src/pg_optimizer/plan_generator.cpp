#include "pg_optimizer/plan_generator.h"

#include <cstdint>
#include <unordered_map>
#include <utility>

#include "common/exception.h"
#include "pg_catalog/catalog.h"
#include "pg_operator/item_expr.h"
#include "pg_operator/operator.h"
#include "pg_operator/physical_operator.h"
#include "pg_optimizer/colref.h"

extern "C" {
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/nodes.h>
#include <nodes/pg_list.h>
#include <nodes/primnodes.h>
#include <optimizer/optimizer.h>
#include <parser/parse_agg.h>
#include <parser/parse_oper.h>
}

namespace pgp {

PlanMeta &PlanMeta::SetPlanStats(GroupExpression *gexpr) {
  plan->startup_cost = 0;
  plan->total_cost = 100;
  plan->plan_width = 8.0;
  plan->plan_rows = 1000.0;
  return *this;
}

PlanMeta &PlanMeta::SetAggGroupInfo(const PhysicalAgg &agg_node) {
  Agg *agg = (Agg *)plan;

  if (auto grouping_cols = agg_node.group_columns; !grouping_cols.empty()) {
    agg->numCols = (int32_t)grouping_cols.size();
    agg->grpColIdx = (AttrNumber *)palloc(agg->numCols * sizeof(AttrNumber));
    agg->grpOperators = (Oid *)palloc(agg->numCols * sizeof(Oid));
    agg->grpCollations = (Oid *)palloc(agg->numCols * sizeof(Oid));

    auto child_target_map = children_metas[0].colid_target_map;
    for (auto [i, group] : std::views::enumerate(grouping_cols)) {
      PGP_ASSERT(child_target_map.contains(group->ref_id), "target entry not found for column reference");
      auto *target_entry = child_target_map[group->ref_id];
      agg->grpColIdx[i] = target_entry->resno;

      // Also find the equality operators to use for each grouping col.
      Oid eq_opr;

      get_sort_group_operators(exprType((Node *)target_entry->expr), false, true, false, nullptr, &eq_opr, nullptr,
                               nullptr);

      agg->grpOperators[i] = eq_opr;
      agg->grpCollations[i] = exprCollation((Node *)target_entry->expr);
    }
  }

  return *this;
}

PlanMeta &PlanMeta::SetSortInfo(const PhysicalSort &sort_node) {
  Sort *sort = (Sort *)plan;

  auto sortspec = sort_node.order_spec;

  sort->numCols = (int)sortspec->SortSize();
  sort->sortColIdx = (AttrNumber *)palloc(sort->numCols * sizeof(AttrNumber));
  sort->sortOperators = (Oid *)palloc(sort->numCols * sizeof(Oid));
  sort->collations = (Oid *)palloc(sort->numCols * sizeof(Oid));
  sort->nullsFirst = (bool *)palloc(sort->numCols * sizeof(bool));

  auto child_target_map = children_metas[0].colid_target_map;
  for (auto [idx, sort_ele] : std::views::enumerate(sortspec->GetSortArray())) {
    PGP_ASSERT(child_target_map.contains(sort_ele.colref->ref_id), "target entry not found for column reference");
    const auto *target = child_target_map[sort_ele.colref->ref_id];
    sort->sortColIdx[idx] = target->resno;
    sort->sortOperators[idx] = sort_ele.sort_op;
    sort->collations[idx] = exprCollation((Node *)target->expr);
    sort->nullsFirst[idx] = (bool)sort_ele.nulls_order;
  }

  return *this;
}

PlanMeta &PlanMeta::GenerateTargetList(const ExprArray &project_list, const ColRefArray &req_cols) {
  List *target_list = NIL;
  std::unordered_map<uint32_t, TargetEntry *> target_map;
  for (const auto &project_node : project_list) {
    auto *colref = project_node->Cast<ItemProjectElement>().colref;
    auto *expr = GenerateExpr(project_node->GetChild(0));

    target_map[colref->ref_id] = GenerateTargetEntry(expr, 0, colref->name, colref->ref_id);
  }

  AttrNumber resno = 1;
  for (auto *colref : req_cols) {
    if (target_map.contains(colref->ref_id)) {
      auto *target_entry = target_map[colref->ref_id];
      target_entry->resno = resno++;
      target_list = lappend(target_list, target_entry);
    } else {
      auto *var = GenerateVarExpr(colref);
      auto *target_entry = GenerateTargetEntry(var, resno++, pstrdup(colref->name.c_str()), colref->ref_id);
      target_list = lappend(target_list, target_entry);
    }
  }
  plan->targetlist = target_list;

  return *this;
}

PlanMeta &PlanMeta::GenerateTargetList(const ColRefArray &req_cols) {
  List *target_list = NIL;
  AttrNumber resno = 1;
  for (auto *colref : req_cols) {
    auto *var = GenerateVarExpr(colref);
    auto *target_entry = GenerateTargetEntry(var, resno++, pstrdup(colref->name.c_str()), colref->ref_id);
    target_list = lappend(target_list, target_entry);
  }
  plan->targetlist = target_list;

  return *this;
}

PlanMeta &PlanMeta::GenerateFilter(const ItemExprPtr &filter_node, bool join_filter) {
  if (filter_node != nullptr) {
    if (join_filter) {
      auto *join = (Join *)plan;
      join->joinqual = list_make1(GenerateExpr(filter_node));
    } else
      plan->qual = list_make1(GenerateExpr(filter_node));
  }

  return *this;
}

PlanMeta &PlanMeta::GenerateResult(int plan_node_id) {
  Result *node = makeNode(Result);
  plan = &node->plan;
  plan->plan_node_id = plan_node_id;
  plan->lefttree = children_metas[0].plan;

  return *this;
}

PlanMeta &PlanMeta::GenerateNestedLoopJoin(int plan_node_id, const PhysicalNLJoin &nljoin_node) {
  NestLoop *nested_loop = makeNode(NestLoop);
  Join *join = &(nested_loop->join);
  plan = &(join->plan);
  plan->plan_node_id = plan_node_id;

  plan->lefttree = children_metas[0].plan;
  plan->righttree = children_metas[1].plan;

  join->jointype = nljoin_node.join_type;

  return *this;
}

PlanMeta &PlanMeta::GenerateLimit(int plan_node_id) {
  Limit *limit = makeNode(Limit);

  plan = &(limit->plan);
  plan->plan_node_id = plan_node_id;
  plan->lefttree = children_metas[0].plan;

  return *this;
}

PlanMeta &PlanMeta::GenerateSort(int plan_node_id) {
  Sort *sort = makeNode(Sort);

  plan = &(sort->plan);
  plan->plan_node_id = plan_node_id;
  plan->lefttree = children_metas[0].plan;

  return *this;
}

PlanMeta &PlanMeta::GenerateSeqScan(int plan_node_id) {
  SeqScan *seq_scan = makeNode(SeqScan);

  seq_scan->scan.scanrelid = range_table_context.rte_index;
  plan = &(seq_scan->scan.plan);
  plan->plan_node_id = plan_node_id;

  return *this;
}

PlanMeta &PlanMeta::GenerateSubplan(const PhysicalApply &apply) {
  generator.generator_context.subplan_entries_list =
      lappend(generator.generator_context.subplan_entries_list, children_metas[1].plan);

  SubPlan *subplan = makeNode(SubPlan);
  subplan->plan_id = list_length(generator.generator_context.subplan_entries_list);
  subplan->plan_name = psprintf("SubPlan %d", subplan->plan_id);
  subplan->firstColType = exprType((Node *)((TargetEntry *)list_nth(children_metas[1].plan->targetlist, 0))->expr);
  subplan->firstColCollation = get_typcollation(subplan->firstColType);
  subplan->firstColTypmod = -1;
  subplan->subLinkType = apply.subquery_type;
  subplan->unknownEqFalse = false;

  auto upper_res_cols_copy = std::move(generator.upper_res_cols);
  // TODO: array subplan
  for (auto *colref : upper_res_cols_copy) {
    PGP_ASSERT(generator.param_id_map.contains(colref->ref_id), "parameter id not found for column reference");
    subplan->parParam = lappend_int(subplan->parParam, generator.param_id_map.at(colref->ref_id));
    subplan->args = lappend(subplan->args, GenerateVarExpr(colref));
  }

  // what if array subplan or multi-column subplan?
  for (auto *colref : apply.expr_refs)
    generator.colid_subplan_map[colref->ref_id] = (Expr *)subplan;

  generator.param_id_map.clear();

  return *this;
}

PlanMeta &PlanMeta::GenerateAgg(int plan_node_id, const PhysicalAgg &agg_node) {
  Agg *agg = makeNode(Agg);

  plan = &(agg->plan);
  plan->plan_node_id = plan_node_id;
  plan->lefttree = children_metas[0].plan;

  // TODO: mixed
  if (agg_node.kind == OperatorType::PhysicalStreamAgg)
    agg->aggstrategy = AGG_SORTED;
  if (agg_node.kind == OperatorType::PhysicalHashAgg)
    agg->aggstrategy = AGG_HASHED;
  if (agg_node.kind == OperatorType::PhysicalScalarAgg)
    agg->aggstrategy = AGG_PLAIN;

  return *this;
}

TargetEntry *PlanMeta::GenerateTargetEntry(Expr *expr, AttrNumber resno, const std::string &col_name, uint32_t colid) {
  auto *target_entry = makeTargetEntry(expr, resno, pstrdup(col_name.c_str()), false);

  if (IsA(expr, Var)) {
    if (range_table_context.init) {
      target_entry->resorigtbl = range_table_context.rel_oid;
      target_entry->resorigcol = ((Var *)expr)->varattno;
    } else {
      for (const auto &child_context : children_metas) {
        if (child_context.colid_target_map.contains(colid)) {
          auto *dependent_target_entry = child_context.colid_target_map.at(colid);
          target_entry->resorigtbl = dependent_target_entry->resorigtbl;
          target_entry->resorigcol = dependent_target_entry->resorigcol;
        }
      }
    }
  }

  colid_target_map.insert({colid, target_entry});
  return target_entry;
}

List *PlanMeta::TranslateScalarChildrenScalar(const ExprArray &pexpr) {
  List *new_list = nullptr;
  for (const auto &expr : pexpr) {
    new_list = lappend(new_list, GenerateExpr(expr));
  }
  return new_list;
}

Expr *PlanMeta::GenerateExpr(const ItemExprPtr &op_node) {
  if (op_node == nullptr)
    return nullptr;

  switch (op_node->kind) {
    case pgp::ExpressionKind::Ident: {
      auto *colref = op_node->Cast<ItemIdent>().colref;
      if (generator.colid_subplan_map.contains(colref->ref_id))
        return generator.colid_subplan_map.at(colref->ref_id);

      return GenerateVarExpr(colref);
    }

    case pgp::ExpressionKind::RelabelType: {
      const auto &pop_sc_cast = op_node->Cast<ItemCastExpr>();

      auto *expr = pop_sc_cast.ToExpr();

      Expr *child_expr = GenerateExpr(op_node->GetChild(0));

      if (pop_sc_cast.funcid != InvalidOid) {
        auto *func_expr = castNode(FuncExpr, expr);
        func_expr->args = lappend(func_expr->args, child_expr);
      } else {
        auto *relabel_type = castNode(RelabelType, expr);
        relabel_type->arg = child_expr;
      }

      return expr;
    }

    case pgp::ExpressionKind::Const: {
      auto *pop_sc_const = op_node->Cast<ItemConst>().value;

      return (Expr *)copyObject(pop_sc_const);
    }

    case pgp::ExpressionKind::FuncExpr: {
      const auto &pop_sc_func = op_node->Cast<ItemFuncExpr>();

      auto *func_expr = pop_sc_func.ToFuncExpr();

      for (const auto &arg : op_node->children)
        func_expr->args = lappend(func_expr->args, GenerateExpr(arg));

      func_expr->inputcollid = exprCollation(linitial_node(Node, func_expr->args));

      return (Expr *)func_expr;
    }

    case pgp::ExpressionKind::OpExpr: {
      const auto &pscop = op_node->Cast<ItemOpExpr>();

      auto *op_expr = pscop.ToOpExpr();

      for (const auto &arg : op_node->children)
        op_expr->args = lappend(op_expr->args, GenerateExpr(arg));

      op_expr->inputcollid = exprCollation(linitial_node(Node, op_expr->args));

      return (Expr *)op_expr;
    }

    case pgp::ExpressionKind::BoolExpr: {
      const auto &pop_sc_bool_op = op_node->Cast<ItemBoolExpr>();

      BoolExpr *scalar_bool_expr = pop_sc_bool_op.ToBoolExpr();

      for (const auto &arg : op_node->children)
        scalar_bool_expr->args = lappend(scalar_bool_expr->args, GenerateExpr(arg));

      return (Expr *)scalar_bool_expr;
    }

    case pgp::ExpressionKind::ScalarArrayOpExpr: {
      const auto &pop = op_node->Cast<ItemArrayOpExpr>();

      ScalarArrayOpExpr *array_op_expr = pop.ToScalarArrayOpExpr();

      Expr *left_expr = GenerateExpr(op_node->GetChild(0));
      Expr *right_expr = GenerateExpr(op_node->GetChild(1));

      array_op_expr->args = list_make2(left_expr, right_expr);
      array_op_expr->inputcollid = exprCollation((Node *)left_expr);

      return (Expr *)array_op_expr;
    }

    case pgp::ExpressionKind::ArrayExpr: {
      const auto &pop = op_node->Cast<ItemArrayExpr>();

      auto *expr = pop.ToArrayExpr();

      for (const auto &arg : op_node->children)
        expr->elements = lappend(expr->elements, GenerateExpr(arg));

      return (Expr *)eval_const_expressions(nullptr, (Node *)expr);
    }

    case pgp::ExpressionKind::CaseExpr: {
      const auto &case_node = op_node->Cast<ItemCaseExpr>();

      auto *case_expr = case_node.ToCaseExpr();

      auto when_then_start = case_node.case_arg_exist ? 1 : 0;
      auto when_then_end = case_node.default_arg_exist ? 1 : 0;

      if (case_node.case_arg_exist)
        case_expr->arg = GenerateExpr(op_node->GetChild(0));

      auto children = op_node->children;
      for (auto i = when_then_start; i < children.size() - when_then_end; i += 2) {
        CaseWhen *case_when = makeNode(CaseWhen);
        case_when->expr = GenerateExpr(children[i]);
        case_when->result = GenerateExpr(children[i + 1]);
        case_expr->args = lappend(case_expr->args, case_when);
      }

      if (case_node.default_arg_exist)
        case_expr->defresult = GenerateExpr(children.back());

      return (Expr *)case_expr;
    }

    case pgp::ExpressionKind::SortGroupClause: {
      auto *expr = op_node->Cast<ItemSortGroupClause>().expr;

      return (Expr *)expr;
    }

    case pgp::ExpressionKind::Aggref: {
      const auto &pop_sc_agg_func = op_node->Cast<ItemAggref>();

      auto *aggref = pop_sc_agg_func.ToAggref();

      List *args = TranslateScalarChildrenScalar(op_node->children);
      aggref->aggdirectargs = TranslateScalarChildrenScalar(pop_sc_agg_func.aggdirectargs);
      aggref->aggorder = TranslateScalarChildrenScalar(pop_sc_agg_func.aggorder);

      std::vector<Index> indexes(list_length(args) + 1, -1);

      {
        ListCell *lc;
        foreach (lc, aggref->aggorder) {
          auto *gc = (SortGroupClause *)lfirst(lc);
          indexes[gc->tleSortGroupRef] = gc->tleSortGroupRef;

          gc->tleSortGroupRef += 1;
        }
      }
      if (pop_sc_agg_func.distinct) {
        List *aggdistinct = TranslateScalarChildrenScalar(pop_sc_agg_func.aggdistinct);

        uint32_t i = 0;
        ListCell *lc;
        foreach (lc, aggdistinct) {
          i++;
          if (i >= list_length(args)) {
            break;
          }

          auto *gc = (SortGroupClause *)lfirst(lc);
          indexes[gc->tleSortGroupRef] = gc->tleSortGroupRef;

          // 'indexes' values are zero-based, but zero means TargetEntry
          // doesn't have corresponding SortGroupClause. So convert back to
          // one-based.
          gc->tleSortGroupRef += 1;

          aggref->aggdistinct = lappend(aggref->aggdistinct, gc);
        }
      }

      AttrNumber attno = 0;
      aggref->args = NIL;
      ListCell *lc;
      foreach (lc, args) {
        attno++;
        TargetEntry *new_target_entry = makeTargetEntry((Expr *)lfirst(lc), attno++, nullptr, false);

        aggref->args = lappend(aggref->args, new_target_entry);
        if (list_length(aggref->aggorder) > 0 || list_length(aggref->aggdistinct) > 0) {
          new_target_entry->ressortgroupref = indexes[attno] == -1 ? 0 : (indexes[attno] + 1);
        }
      }

      Oid input_types[FUNC_MAX_ARGS];
      int num_arguments;

      auto aggtranstype = Catalog::GetAggTranstype(aggref->aggfnoid);

      num_arguments = get_aggregate_argtypes(aggref, input_types);

      aggtranstype = resolve_aggregate_transtype(aggref->aggfnoid, aggtranstype, input_types, num_arguments);
      aggref->aggtranstype = aggtranstype;

      if (args != nullptr)
        aggref->inputcollid = exprCollation(linitial_node(Node, args));

      return (Expr *)aggref;
    }

    default:
      throw OptException("Unsupported operator type:");
      return nullptr;
  }
}

Expr *PlanMeta::GenerateVarExpr(ColRef *colref) {
  if (generator.upper_res_cols.contains(colref)) {
    auto *param = makeNode(Param);
    param->paramkind = PARAM_EXEC;
    param->paramtype = colref->type;
    param->paramid = generator.generator_context.GetNextParamId(param->paramtype);
    param->paramtypmod = colref->modifier;
    param->paramcollid = get_typcollation(param->paramtype);

    generator.param_id_map.insert({colref->ref_id, param->paramid});

    return (Expr *)param;
  }

  if (generator.colid_subplan_map.contains(colref->ref_id))
    return generator.colid_subplan_map.at(colref->ref_id);

  Index varno = 0, varnosyn = 0;
  AttrNumber attno = 0, varattnosyn = 0;

  if (range_table_context.init) {
    varno = range_table_context.rte_index;
    attno = colref->attnum;

    varnosyn = varno;
    varattnosyn = attno;
  } else if (!children_metas.empty()) {
    // TODO: support multiple children
    PGP_ASSERT(children_metas.size() <= 2, "Only support two children for now");
    const auto &left_child = children_metas[0];
    TargetEntry *target_entry = nullptr;
    if (left_child.colid_target_map.contains(colref->ref_id)) {
      varno = OUTER_VAR;
      target_entry = left_child.colid_target_map.at(colref->ref_id);
      attno = target_entry->resno;
    } else if (children_metas.size() == 2) {
      const auto &right_child = children_metas[1];
      if (right_child.colid_target_map.contains(colref->ref_id)) {
        varno = INNER_VAR;
        target_entry = right_child.colid_target_map.at(colref->ref_id);
        attno = target_entry->resno;
      } else
        throw std::runtime_error("Column not found in children");
    }

    if (IsA(target_entry->expr, Var)) {
      varnosyn = ((Var *)target_entry->expr)->varno;
      varattnosyn = ((Var *)target_entry->expr)->varattno;
    } else {
      varnosyn = varno;
      varattnosyn = attno;
    }
  }

  auto *var = makeVar((int)varno, attno, colref->type, colref->modifier, 100, 0);

  var->varnosyn = varnosyn;
  var->varattnosyn = varattnosyn;

  return (Expr *)var;
}

PlanMeta &PlanMeta::InitRangeTableContext(RangeTblEntry *rte) {
  range_table_context.init = true;
  range_table_context.rte_index = generator.generator_context.GetRTEIndexByAssignedQueryId();
  range_table_context.rel_oid = rte->relid;
  generator.generator_context.AddRTE(rte);

  return *this;
}

PlannedStmt *PlanGenerator::BuildStmt(const PlanMeta &plan_meta, std::vector<std::string> pdrgpmdname) const {
  int idx = 0;
  foreach_node(TargetEntry, target, plan_meta.plan->targetlist) {
    pfree(target->resname);
    target->resname = pstrdup(pdrgpmdname[idx++].c_str());
  }

  PlannedStmt *planned_stmt = makeNode(PlannedStmt);

  planned_stmt->rtable = generator_context.rtable_entries_list;
  planned_stmt->subplans = generator_context.subplan_entries_list;
  planned_stmt->planTree = plan_meta.plan;
  planned_stmt->canSetTag = true;
  planned_stmt->commandType = CMD_SELECT;
  planned_stmt->resultRelations = nullptr;
  planned_stmt->paramExecTypes = generator_context.param_types_list;

  return planned_stmt;
}

PlanMeta PlanGenerator::BuildPlan(GroupExpression *gexpr, const ColRefArray &req_cols,
                                  const std::vector<PlanMeta> &children_metas) {
  switch (gexpr->Pop()->kind) {
    case OperatorType::PhysicalScan: {
      const auto &scan_node = gexpr->Pop()->Cast<PhysicalScan>();

      PlanMeta scan_meta{.generator = *this, .children_metas = children_metas};

      scan_meta.InitRangeTableContext(scan_node.table_desc)
          .GenerateSeqScan(generator_context.GetNextPlanId())
          .GenerateTargetList(req_cols)
          .GenerateFilter(scan_node.filter)
          .SetPlanStats(gexpr);
      return scan_meta;
    }

    case OperatorType::PhysicalStreamAgg:
    case OperatorType::PhysicalHashAgg:
    case OperatorType::PhysicalScalarAgg: {
      const auto &agg_node = gexpr->Pop()->Cast<PhysicalAgg>();

      PlanMeta agg_meta{.generator = *this, .children_metas = children_metas};

      agg_meta.GenerateAgg(generator_context.GetNextPlanId(), agg_node)
          .GenerateTargetList(agg_node.project_exprs, req_cols)
          .SetAggGroupInfo(agg_node)
          .SetPlanStats(gexpr);

      auto *plan = (Agg *)agg_meta.plan;
      plan->numGroups = (int64_t)plan->plan.plan_rows;

      return agg_meta;
    }

    case OperatorType::PhysicalLimit: {
      const auto &limit_node = gexpr->Pop()->Cast<PhysicalLimit>();

      PlanMeta limit_meta{.generator = *this, .children_metas = children_metas};

      limit_meta.GenerateLimit(generator_context.GetNextPlanId()).GenerateTargetList(req_cols).SetPlanStats(gexpr);

      auto *limit = (Limit *)limit_meta.plan;
      limit->limitCount = (Node *)limit_meta.GenerateExpr(limit_node.limit);
      limit->limitOffset = (Node *)limit_meta.GenerateExpr(limit_node.offset);

      return limit_meta;
    }

    // TODO: push filter into inner child and resolve outer references
    case OperatorType::PhysicalNLJoin: {
      const auto &join_node = gexpr->Pop()->Cast<PhysicalNLJoin>();

      PlanMeta join_meta{.generator = *this, .children_metas = children_metas};

      join_meta.GenerateNestedLoopJoin(generator_context.GetNextPlanId(), join_node)
          .GenerateTargetList(req_cols)
          .GenerateFilter(join_node.filter, true)
          .SetPlanStats(gexpr);

      return join_meta;
    }

    case OperatorType::PhysicalApply: {
      const auto &apply_node = gexpr->Pop()->Cast<PhysicalApply>();

      PlanMeta apply_meta{.generator = *this, .children_metas = children_metas};

      if (apply_node.subquery_type == EXPR_SUBLINK) {
        apply_meta.GenerateResult(generator_context.GetNextPlanId())
            .GenerateSubplan(apply_node)
            .GenerateTargetList(req_cols)
            .GenerateFilter(apply_node.filter);
      } else if (apply_node.subquery_type == ANY_SUBLINK || apply_node.subquery_type == ALL_SUBLINK) {
        apply_meta.GenerateResult(generator_context.GetNextPlanId())
            .GenerateSubplan(apply_node)
            .GenerateTargetList(req_cols);
        auto *plan = apply_meta.plan;
        const auto &pop_sc_cmp = apply_node.filter->Cast<ItemOpExpr>();

        auto *paramref = (pop_sc_cmp.GetChild(1))->Cast<ItemIdent>().colref;

        auto *op_expr = pop_sc_cmp.ToOpExpr();

        auto *param = makeNode(Param);
        param->paramkind = PARAM_EXEC;
        param->paramtype = paramref->type;
        param->paramid = generator_context.GetNextParamId(param->paramtype);
        param->paramtypmod = paramref->modifier;
        param->paramcollid = get_typcollation(param->paramtype);

        op_expr->args = lappend(op_expr->args, apply_meta.GenerateExpr(pop_sc_cmp.GetChild(0)));
        op_expr->args = lappend(op_expr->args, param);

        for (auto *colref : apply_node.expr_refs) {
          auto *subplan = (SubPlan *)colid_subplan_map.at(colref->ref_id);
          subplan->paramIds = lappend_int(subplan->paramIds, param->paramid);
          subplan->testexpr = (Node *)op_expr;

          if (apply_node.is_not_subquery) {
            BoolExpr *scalar_bool_expr = makeNode(BoolExpr);
            scalar_bool_expr->boolop = NOT_EXPR;
            scalar_bool_expr->args = list_make1(subplan);
            plan->qual = lappend(plan->qual, scalar_bool_expr);
          } else
            plan->qual = lappend(plan->qual, subplan);
        }

      } else if (apply_node.subquery_type == EXISTS_SUBLINK) {
        apply_meta.GenerateResult(generator_context.GetNextPlanId())
            .GenerateSubplan(apply_node)
            .GenerateTargetList(req_cols);

        auto *plan = apply_meta.plan;
        for (auto *colref : apply_node.expr_refs) {
          auto *subplan = colid_subplan_map.at(colref->ref_id);
          if (apply_node.is_not_subquery) {
            BoolExpr *scalar_bool_expr = makeNode(BoolExpr);
            scalar_bool_expr->boolop = NOT_EXPR;
            scalar_bool_expr->args = list_make1(subplan);
            plan->qual = lappend(plan->qual, scalar_bool_expr);
          } else
            plan->qual = lappend(plan->qual, subplan);
        }

      } else
        throw OptException("TODO: new apply type");

      upper_res_cols.clear();
      colid_subplan_map.clear();
      return apply_meta;
    }

    case OperatorType::PhysicalSort: {
      const auto &sort_node = gexpr->Pop()->Cast<PhysicalSort>();

      PlanMeta sort_meta{.generator = *this, .children_metas = children_metas};

      sort_meta.GenerateSort(generator_context.GetNextPlanId())
          .GenerateTargetList(req_cols)
          .SetSortInfo(sort_node)
          .SetPlanStats(gexpr);

      return sort_meta;
    }

    case OperatorType::PhysicalComputeScalar: {
      const auto &result_node = gexpr->Pop()->Cast<PhysicalComputeScalar>();
      PlanMeta result_meta{.generator = *this, .children_metas = children_metas};

      result_meta.GenerateResult(generator_context.GetNextPlanId())
          .GenerateTargetList(result_node.project_exprs, req_cols)
          .SetPlanStats(gexpr);

      return result_meta;
    }

    default:
      return PlanMeta{.generator = *this, .children_metas{}};
  }
}
}  // namespace pgp