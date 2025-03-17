#pragma once

#include <cstdint>
#include <unordered_map>
#include <vector>

#include "pg_operator/item_expr.h"
#include "pg_operator/operator_node.h"
#include "pg_optimizer/colref.h"
#include "pg_optimizer/group_expression.h"
extern "C" {
#include <postgres.h>

#include <nodes/parsenodes.h>
#include <nodes/plannodes.h>
#include <nodes/primnodes.h>
}

namespace pgp {

class PhysicalAgg;
class PhysicalSort;
class PhysicalApply;
class PhysicalNLJoin;

struct PlanGenerator;

struct PlanMeta {
  PlanGenerator &generator;

  Plan *plan;

  const std::vector<PlanMeta> &children_metas;

  std::unordered_map<uint32_t, TargetEntry *> colid_target_map;

  struct {
    bool init{false};
    Oid rel_oid;
    Index rte_index;
    std::unordered_map<uint32_t, int> colid_to_attno_map;
  } range_table_context;

  PlanMeta &InitRangeTableContext(RangeTblEntry *rte, std::unordered_map<uint32_t, int> colid2attno);

  PlanMeta &SetPlanStats(GroupExpression *gexpr);

  PlanMeta &GenerateTargetList(const ExprArray &project_list, const ColRefArray &req_cols);

  PlanMeta &GenerateTargetList(const ColRefArray &req_cols);

  PlanMeta &GenerateFilter(const ItemExprPtr &filter_node, bool join_filter = false);

  PlanMeta &GenerateResult(int plan_node_id);

  PlanMeta &GenerateSeqScan(int plan_node_id);

  PlanMeta &GenerateSort(int plan_node_id);

  PlanMeta &GenerateLimit(int plan_node_id);

  PlanMeta &GenerateNestedLoopJoin(int plan_node_id, const PhysicalNLJoin &nljoin_node);

  PlanMeta &GenerateAgg(int plan_node_id, const PhysicalAgg &agg_node);

  PlanMeta &GenerateSubplan(SubLinkType slinktype, const PhysicalApply &apply);

  PlanMeta &SetAggGroupInfo(const PhysicalAgg &agg_node);

  PlanMeta &SetSortInfo(const PhysicalSort &sort_node);

  TargetEntry *GenerateTargetEntry(Expr *expr, AttrNumber resno, const std::string &col_name, uint32_t colid);

  Expr *GenerateExpr(const ItemExprPtr &op_node);

  Expr *GenerateVarExpr(ColRef *colref);

  List *TranslateScalarChildrenScalar(const ExprArray &pexpr);
};

struct PlanGenerator {
  struct {
    int32_t plan_id_counter{0};

    int32_t param_id_counter{0};

    List *param_types_list{nullptr};

    List *rtable_entries_list{nullptr};

    List *subplan_entries_list{nullptr};

    int32_t GetNextPlanId() { return ++plan_id_counter; }

    int32_t GetNextParamId(Oid typeoid) {
      param_types_list = lappend_oid(param_types_list, typeoid);
      return ++param_id_counter;
    }

    void AddRTE(RangeTblEntry *rte) { rtable_entries_list = lappend(rtable_entries_list, rte); }

    Index GetRTEIndexByAssignedQueryId() const { return list_length(rtable_entries_list) + 1; }
  } generator_context;

  std::unordered_map<uint32_t, Expr *> colid_subplan_map;

  // subplan params
  ColRefSet upper_res_cols;

  std::unordered_map<uint32_t, int> param_id_map;

  PlanMeta BuildPlan(GroupExpression *gexpr, const ColRefArray &req_cols, const std::vector<PlanMeta> &children_metas);

  PlannedStmt *BuildStmt(const PlanMeta &plan_meta, std::vector<std::string> pdrgpmdname) const;
};

}  // namespace pgp