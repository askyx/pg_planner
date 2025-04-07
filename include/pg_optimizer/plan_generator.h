#pragma once

#include <cstdint>
#include <unordered_map>
#include <vector>

#include "pg_operator/item_expr.h"
#include "pg_optimizer/colref.h"
#include "pg_optimizer/group_expression.h"
extern "C" {
#include <postgres.h>

#include <nodes/nodes.h>
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

template <typename T>
concept IsScanDerived = requires() { T::scan; };

template <typename T>
concept IsPlanDerived = requires() { T::plan; };

template <typename T>
concept IsJoinDerived = requires() { T::join; };

enum class FilterHostType {
  SCAN,
  JOIN,
  INDEXSCAN,
  INDEXONLYSCAN,
};

/*
 生成一个node的时候使用，保存所有需要的上下文
*/
struct PlanMeta {
  PlanGenerator &generator;

  Plan *plan;

  // child contexts, 用于生成left，right tree以及targetlist
  const std::vector<PlanMeta> &children_metas;

  // targetlist和colrefid的映射，用于上层生成targetlist
  std::unordered_map<uint32_t, TargetEntry *> colid_target_map;

  // 表的基础信息，
  struct {
    bool init{false};
    bool use_index{false};
    Oid rel_oid;
    Index rte_index;
    std::unordered_map<uint32_t, AttrNumber> colref_to_index_map;
  } range_table_context;

  template <IsScanDerived N, NodeTag T>
  PlanMeta &GenerateScanNode();

  template <IsPlanDerived N, NodeTag T>
  PlanMeta &GeneratePlanNode();

  template <IsJoinDerived N, NodeTag T>
  PlanMeta &GenerateJoinNode(JoinType join_type);

  PlanMeta &InitRangeTableContext(RangeTblEntry *rte);

  PlanMeta &SetPlanStats(GroupExpression *gexpr);

  // for computed target list
  PlanMeta &GenerateTargetList(const ExprArray &project_list, const ColRefArray &req_cols);

  PlanMeta &GenerateTargetList(const ColRefArray &req_cols);

  PlanMeta &GenerateIndexTargetList(const ColRefArray &req_cols);

  template <FilterHostType FH>
  PlanMeta &GenerateFilter(const ItemExprPtr &filter_node);

  PlanMeta &GenerateSubplan(const PhysicalApply &apply);

  PlanMeta &SetAggGroupInfo(const PhysicalAgg &agg_node);

  PlanMeta &SetSortInfo(const PhysicalSort &sort_node);

  PlanMeta &SetIndexInfo(Oid relid, Oid index);

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

    int32_t GetNextPlanId() { return plan_id_counter++; }

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