#include "pg_optimizer/property_deriver.h"

#include <vector>

#include "common/exception.h"
#include "pg_operator/item_expr.h"
#include "pg_operator/logical_operator.h"
#include "pg_operator/operator.h"
#include "pg_operator/physical_operator.h"
#include "pg_optimizer/colref.h"
#include "pg_optimizer/group_expression.h"

extern "C" {
#include <nodes/nodes.h>
#include <utils/typcache.h>
}
namespace pgp {
/*
 上层输出决定下层输入的列
 * 上层有输出可能是由中间某些node定义的列，例如 agg，result 等，这些node的下层需要输出当前表达式的usecol
 * 如果是定义的列，则下层是无法输出，则需要去掉当前node定义的列
 * 此外还有一些node需要额外的列进行某些操作，例如 group 列，如果不输出，但是下层也需要提供，类似的还有filter
 * 对于upperref，可能会出现在output中，但是这些列由node自行管理，其他时候，下层无法直接提供upperref
 * 所以需要最后进行一个交集去掉upperref

 * 对于输出顺序，使用 vec 保存node的输入输出，最顶层例如 limit等，需要和最终的输出保持一致
 * 下层某些node不会对tuple进行重组操作，例如filter，sort等，这些node的输入输出顺序也需要和上层保持一致
 * 对于会对tuple进行重组操作的node，例如join，agg等，则可以按照任意顺序输出，这里按照表的结构定义顺序进行输出
*/
ColRef2DArray PropertiesDriver::PcrsRequired(GroupExpression *gexpr, const ColRefArray &pcrs_required) {
  const auto &pop_physical = gexpr->Pop();
  switch (pop_physical->kind) {
    case OperatorType::PhysicalHashAgg:
    case OperatorType::PhysicalScalarAgg:
    case OperatorType::PhysicalStreamAgg: {
      const auto &agg = pop_physical->Cast<PhysicalAgg>();
      ColRefSetWapper pcrs;

      ColRefSetWapper projecte_used_cols;
      for (const auto &project : agg.project_exprs) {
        projecte_used_cols.Union(project->DeriveUsedColumns());
      }
      // grouping + upper req + aggfunc used  - defined cols(aggfunc self)
      pcrs.AddColRef(agg.group_columns)
          .Union(pcrs_required)
          .Union(projecte_used_cols)
          .Difference(gexpr->GetGroup()->GroupProperties()->GetDefinedColumns());

      // intersect computed column set with child's output columns
      ColRef2DArray req_child_cols;
      for (auto *child : gexpr->GetChildGroup()) {
        ColRefSetWapper child_cols{child->GroupProperties()->GetOutputColumns()};
        req_child_cols.emplace_back(child_cols.Intersection(pcrs).ToArray());
      }
      return req_child_cols;
    }

    case OperatorType::PhysicalComputeScalar: {
      const auto &computescalar = pop_physical->Cast<PhysicalComputeScalar>();
      ColRefSetWapper pcrs;

      ColRefSetWapper projecte_used_cols;
      ColRefSetWapper projecte_defined_cols = gexpr->GetGroup()->GroupProperties()->GetDefinedColumns();
      for (const auto &project : computescalar.project_exprs) {
        projecte_used_cols.Union(project->DeriveUsedColumns());
      }

      pcrs.Union(pcrs_required).Union(projecte_used_cols).Difference(projecte_defined_cols);

      ColRef2DArray req_child_cols;
      for (auto *child : gexpr->GetChildGroup()) {
        ColRefSetWapper child_cols{child->GroupProperties()->GetOutputColumns()};
        req_child_cols.emplace_back(child_cols.Intersection(pcrs).ToArray());
      }
      return req_child_cols;
    }
    case OperatorType::PhysicalFilter: {
      const auto &computescalar = pop_physical->Cast<PhysicalFilter>();
      ColRefSetWapper pcrs;

      pcrs.Union(pcrs_required).Union(computescalar.filter->DeriveUsedColumns());

      ColRef2DArray req_child_cols;
      for (auto *child : gexpr->GetChildGroup()) {
        ColRefSetWapper child_cols{child->GroupProperties()->GetOutputColumns()};
        req_child_cols.emplace_back(child_cols.Intersection(pcrs).ToArray());
      }
      return req_child_cols;
    }

    case OperatorType::PhysicalFullMergeJoin:
    case OperatorType::PhysicalHashJoin: {
      const auto &computescalar = pop_physical->Cast<PhysicalHashJoin>();

      ColRefSetWapper pcrs;

      pcrs.Union(pcrs_required)
          .Union(computescalar.filter->DeriveUsedColumns())
          .Difference(gexpr->GetGroup()->GroupProperties()->GetDefinedColumns());

      ColRef2DArray req_child_cols;
      for (auto *child : gexpr->GetChildGroup()) {
        ColRefSetWapper child_cols{child->GroupProperties()->GetOutputColumns()};
        req_child_cols.emplace_back(child_cols.Intersection(pcrs).ToArray());
      }
      return req_child_cols;
    }

    case OperatorType::PhysicalLimit:
    case OperatorType::PhysicalScan:
    case OperatorType::PhysicalIndexScan:
    case OperatorType::PhysicalSort: {
      return {pcrs_required};
    }

    case OperatorType::PhysicalApply: {
      const auto &apply = pop_physical->Cast<PhysicalApply>();

      ColRefSetWapper pcrs;

      pcrs.Union(pcrs_required).Union(apply.filter->DeriveUsedColumns());

      // For subqueries in the projection list, the required columns from the outer child
      // are often pushed down to the inner child and are not visible at the top level
      // so we can use the outer refs of the inner child as required from outer child
      ColRefSetWapper outer_refs = gexpr->GetChildGroup()[1]->GroupProperties()->GetOuterReferences();
      outer_refs.Union(pcrs).Intersection(gexpr->GetChildGroup()[0]->GroupProperties()->GetOutputColumns());

      // request inner child of correlated join to provide required inner columns
      ColRefSetWapper apply_cols = apply.expr_refs;
      apply_cols.Union(pcrs).Intersection(gexpr->GetChildGroup()[1]->GroupProperties()->GetOutputColumns());

      return {outer_refs.ToArray(), apply_cols.ToArray()};
    }

    case OperatorType::PhysicalNLJoin: {
      const auto &nejoin = pop_physical->Cast<PhysicalNLJoin>();

      ColRefSetWapper pcrs;

      pcrs.Union(pcrs_required).Union(nejoin.filter->DeriveUsedColumns());

      // For subqueries in the projection list, the required columns from the outer child
      // are often pushed down to the inner child and are not visible at the top level
      // so we can use the outer refs of the inner child as required from outer child
      // if (0 == child_index) {
      ColRefSetWapper outer_refs = gexpr->GetChildGroup()[1]->GroupProperties()->GetOuterReferences();

      return {outer_refs.Union(pcrs)
                  .Difference(gexpr->GetGroup()->GroupProperties()->GetDefinedColumns())
                  .Intersection(gexpr->GetChildGroup()[0]->GroupProperties()->GetOutputColumns())
                  .ToArray(),
              pcrs.Difference(gexpr->GetGroup()->GroupProperties()->GetDefinedColumns())
                  .Intersection(gexpr->GetChildGroup()[1]->GroupProperties()->GetOutputColumns())
                  .ToArray()};
    }

    default:
      throw OptException("Unsupported operator for PropertiesDriver::PcrsRequired ");
  }
  return {};
}

ColRefSet PropertiesDriver::DeriveOutputColumns(OperatorNode *expr) {
  const auto &logical_operator = expr->content;

  switch (logical_operator->kind) {
    case OperatorType::LogicalLimit: {
      return expr->GetChild(0)->DeriveOutputColumns();
    }

    case OperatorType::LogicalJoin: {
      const auto &logical_join = logical_operator->Cast<LogicalJoin>();
      if (logical_join.join_type == JOIN_SEMI || logical_join.join_type == JOIN_ANTI)
        return expr->GetChild(0)->DeriveOutputColumns();

      ColRefSet pcrs;

      // union columns from the first N-1 children
      for (const auto &child : expr->children)
        ColRefSetUnion(pcrs, child->DeriveOutputColumns());

      return pcrs;
    }

    case OperatorType::LogicalGet: {
      return ColRefArrayToSet(logical_operator->Cast<LogicalGet>().relation_info->output_columns);
    }

    case OperatorType::LogicalApply: {
      const auto &logical_apply = logical_operator->Cast<LogicalApply>();
      if (logical_apply.subquery_type == EXPR_SUBLINK) {
        ColRefSet pcrs;

        // union columns from the first N-1 children
        for (const auto &child : expr->children)
          ColRefSetUnion(pcrs, child->DeriveOutputColumns());

        return pcrs;
      }
      return expr->GetChild(0)->DeriveOutputColumns();
    }

    case OperatorType::LogicalGbAgg: {
      const auto &logical_agg = logical_operator->Cast<LogicalGbAgg>();

      // include the intersection of the grouping columns and the child's output
      auto pcrs = ColRefArrayToSet(logical_agg.group_columns);
      ColRefSetIntersection(pcrs, expr->GetChild(0)->DeriveOutputColumns());

      for (const auto &project : logical_agg.project_exprs) {
        auto *colref = project->Cast<ItemProjectElement>().colref;
        pcrs.insert(colref);
      }

      return pcrs;
    }

    case OperatorType::LogicalFilter: {
      return expr->GetChild(0)->DeriveOutputColumns();
    }

    case OperatorType::LogicalProject: {
      ColRefSet pcrs;

      ColRefSetUnion(pcrs, expr->GetChild(0)->DeriveOutputColumns());
      ColRefSetUnion(pcrs, expr->DeriveDefinedColumns());

      return pcrs;
    }

    default:
      throw OptException("Unsupported operator for PropertiesDriver::DeriveOutputColumns ");
  }

  return {};
}

KeyCollection PropertiesDriver::DeriveKeyCollection(OperatorNode *expr) {
  const auto &logical_operator = expr->content;

  switch (logical_operator->kind) {
    case OperatorType::LogicalLimit:
      return expr->GetChild(0)->DeriveProp()->GetKeyCollection();
    case OperatorType::LogicalJoin: {
      const auto &logical_join = logical_operator->Cast<LogicalJoin>();
      if (logical_join.join_type == JOIN_ANTI || logical_join.join_type == JOIN_SEMI)
        return expr->GetChild(0)->DeriveProp()->GetKeyCollection();

      ColRefSet pcrs;
      for (const auto &child : expr->children) {
        auto pkc = child->DeriveKeyCollection();
        if (pkc.empty()) {
          // if a child has no key, the operator has no key
          return {};
        }

        auto colref_array = pkc[0];
        AddColRef(colref_array, colref_array);
      }

      return {pcrs};
    }

    case OperatorType::LogicalApply: {
      const auto &logical_apply = logical_operator->Cast<LogicalApply>();
      if (auto subquery_type = logical_apply.subquery_type;
          subquery_type == ALL_SUBLINK || subquery_type == ANY_SUBLINK || subquery_type == EXISTS_SUBLINK)
        return expr->GetChild(0)->DeriveProp()->GetKeyCollection();
      ColRefSet pcrs;
      for (const auto &child : expr->children) {
        auto pkc = child->DeriveKeyCollection();
        if (pkc.empty()) {
          // if a child has no key, the operator has no key
          return {};
        }

        auto colref_array = pkc[0];
        AddColRef(colref_array, colref_array);
      }

      return {pcrs};
    }
    case OperatorType::LogicalGbAgg: {
      const auto &logical_agg = logical_operator->Cast<LogicalGbAgg>();
      KeyCollection pkc;

      // Gb produces a key only if it's global
      if (0 < logical_agg.group_columns.size()) {
        // grouping columns always constitute a key
        auto pcrs = ColRefArrayToSet(logical_agg.group_columns);
        pkc.emplace_back(pcrs);
      } else {
        ColRefSet pcrs;
        for (const auto &project : logical_agg.project_exprs) {
          auto *colref = project->Cast<ItemProjectElement>().colref;
          pcrs.insert(colref);
        }

        if (0 == pcrs.size()) {
          // aggregate defines no columns, e.g. select 1 from r group by a
          return {};
        }

        pkc.emplace_back(pcrs);
      }

      return pkc;
    }
    case OperatorType::LogicalFilter:
    case OperatorType::LogicalProject:
      return expr->GetChild(0)->DeriveProp()->GetKeyCollection();
    case OperatorType::LogicalGet:
      return {};

    default:
      throw OptException("Unsupported operator for PropertiesDriver::DeriveKeyCollection ");
  }

  return {};
}

ColRefSet PropertiesDriver::DeriveNotNullColumns(OperatorNode *expr) {
  const auto &logical_operator = expr->content;

  switch (logical_operator->kind) {
    case OperatorType::LogicalJoin: {
      const auto &logical_join = logical_operator->Cast<LogicalJoin>();
      auto join_type = logical_join.join_type;
      if (join_type == JOIN_INNER) {
        ColRefSet pcrs;

        // union not nullable columns from the first N-1 children
        for (const auto &child : expr->children)
          ColRefSetUnion(pcrs, child->DeriveDefinedColumns());

        return pcrs;
      }
      if (join_type == JOIN_ANTI || join_type == JOIN_LEFT || join_type == JOIN_SEMI)
        return expr->GetChild(0)->DeriveNotNullColumns();
      if (join_type == JOIN_RIGHT) {
        auto pcrs = expr->GetChild(1)->DeriveNotNullColumns();

        return pcrs;
      }
      return {};
    }

    case OperatorType::LogicalApply: {
      const auto &logical_apply = logical_operator->Cast<LogicalApply>();
      if (logical_apply.subquery_type == EXPR_SUBLINK) {
        ColRefSet pcrs;

        // union not nullable columns from the first N-1 children

        for (const auto &child : expr->children)
          ColRefSetUnion(pcrs, child->DeriveNotNullColumns());

        return pcrs;
      }
      return expr->GetChild(0)->DeriveNotNullColumns();
    }
    case OperatorType::LogicalFilter:
    case OperatorType::LogicalProject:
      return expr->GetChild(0)->DeriveNotNullColumns();
    case OperatorType::LogicalGbAgg: {
      const auto &logical_agg = logical_operator->Cast<LogicalGbAgg>();

      ColRefSet pcrs;

      // include grouping columns
      AddColRef(pcrs, logical_agg.group_columns);

      // intersect with not nullable columns from relational child
      ColRefSetIntersection(pcrs, expr->GetChild(0)->DeriveNotNullColumns());

      return pcrs;
    }

    case OperatorType::LogicalGet: {
      ColRefSet pcrs;
      auto xx = expr->DeriveOutputColumns();
      AddColRef(pcrs, xx);

      // filters out nullable columns
      for (auto *colref : xx) {
        if (colref->nullable) {
          pcrs.erase(colref);
        }
      }

      return pcrs;
    }
    case OperatorType::LogicalLimit:
      return {};

    default:
      throw OptException("Unsupported operator for PropertiesDriver::DeriveKeyCollection");
  }

  return {};
}

static Cardinality MaxcardDef(OperatorNode *expr) {
  Cardinality maxcard = 1;

  for (const auto &child : expr->children) {
    maxcard *= child->DeriveMaxCard();
  }

  return maxcard;
}

Cardinality PropertiesDriver::DeriveMaxCard(OperatorNode *expr) {
  const auto &logical_operator = expr->content;
  switch (logical_operator->kind) {
    case OperatorType::LogicalJoin: {
      const auto &logical_join = logical_operator->Cast<LogicalJoin>();
      auto join_type = logical_join.join_type;
      if (join_type == JOIN_INNER)
        return MaxcardDef(expr);

      if (join_type == JOIN_SEMI)
        return expr->GetChild(0)->DeriveMaxCard();

      if (join_type == JOIN_LEFT) {
        Cardinality max_card = expr->GetChild(0)->DeriveMaxCard();
        Cardinality max_card_inner = expr->GetChild(1)->DeriveMaxCard();
        return max_card_inner > 0 ? max_card * max_card_inner : max_card;
      }

      if (join_type == JOIN_RIGHT) {
        Cardinality max_card = expr->GetChild(1)->DeriveMaxCard();
        Cardinality max_card_outer = expr->GetChild(0)->DeriveMaxCard();
        return max_card_outer > 0 ? max_card * max_card_outer : max_card;
      }

      if (join_type == JOIN_FULL) {
        Cardinality left_child_maxcard = expr->GetChild(0)->DeriveMaxCard();
        Cardinality right_child_maxcard = expr->GetChild(1)->DeriveMaxCard();

        if (left_child_maxcard > 0 && right_child_maxcard > 0) {
          Cardinality result_max_card = left_child_maxcard;
          result_max_card *= right_child_maxcard;
          return result_max_card;
        }

        return std::min(left_child_maxcard, right_child_maxcard);
      }

      return 0;
    }

    case OperatorType::LogicalApply: {
      const auto &logical_apply = logical_operator->Cast<LogicalApply>();
      auto subquery_type = logical_apply.subquery_type;
      if (subquery_type == ANY_SUBLINK || subquery_type == EXISTS_SUBLINK || subquery_type == EXPR_SUBLINK)
        return expr->GetChild(0)->DeriveMaxCard();

      if (subquery_type == EXPR_SUBLINK)
        return MaxcardDef(expr);
      // pass on max card of first child
      return expr->GetChild(0)->DeriveMaxCard();
    }
    case OperatorType::LogicalGbAgg: {
      const auto &logical_agg = logical_operator->Cast<LogicalGbAgg>();
      return logical_agg.group_columns.size() == 0 ? 1 : 0;
    }
    case OperatorType::LogicalFilter:
    case OperatorType::LogicalLimit:
    case OperatorType::LogicalProject:
      return expr->GetChild(0)->DeriveMaxCard();
    case OperatorType::LogicalGet:
      return 0;

    default:
      throw OptException("Unsupported operator for PropertiesDriver::DeriveKeyCollection");
  }

  return 0;
}

static ColRefSet DeriveOuterReferences(OperatorNode *expr, const ColRefSet &pcrs_used_additional) {
  ColRefSet outer_refs;

  // collect output columns from relational children
  // and used columns from scalar children
  ColRefSet pcrs_output;

  ColRefSet pcrs_used;
  for (const auto &child : expr->children) {
    // add outer references from relational children
    ColRefSetUnion(outer_refs, child->DeriveOuterReferences());
    ColRefSetUnion(pcrs_output, child->DeriveOutputColumns());
  }

  if (!pcrs_used_additional.empty()) {
    ColRefSetUnion(pcrs_used, pcrs_used_additional);
  }

  // outer references are columns used by scalar child
  // but are not included in the output columns of relational children
  ColRefSetUnion(outer_refs, pcrs_used);
  ColRefSetDifference(outer_refs, pcrs_output);

  return outer_refs;
}

ColRefSet PropertiesDriver::DeriveOuterReferences(OperatorNode *expr) {
  const auto &logical_operator = expr->content;
  switch (logical_operator->kind) {
    case OperatorType::LogicalLimit: {
      const auto &logical_limit = logical_operator->Cast<LogicalLimit>();
      auto pcrs_sort = logical_limit.order_spec->GetUsedColumns();
      auto outer_refs = ::pgp::DeriveOuterReferences(expr, pcrs_sort);

      return outer_refs;
    }

    case OperatorType::LogicalGbAgg: {
      const auto &logical_agg = logical_operator->Cast<LogicalGbAgg>();
      auto pcrs_grp = ColRefArrayToSet(logical_agg.group_columns);

      auto child = expr->GetChild(0);
      ColRefSet outer_refs = child->DeriveOuterReferences();

      // collect output columns from relational children
      // and used columns from scalar children
      ColRefSet pcrs_output = child->DeriveOutputColumns();

      ColRefSet pcrs_used;
      for (const auto &child : logical_agg.project_exprs) {
        ColRefSetUnion(pcrs_used, child->DeriveUsedColumns());
      }

      if (!pcrs_grp.empty()) {
        ColRefSetUnion(pcrs_used, pcrs_grp);
      }

      // outer references are columns used by scalar child
      // but are not included in the output columns of relational children
      ColRefSetUnion(outer_refs, pcrs_used);
      ColRefSetDifference(outer_refs, pcrs_output);

      return outer_refs;
    }
    case OperatorType::LogicalFilter: {
      const auto &logical_select = logical_operator->Cast<LogicalFilter>();
      ColRefSet outer_refs = expr->GetChild(0)->DeriveOuterReferences();

      ColRefSet pcrs_output = expr->GetChild(0)->DeriveOutputColumns();

      ColRefSet pcrs_used = logical_select.filter->DeriveUsedColumns();

      ColRefSetUnion(outer_refs, pcrs_used);
      ColRefSetDifference(outer_refs, pcrs_output);

      return outer_refs;
    }
    case OperatorType::LogicalApply: {
      const auto &logical_apply = logical_operator->Cast<LogicalApply>();
      ColRefSet outer_refs = expr->GetChild(0)->DeriveOuterReferences();
      ColRefSetUnion(outer_refs, expr->GetChild(1)->DeriveOuterReferences());

      ColRefSet pcrs_output = expr->GetChild(0)->DeriveOutputColumns();
      ColRefSetUnion(pcrs_output, expr->GetChild(1)->DeriveOutputColumns());

      ColRefSet pcrs_used = logical_apply.filter->DeriveUsedColumns();

      ColRefSetUnion(outer_refs, pcrs_used);
      ColRefSetDifference(outer_refs, pcrs_output);

      return outer_refs;
    }
    case OperatorType::LogicalJoin: {
      const auto &logical_join = logical_operator->Cast<LogicalJoin>();
      ColRefSet outer_refs = expr->GetChild(0)->DeriveOuterReferences();
      ColRefSetUnion(outer_refs, expr->GetChild(1)->DeriveOuterReferences());

      ColRefSet pcrs_output = expr->GetChild(0)->DeriveOutputColumns();
      ColRefSetUnion(pcrs_output, expr->GetChild(1)->DeriveOutputColumns());

      ColRefSet pcrs_used = logical_join.filter->DeriveUsedColumns();

      ColRefSetUnion(outer_refs, pcrs_used);
      ColRefSetDifference(outer_refs, pcrs_output);

      return outer_refs;
    }
    case OperatorType::LogicalGet: {
      const auto &logical_get = logical_operator->Cast<LogicalGet>();
      ColRefSet outer_refs;

      ColRefSet pcrs_output = ColRefArrayToSet(logical_get.relation_info->output_columns);

      if (logical_get.filter != nullptr) {
        ColRefSet pcrs_used = logical_get.filter->DeriveUsedColumns();
        ColRefSetUnion(outer_refs, pcrs_used);
      }
      ColRefSetDifference(outer_refs, pcrs_output);

      return outer_refs;
    }
    case OperatorType::LogicalProject: {
      const auto &logical_project = logical_operator->Cast<LogicalProject>();
      ColRefSet outer_refs = expr->GetChild(0)->DeriveOuterReferences();

      // collect output columns from relational children
      // and used columns from scalar children
      ColRefSet pcrs_output = expr->GetChild(0)->DeriveOutputColumns();

      ColRefSet pcrs_used;
      for (const auto &child : logical_project.project_exprs) {
        ColRefSetUnion(pcrs_used, child->DeriveUsedColumns());
      }

      // outer references are columns used by scalar child
      // but are not included in the output columns of relational children
      ColRefSetUnion(outer_refs, pcrs_used);
      ColRefSetDifference(outer_refs, pcrs_output);

      return outer_refs;
    }

    default:
      throw OptException("Unsupported operator for PropertiesDriver::DeriveKeyCollection");
  }
  return {};
}

}  // namespace pgp