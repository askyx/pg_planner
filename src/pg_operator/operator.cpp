#include "pg_operator/operator.h"

#include <format>
#include <string>

#include "pg_operator/logical_operator.h"
#include "pg_operator/physical_operator.h"

namespace pgp {

std::string Operator::ToString() const {
  switch (kind) {
    case OperatorType::LogicalGet: {
      const auto &get = Cast<LogicalGet>();
      if (get.table_desc->alias != nullptr)
        return std::format("LogicalGet: {}", get.table_desc->alias->aliasname);

      return std::format("LogicalGet: {}", get.table_desc->eref->aliasname);
    }

    case OperatorType::LogicalFilter: {
      const auto &select = Cast<LogicalFilter>();
      return std::format("LogicalFilter: {}", select.filter->ToString());
    }

    case OperatorType::LogicalGbAgg: {
      const auto &gb_agg = Cast<LogicalGbAgg>();

      std::string group_cols_str;
      for (const auto &col : gb_agg.group_columns) {
        group_cols_str += col->ToString() + ", ";
      }
      if (!group_cols_str.empty()) {
        group_cols_str.pop_back();
        group_cols_str.pop_back();
      }

      std::string agg_cols_str;
      for (const auto &col : gb_agg.project_exprs) {
        agg_cols_str += col->ToString() + ", ";
      }
      if (!agg_cols_str.empty()) {
        agg_cols_str.pop_back();
        agg_cols_str.pop_back();
      }

      return std::format("LogicalGbAgg: group: {}, agg: {}", group_cols_str, agg_cols_str);
    }
    case OperatorType::LogicalLimit: {
      const auto &limit = Cast<LogicalLimit>();
      std::string limit_str;
      if (limit.limit != nullptr)
        limit_str = limit.limit->ToString();
      else
        limit_str = "NULL";
      std::string offset_str;
      if (limit.offset != nullptr)
        offset_str = limit.offset->ToString();
      else
        offset_str = "NULL";
      return std::format("LogicalLimit: limit: {} offset: {}", limit_str, offset_str);
    }
    case OperatorType::LogicalProject: {
      const auto &project = Cast<LogicalProject>();
      std::string project_cols_str;
      for (const auto &col : project.project_exprs) {
        project_cols_str += col->ToString() + ", ";
      }
      if (!project_cols_str.empty()) {
        project_cols_str.pop_back();
        project_cols_str.pop_back();
      }
      return std::format("LogicalProject: {}", project_cols_str);
    }
    case OperatorType::LogicalApply: {
      const auto &id = Cast<LogicalApply>();
      switch (id.subquery_type) {
        case ALL_SUBLINK:
          return std::format("LogicalApply: ALL_SUBLINK");
        case ANY_SUBLINK:
          return std::format("LogicalApply: ANY_SUBLINK");
        case EXISTS_SUBLINK:
          return std::format("LogicalApply: EXISTS_SUBLINK");
        case EXPR_SUBLINK:
          return std::format("LogicalApply: EXPR_SUBLINK");
        default:
          return std::format("LogicalApply:");
      }
    }

    case OperatorType::LogicalJoin: {
      const auto &join = Cast<LogicalJoin>();
      switch (join.join_type) {
        case JOIN_INNER:
          return "LogicalJoin: INNER";
        case JOIN_LEFT:
          return "LogicalJoin: LEFT";
        case JOIN_FULL:
          return "LogicalJoin: FULL";
        case JOIN_RIGHT:
          return "LogicalJoin: RIGHT";
        case JOIN_SEMI:
          return "LogicalJoin: SEMI";
        case JOIN_ANTI:
          return "LogicalJoin: ANTI";
        case JOIN_RIGHT_ANTI:
          return "LogicalJoin: RIGHT_ANTI";
        case JOIN_UNIQUE_OUTER:
        case JOIN_UNIQUE_INNER:
          return "LogicalJoin";
          break;
      }
    }

    case OperatorType::PhysicalFilter:
      return "PhysicalFilter";
    case OperatorType::PhysicalNLJoin: {
      const auto &nested_join = Cast<PhysicalNLJoin>();
      switch (nested_join.join_type) {
        case JOIN_INNER:
          return "PhysicalNLJoin: INNER";
        case JOIN_LEFT:
          return "PhysicalNLJoin: LEFT";
        case JOIN_FULL:
          return "PhysicalNLJoin: FULL";
        case JOIN_RIGHT:
          return "PhysicalNLJoin: RIGHT";
        case JOIN_SEMI:
          return "PhysicalNLJoin: SEMI";
        case JOIN_ANTI:
          return "PhysicalNLJoin: ANTI";
        case JOIN_RIGHT_ANTI:
          return "PhysicalNLJoin: RIGHT_ANTI";
        case JOIN_UNIQUE_OUTER:
        case JOIN_UNIQUE_INNER:
          return "PhysicalNLJoin";
          break;
      }
    }

    case OperatorType::PhysicalScan:
      return "PhysicalScan";

    case pgp::OperatorType::PhysicalIndexScan:
      return "PhysicalIndexScan";

    case pgp::OperatorType::PhysicalIndexOnlyScan:
      return "PhysicalIndexOnlyScan";

    case OperatorType::PhysicalFullMergeJoin:
      return "PhysicalFullMergeJoin";

    case OperatorType::PhysicalApply: {
      const auto &id = Cast<PhysicalApply>();
      switch (id.subquery_type) {
        case ALL_SUBLINK:
          return std::format("PhysicalApply: ALL_SUBLINK");
        case ANY_SUBLINK:
          return std::format("PhysicalApply: ANY_SUBLINK");
        case EXISTS_SUBLINK:
          return std::format("PhysicalApply: EXISTS_SUBLINK");
        case EXPR_SUBLINK:
          return std::format("PhysicalApply: EXPR_SUBLINK");
        default:
          return std::format("PhysicalApply:");
      }
    }

    case OperatorType::PhysicalHashJoin:
      return "PhysicalHashJoin";

    case OperatorType::PhysicalHashAgg:
      return "PhysicalHashAgg";
    case OperatorType::PhysicalStreamAgg:
      return "PhysicalStreamAgg";
    case OperatorType::PhysicalScalarAgg:
      return "PhysicalScalarAgg";

    case OperatorType::PhysicalSort:
      return "PhysicalSort";
    case OperatorType::PhysicalLimit: {
      const auto &limit = Cast<PhysicalLimit>();
      std::string limit_str;
      if (limit.limit != nullptr)
        limit_str = limit.limit->ToString();
      else
        limit_str = "NULL";
      std::string offset_str;
      if (limit.offset != nullptr)
        offset_str = limit.offset->ToString();
      else
        offset_str = "NULL";
      return std::format("PhysicalLimit: limit: {} offset: {}", limit_str, offset_str);
    }
    case OperatorType::PhysicalComputeScalar:
      return "PhysicalComputeScalar";

    default:
      return "Unknown Operator Id";
  }
}
}  // namespace pgp