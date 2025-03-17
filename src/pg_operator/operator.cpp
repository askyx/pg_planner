#include "pg_operator/operator.h"

#include <format>
#include <string>

#include "pg_operator/logical_operator.h"
#include "pg_operator/physical_operator.h"

namespace pgp {

std::string Operator::EopIdToString(OperatorType eopid) const {
  switch (eopid) {
    case OperatorType::LogicalGet:
      return "LogicalGet";
    case OperatorType::LogicalSelect:
      return "LogicalSelect";

    case OperatorType::LogicalGbAgg:
      return "LogicalGbAgg";
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
    case OperatorType::LogicalProject:
      return "LogicalProject";
    case OperatorType::LogicalApply: {
      const auto &id = Cast<LogicalApply>();
      switch (id.subquery_type) {
        case SubQueryType::ALL_SUBLINK:
          return std::format("LogicalApply: ALL_SUBLINK");
        case SubQueryType::ANY_SUBLINK:
          return std::format("LogicalApply: ANY_SUBLINK");
        case SubQueryType::EXISTS_SUBLINK:
          return std::format("LogicalApply: EXISTS_SUBLINK");
        case SubQueryType::EXPR_SUBLINK:
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

    case OperatorType::PhysicalScan:
      return "PhysicalScan";
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
    case OperatorType::PhysicalFullMergeJoin:
      return "PhysicalFullMergeJoin";

    case OperatorType::PhysicalApply: {
      const auto &id = Cast<PhysicalApply>();
      switch (id.subquery_type) {
        case SubQueryType::ALL_SUBLINK:
          return std::format("PhysicalApply: ALL_SUBLINK");
        case SubQueryType::ANY_SUBLINK:
          return std::format("PhysicalApply: ANY_SUBLINK");
        case SubQueryType::EXISTS_SUBLINK:
          return std::format("PhysicalApply: EXISTS_SUBLINK");
        case SubQueryType::EXPR_SUBLINK:
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