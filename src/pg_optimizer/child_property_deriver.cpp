#include "pg_optimizer/child_property_deriver.h"

#include <memory>

#include "common/exception.h"
#include "pg_operator/operator.h"
#include "pg_operator/physical_operator.h"
#include "pg_optimizer/group_expression.h"
#include "pg_optimizer/properties.h"
#include "pg_optimizer/property.h"

namespace pgp {

/*
 * 使用 requirements 获得当前节点的输入和输出属性
 * 如果当前节点无法提供 requirements 类型的属性，则按照实际输出输入属性返回
 *  例如当前节点是 hashagg，要求的是 sort，此时 hash agg 可以直接返回null
 *
 * 如果 node 本身自己含有同类属性，则输出是 requirements， 输入时 node 的自身要求的属性，例如子查询中的
 * limit 中的 order 属性
 *
 * 对于某些会提供额外列的 node， 例如 project，则判断
 *
 */
std::vector<std::pair<std::shared_ptr<PropertySet>, std::vector<std::shared_ptr<PropertySet>>>>
ChildPropertyDeriver::GetProperties(Memo *memo, const std::shared_ptr<PropertySet> &requirements,
                                    GroupExpression *gexpr) {
  const auto &op = gexpr->Pop();
  switch (op->kind) {
    case OperatorType::PhysicalScan:
      return {{std::make_shared<PropertySet>(), std::vector<std::shared_ptr<PropertySet>>{}}};

    // limit 提供 order 属性
    case OperatorType::PhysicalLimit: {
      const auto &limit = op->Cast<PhysicalLimit>();

      auto property_set = std::make_shared<PropertySet>();
      property_set->AddProperty(std::make_shared<PropertySort>(limit.order_spec->Copy()));
      return {{requirements->Copy(), {property_set->Copy()}}};
    }
    // 属性下推到子节点
    case OperatorType::PhysicalComputeScalar: {
      if (const auto &sort = requirements->GetPropertyOfType(PropertyType::SORT); sort != nullptr) {
        auto sort_spec = sort->As<PropertySort>()->GetSortSpec();
        auto pcrs_sort = sort_spec->GetUsedColumns();

        if (!ColRefSetIsDisjoint(pcrs_sort, gexpr->GetGroup()->GroupProperties()->GetDefinedColumns())) {
          // if required order uses any column defined by ComputeScalar, we cannot
          // request it from child, and we pass an empty order spec;
          // order enforcer function takes care of enforcing this order on top of
          // ComputeScalar operator
          return {{std::make_shared<PropertySet>(),
                   std::vector<std::shared_ptr<PropertySet>>{std::make_shared<PropertySet>()}}};
        }
      }

      // otherwise, we pass through required order
      return {{requirements->Copy(), std::vector<std::shared_ptr<PropertySet>>{requirements->Copy()}}};
    }
    case OperatorType::PhysicalFilter:
      return {{requirements->Copy(), std::vector<std::shared_ptr<PropertySet>>{requirements->Copy()}}};

    case OperatorType::PhysicalStreamAgg: {
      const auto &agg = gexpr->Pop()->Cast<PhysicalStreamAgg>();
      if (const auto &sort = requirements->GetPropertyOfType(PropertyType::SORT); sort != nullptr) {
        auto sort_spec = sort->As<PropertySort>()->GetSortSpec();
        auto pos = PhysicalStreamAgg::PosCovering(sort_spec, agg.group_columns);
        if (nullptr == pos) {
          // failed to find a covering order spec, use local order spec
          pos = agg.order_spec;
        }

        auto property_set = std::make_shared<PropertySet>();
        property_set->AddProperty(
            std::make_shared<PropertySort>(pos == nullptr ? agg.order_spec->Copy() : pos->Copy()));
        return {{requirements->Copy(), {property_set}}};
      }

      auto property_set = std::make_shared<PropertySet>();
      property_set->AddProperty(std::make_shared<PropertySort>(agg.order_spec->Copy()));
      return {{std::make_shared<PropertySet>(), std::vector<std::shared_ptr<PropertySet>>{property_set}}};
    }

    // 属性下推到 left
    // apply right 是子查询，可以拥有自我属性，上层属性是否传导到下层后续考虑，当前不考虑
    case OperatorType::PhysicalNLJoin:
    case OperatorType::PhysicalApply: {
      if (const auto &sort = requirements->GetPropertyOfType(PropertyType::SORT); sort != nullptr) {
        auto sort_spec = sort->As<PropertySort>()->GetSortSpec();
        // propagate the order requirement to the outer child only if all the columns
        // specified by the order requirement come from the outer child
        auto pcrs = sort_spec->GetUsedColumns();
        bool f_outer_sort_cols = ContainsAll(gexpr->GetChildGroup()[0]->GroupProperties()->GetOutputColumns(), pcrs);
        if (f_outer_sort_cols) {
          return {{requirements->Copy(), {requirements->Copy(), std::make_shared<PropertySet>()}}};
        }
        return {{requirements->Copy(), {std::make_shared<PropertySet>(), std::make_shared<PropertySet>()}}};
      }
      return {{requirements->Copy(), {requirements->Copy(), std::make_shared<PropertySet>()}}};
    }

    case OperatorType::PhysicalHashAgg:
    case OperatorType::PhysicalScalarAgg:
      return {{std::make_shared<PropertySet>(),
               std::vector<std::shared_ptr<PropertySet>>{std::make_shared<PropertySet>()}}};

    default:
      throw OptException("Unsupported operator type:");
  }
  return {};
}

}  // namespace pgp