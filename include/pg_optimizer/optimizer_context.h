#pragma once

#include "common/exception.h"
#include "pg_catalog/catalog.h"
#include "pg_optimizer/colref_pool.h"
#include "pg_optimizer/group.h"
#include "pg_optimizer/group_expression.h"
#include "pg_optimizer/memo.h"
#include "pg_optimizer/optimizer_task.h"
#include "postgres_ext.h"

namespace pgp {

class OptimizerContext {
 public:
  Memo memo;
  RuleSet rule_set;
  Catalog catalog_accessor;
  OptimizerTaskStack task_stack;
  ColRefPool column_factory;
  RelationInfoMap relation_info;

  static OptimizerContext *optimizer_context;

  OptimizerContext() = default;

  static OptimizerContext *GetOptimizerContextInstance() { return optimizer_context; }

  GroupExpression *MakeGroupExpression(const OperatorNodePtr &node);

  bool RecordOptimizerNodeIntoGroup(const OperatorNodePtr &node, GroupExpression **gexpr) {
    return RecordOptimizerNodeIntoGroup(node, gexpr, nullptr);
  }

  bool RecordOptimizerNodeIntoGroup(const OperatorNodePtr &node, GroupExpression **gexpr, Group *target_group) {
    auto *new_gexpr = MakeGroupExpression(node);
    auto *ptr = memo.InsertExpression(node, new_gexpr, target_group, false);
    PGP_ASSERT(ptr, "Root of expr should not fail insertion");

    (*gexpr) = ptr;
    return (ptr == new_gexpr);
  }

  void PushTask(OptimizerTask *task) { task_stack.Push(task); }
};

}  // namespace pgp