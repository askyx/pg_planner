#pragma once

#include <unordered_map>
#include <vector>

#include "common/exception.h"
#include "pg_catalog/catalog.h"
#include "pg_optimizer/colref_pool.h"
#include "pg_optimizer/group.h"
#include "pg_optimizer/group_expression.h"
#include "pg_optimizer/memo.h"
#include "pg_optimizer/optimizer_task.h"
#include "postgres_ext.h"
extern "C" {
#include "nodes/parsenodes.h"
}

namespace pgp {

class OptimizerContext {
 public:
  static OptimizerContext *optimizer_context;

  OptimizerContext() = default;

  static OptimizerContext *GetOptimizerContextInstance() { return optimizer_context; }

  Memo &GetMemo() { return memo_; }

  RuleSet &GetRuleSet() { return rule_set_; }

  Catalog &GetCatalogAccessor() { return catalog_accessor_; }

  ColRefPool &GetColumnFactory() { return column_factory_; }

  GroupExpression *MakeGroupExpression(OperatorNode *node);

  bool RecordOptimizerNodeIntoGroup(OperatorNode *node, GroupExpression **gexpr) {
    return RecordOptimizerNodeIntoGroup(node, gexpr, nullptr);
  }

  bool RecordOptimizerNodeIntoGroup(OperatorNode *node, GroupExpression **gexpr, Group *target_group) {
    auto *new_gexpr = MakeGroupExpression(node);
    auto *ptr = memo_.InsertExpression(node, new_gexpr, target_group, false);
    PGP_ASSERT(ptr, "Root of expr should not fail insertion");

    (*gexpr) = ptr;
    return (ptr == new_gexpr);
  }

  OptimizerTaskStack &GetTaskStack() { return task_stack_; }

  void PushTask(OptimizerTask *task) { task_stack_.Push(task); }

 private:
  Memo memo_;
  RuleSet rule_set_;
  Catalog catalog_accessor_;
  OptimizerTaskStack task_stack_;
  ColRefPool column_factory_;
};

}  // namespace pgp