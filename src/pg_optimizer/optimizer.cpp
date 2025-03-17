#include "pg_optimizer/optimizer.h"

#include <cstdint>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/formator.h"
#include "common/macros.h"
#include "common/scope_timer.h"
#include "main/from_pg_query.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "pg_operator/logical_operator.h"
#include "pg_operator/normalizer.h"
#include "pg_operator/operator.h"
#include "pg_operator/operator_node.h"
#include "pg_optimizer/colref.h"
#include "pg_optimizer/group.h"
#include "pg_optimizer/optimization_context.h"
#include "pg_optimizer/optimizer_task.h"
#include "pg_optimizer/plan_generator.h"
#include "pg_optimizer/property.h"
#include "pg_optimizer/property_deriver.h"

extern "C" {
#include <miscadmin.h>
#include <nodes/plannodes.h>
#include <tcop/tcopprot.h>
#include <utils/fmgroids.h>
#include <utils/guc.h>
}

namespace pgp {

Optimizer::Optimizer(Query *query) : query(query) {
  OptimizerContext::optimizer_context = &context;
}

PlannedStmt *Optimizer::Planner() {
  try {
    auto query_info = QueryToOperator().Normalizer();

    auto *root = OptimizePlan(query_info);
    if (root == nullptr) {
      throw OptException("OptimizePlan get error");
    }
    return GeneratePlan(query_info, root);

  } catch (pgp::Exception &ex) {
    throw ex;
  } catch (...) {
  }

  UNREACHABLE;
}

QueryInfo &QueryInfo::Normalizer() {
  expr = Normalizer::NormalizerTree(expr);

  OLOG("query after normalization:\n{}", Format<OperatorNode>::ToString(expr));

  return *this;
}

QueryInfo Optimizer::QueryToOperator() {
  TranslatorQuery transformer{&context, query};

  auto *tree = transformer.TranslateQuery();

  OLOG("query from query:\n{}\n{}", debug_query_string, Format<OperatorNode>::ToString(tree));

  std::vector<std::string> output_col_names;

  foreach_node(TargetEntry, target, query->targetList) {
    if (target->resname == nullptr)
      output_col_names.emplace_back("?column?");
    else
      output_col_names.emplace_back(target->resname);
  }

  return {.expr = tree,
          .output_col_names = std::move(output_col_names),
          .output_array = transformer.GetQueryOutputCols(),
          .properties = transformer.GetPropertySet()};
}

Group *Optimizer::OptimizePlan(QueryInfo &query_info) {
  GroupExpression *gexpr = nullptr;
  [[maybe_unused]] auto insert = context.RecordOptimizerNodeIntoGroup(query_info.expr, &gexpr);
  PGP_ASSERT(insert, "failed to insert into group");
  auto *root_group = gexpr->GetGroup();
  auto *root_context = new OptimizationContext(query_info.properties);

  uint64_t elapsed_time = 0;

  OLOGLN("Memo before Optimize:\n{}\n{}", context.GetMemo().ToString(),
         root_context->GetRequiredProperties()->ToString());

  auto &task_stack = context.GetTaskStack();

  root_context->SetGlobalOptimizerContext(&context);

  task_stack.Push(new OptimizeGroup(root_group, root_context));

  // Iterate through the task stack
  while (!task_stack.Empty()) {
    CHECK_FOR_INTERRUPTS();
    // Check to see if we have at least one plan, and if we have exceeded our
    // timeout limit
    if (elapsed_time >= OPT_CONFIG(task_execution_timeout) &&
        root_group->HasExpressions(root_context->GetRequiredProperties())) {
      throw OptException("Optimizer task execution timed out");
    }

    uint64_t task_runtime = 0;
    auto *task = task_stack.Pop();
    {
      ScopedTimer<std::chrono::seconds> timer(&task_runtime);
      task->Execute();
    }
    delete task;
    elapsed_time += task_runtime;
  }

  OLOG("Memo after Optimize:\n{}", context.GetMemo().ToString());

  return root_group;
}

PlanMeta GetPlanTrere(Group *root, const std::shared_ptr<PropertySet> &prop, const ColRefArray &output_array,
                      PlanGenerator &generator) {
  auto *gexpr = root->GetBestExpression(prop);
  PGP_ASSERT(gexpr, "no plan found");

  // drive output col
  auto xx = PropertiesDriver::PcrsRequired(gexpr, output_array);

  std::vector<PlanMeta> children_metas;

  auto required_inputs = gexpr->GetInputProperties(prop);

  ColRefSet upper_res_cols;
  if (gexpr->Pop()->kind == OperatorType::PhysicalApply) {
    auto *right_child = gexpr->GetChildGroup()[1];
    upper_res_cols = right_child->GroupProperties()->GetOuterReferences();
  }

  for (auto i = 0; i < gexpr->GetChildGroup().size(); i++) {
    auto *child = gexpr->GetChildGroup()[i];
    auto child_prop = required_inputs[i];

    if (i == 1 && !upper_res_cols.empty())
      generator.upper_res_cols = upper_res_cols;
    auto childt = GetPlanTrere(child, child_prop, xx[i], generator);
    children_metas.emplace_back(std::move(childt));
  }

  return generator.BuildPlan(gexpr, output_array, children_metas);
}

// TODO: simple code
PlannedStmt *Optimizer::GeneratePlan(QueryInfo &query_info, Group *root) {
  PlanGenerator generator;

  auto physical_plan = GetPlanTrere(root, query_info.properties, query_info.output_array, generator);

  return generator.BuildStmt(physical_plan, query_info.output_col_names);
}

}  // namespace pgp