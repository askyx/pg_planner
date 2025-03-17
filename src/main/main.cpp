#include "main/main.h"

#include "common/config.h"
#include "common/exception.h"
#include "pg_optimizer/optimizer.h"

extern "C" {
#include "nodes/nodes.h"
}
static planner_hook_type prev_planner_hook = nullptr;
static ExplainOneQuery_hook_type prev_explain_hook = nullptr;

#undef OPTIMIZERDOMAIN
#define OPTIMIZERDOMAIN PG_TEXTDOMAIN("planner")

#define my_ereport_domain(elevel, ...)      \
  do {                                      \
    const int elevel_ = (elevel);           \
    pg_prevent_errno_in_scope();            \
    if (errstart(elevel_, OPTIMIZERDOMAIN)) \
      __VA_ARGS__;                          \
    if (elevel_ >= ERROR)                   \
      pg_unreachable();                     \
  } while (0)

namespace pgp {

static bool explain = false;

static PlannedStmt *PgPlanner(Query *parse, const char *query_string, int cursor_options, ParamListInfo bound_params) {
  if (OPT_CONFIG(enable_optimizer)) {
    try {
      if (parse->commandType == CMD_SELECT) {
        auto *query = copyObject(parse);

        Optimizer optimizer{query};

        explain = true;
        return optimizer.Planner();
      }
    } catch (const pgp::Exception &e) {
      my_ereport_domain(WARNING, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("%s", e.what()),
                                  errfinish(e.filename, e.line, e.function)));
    } catch (...) {
      elog(WARNING, "pg_planner Failed to plan query, get unknown error");
    }
  }

  explain = false;

  if (prev_planner_hook != nullptr)
    return prev_planner_hook(parse, query_string, cursor_options, bound_params);

  return standard_planner(parse, query_string, cursor_options, bound_params);
}

static void ExplainOneQuery(Query *query, int cursor_options, IntoClause *into, ExplainState *es,
                            const char *query_string, ParamListInfo params, QueryEnvironment *query_env) {
  prev_explain_hook(query, cursor_options, into, es, query_string, params, query_env);
  if (explain && OPT_CONFIG(enable_optimizer)) {
    ExplainPropertyText("Optimizer", "PGP", es);
  }
}

void DefinePlannerHooks() {
  prev_planner_hook = planner_hook;
  planner_hook = pgp::PgPlanner;

  prev_explain_hook = (ExplainOneQuery_hook != nullptr) ? ExplainOneQuery_hook : standard_ExplainOneQuery;
  ExplainOneQuery_hook = pgp::ExplainOneQuery;
}

void DefinePlannerGuc() {
  // clang-format off
  DefineCustomBoolVariable(
    "pg_planner.enable_planner",
    "use PGP planner.",
    nullptr,
    &OPT_CONFIG(enable_optimizer),
    false,
    PGC_SUSET,
    0,
    nullptr,
    nullptr,
    nullptr
  );

  DefineCustomBoolVariable(
    "pg_planner.test_flag",
    "use PGP planner.",
    nullptr,
    &OPT_CONFIG(enable_test_flag),
    false,
    PGC_SUSET,
    0,
    nullptr,
    nullptr,
    nullptr
  );

  DefineCustomIntVariable(
    "pg_planner.task_execution_timeout",
    "Sets the interval between dumps of shared buffers",
    "If set to zero, time-based dumping is disabled.",
    &OPT_CONFIG(task_execution_timeout),
    6000,
    0, INT_MAX / 1000,
    PGC_SUSET,
    GUC_UNIT_S,
    nullptr,
	  nullptr,  
	  nullptr
  );

  // clang-format on
}

}  // namespace pgp