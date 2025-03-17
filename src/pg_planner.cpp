#include <memory_resource>

extern "C" {
#include <postgres.h>
#include <fmgr.h>

#include <access/htup_details.h>
#include <nodes/pg_list.h>
#include <tcop/tcopprot.h>
#include <utils/builtins.h>

#include <server/funcapi.h>
}

#include "common/formator.h"
#include "main/main.h"
#include "pg_optimizer/optimizer.h"
#include "pg_test.h"

extern "C" {

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(transform_query);
Datum transform_query(PG_FUNCTION_ARGS) {  // NOLINT
  char *transformed_query_tree = nullptr;

  char *sql = text_to_cstring(PG_GETARG_TEXT_PP(0));

  List *parsetree_list = pg_parse_query(sql);

  foreach_node(RawStmt, parsetree, parsetree_list) {
    List *querytree_list = pg_analyze_and_rewrite_fixedparams(parsetree, sql, nullptr, 0, nullptr);
    foreach_node(Query, query, querytree_list) {
      if (query->commandType == CMD_UTILITY)
        elog(ERROR, "could not transform untility query");

      pgp::Optimizer optimizer{query};

      auto op_tree = optimizer.QueryToOperator();
      auto tree = pgp::Format<pgp::OperatorNode>::ToString(op_tree.expr);
      if (transformed_query_tree == nullptr)
        transformed_query_tree = psprintf("%s", pstrdup(tree.c_str()));
      else
        transformed_query_tree = psprintf("%s\n%s", transformed_query_tree, pstrdup(tree.c_str()));
    }
  }

  PG_RETURN_CSTRING(transformed_query_tree);
}

PG_FUNCTION_INFO_V1(planner_test);
Datum planner_test(PG_FUNCTION_ARGS) {
  auto *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

  Datum values[3];
  bool nulls[4] = {false, false, false};

  InitMaterializedSRF(fcinfo, 0);

  foreach_node(pg_test::PGTestSuite, test, pg_test::PGTest::PGTestInstance().GetTestSuites()) {
    test->Test();
    const auto &statu = test->Status();
    values[0] = PointerGetDatum(cstring_to_text(pstrdup(statu.name)));
    values[1] = PointerGetDatum(cstring_to_text(pstrdup(statu.status)));
    if (statu.success)
      nulls[2] = true;
    else {
      nulls[2] = false;
      values[2] = PointerGetDatum(cstring_to_text(psprintf("%s:%d (%s)", statu.location, statu.line, statu.details)));
    }
    tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
    fflush(stdout);
  }
  return 0;
}

void _PG_init(void) {
  (void)std::pmr::new_delete_resource();
  pgp::DefinePlannerGuc();
  pgp::DefinePlannerHooks();
}
}