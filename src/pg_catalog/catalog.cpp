#include "pg_catalog/catalog.h"

#include <cstdlib>
#include <cstring>
#include <vector>

#include "nodes/pg_list.h"

extern "C" {
#include <access/htup_details.h>
#include <catalog/pg_aggregate.h>
#include <catalog/pg_am_d.h>
#include <parser/parse_coerce.h>
#include <parser/parse_type.h>
#include <postgres_ext.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/syscache.h>

#include "access/table.h"
}

namespace pgp {

char *Catalog::GetFuncName(Oid func_oid) {
  return get_func_name(func_oid);
}
char *Catalog::GetOperName(Oid func_oid) {
  return get_opname(func_oid);
}

Oid Catalog::GetFuncRetType(Oid func_oid) {
  return get_func_rettype(func_oid);
}

bool Catalog::GetFuncRetSet(Oid func_oid) {
  return get_func_retset(func_oid);
}

bool Catalog::FuncStrict(Oid func_oid) {
  return func_strict(func_oid);
}

Oid Catalog::GetCastFuncId(Oid src_type, Oid dest_type) {
  if (IsBinaryCoercible(src_type, dest_type)) {
    return 0;
  }

  Oid cast_fn_oid;
  find_coercion_pathway(dest_type, src_type, COERCION_IMPLICIT, &cast_fn_oid);
  return cast_fn_oid;
}

Oid Catalog::GetInverseOp(Oid op_oid) {
  return get_negator(op_oid);
}

bool Catalog::OpStrict(Oid op_oid) {
  return op_strict(op_oid);
}

Oid Catalog::GetOpCode(Oid op_oid) {
  return get_opcode(op_oid);
}

Oid Catalog::GetAggSortOp(Oid aggfnoid) {
  HeapTuple agg_tuple;
  Form_pg_aggregate aggform;
  Oid aggsortop;

  /* fetch aggregate entry from pg_aggregate */
  agg_tuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggfnoid));
  if (!HeapTupleIsValid(agg_tuple))
    return InvalidOid;
  aggform = (Form_pg_aggregate)GETSTRUCT(agg_tuple);
  aggsortop = aggform->aggsortop;
  ReleaseSysCache(agg_tuple);

  return aggsortop;
}

bool Catalog::TypeByVal(Oid type_oid) {
  auto *base_type = typeidType(type_oid);
  bool is_passed_by_value = typeByVal(base_type);
  ReleaseSysCache(base_type);
  return is_passed_by_value;
}

Oid Catalog::GetAggTranstype(Oid aggfnoid) {
  HeapTuple tp;
  Oid result;

  tp = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggfnoid));
  if (!HeapTupleIsValid(tp))
    elog(ERROR, "cache lookup failed for aggregate %u", aggfnoid);

  result = ((Form_pg_aggregate)GETSTRUCT(tp))->aggtranstype;
  ReleaseSysCache(tp);
  return result;
}

std::vector<Oid> Catalog::RelationGetIndexList(Oid rel_oid) {
  auto *relation = table_open(rel_oid, NoLock);
  auto *indexlist = ::RelationGetIndexList(relation);
  std::vector<Oid> index_oids;
  foreach_oid(index, indexlist) index_oids.emplace_back(index);

  table_close(relation, NoLock);
  list_free(indexlist);

  return index_oids;
}

}  // namespace pgp