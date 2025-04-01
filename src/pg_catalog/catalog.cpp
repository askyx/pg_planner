#include "pg_catalog/catalog.h"

#include <cstdlib>
#include <cstring>
#include <utility>
#include <vector>

#include "common/defer.h"
#include "common/macros.h"
#include "pg_optimizer/colref.h"
#include "pg_optimizer/optimizer_context.h"

extern "C" {
#include <access/genam.h>
#include <access/htup_details.h>
#include <catalog/pg_aggregate.h>
#include <catalog/pg_am_d.h>
#include <nodes/pg_list.h>
#include <parser/parse_coerce.h>
#include <parser/parse_type.h>
#include <postgres_ext.h>
#include <storage/lockdefs.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/syscache.h>
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

// TODO: check for get_relation_info
// TODO: system cols
RelationInfoPtr Catalog::GetRelationInfo(Oid rel_oid) {
  auto *rel = RelationIdGetRelation(rel_oid);
  auto rel_close = pgp::ScopedDefer{[] {}, [&]() { RelationClose(rel); }};

  RelationInfoPtr relation_info = std::make_shared<RelationInfo>();

  auto *optimizer_context = OptimizerContext::GetOptimizerContextInstance();

  ColRefArray output_columns;
  for (int i = 0; i < RelationGetNumberOfAttributes(rel); i++) {
    Form_pg_attribute att = TupleDescAttr(rel->rd_att, i);
    auto item_width = get_attavgwidth(RelationGetRelid(rel), (AttrNumber)i);
    if (item_width <= 0) {
      item_width = get_typavgwidth(att->atttypid, att->atttypmod);
    }
    ColRef *colref = optimizer_context->column_factory.PcrCreate(att->atttypid, att->atttypmod, false, true,
                                                                 NameStr(att->attname), item_width);
    colref->attnum = att->attnum;
    output_columns.emplace_back(colref);
  }

  // TODO: check index is available
  // TODO: expression index
  // TODO: partition index
  // TODO: more index types
  std::vector<IndexInfo> index_info;
  if (auto *indexoidlist = RelationGetIndexList(rel); indexoidlist) {
    ColRefArray index_columns;
    ColRefArray index_include;
    std::vector<Oid> sortopfamily;
    std::vector<bool> reverse_sort;
    std::vector<bool> null_first;
    foreach_oid(ioid, indexoidlist) {
      auto *index_rel = index_open(ioid, NoLock);
      auto index_clos = pgp::ScopedDefer{[] {}, [&]() { index_close(index_rel, NoLock); }};
      Oid relam = index_rel->rd_rel->relam;
      auto *index = index_rel->rd_index;

      ColRefArray index_columns;
      ColRefArray index_include;
      std::vector<Oid> opcintype(index->indnkeyatts, InvalidOid);
      std::vector<Oid> sortopfamily(index->indnkeyatts, InvalidOid);
      std::vector<bool> reverse_sort(index->indnkeyatts, false);
      std::vector<bool> null_first(index->indnkeyatts, false);

      for (auto i = 0; i < index->indnatts; i++) {
        // attno indexed from 0
        auto attno = index->indkey.values[i];
        if (i < index->indnkeyatts) {
          index_columns.emplace_back(output_columns[attno - 1]);
          opcintype[i] = index_rel->rd_opcintype[i];
          sortopfamily[i] = index_rel->rd_opfamily[i];
        } else
          index_include.emplace_back(output_columns[attno - 1]);
      }
      if (index_rel->rd_rel->relkind != RELKIND_PARTITIONED_INDEX) {
        if (relam == BTREE_AM_OID) {
          for (auto i = 0; i < index->indnkeyatts; i++) {
            auto opt = index_rel->rd_indoption[i];
            reverse_sort[i] = (opt & INDOPTION_DESC) != 0;
            null_first[i] = (opt & INDOPTION_NULLS_FIRST) != 0;
          }
        }
      }
      index_info.emplace_back(ioid, relam, std::move(index_columns), std::move(index_include), std::move(sortopfamily),
                              std::move(opcintype), std::move(reverse_sort), std::move(null_first));
    }
  }

  relation_info->output_columns = std::move(output_columns);
  relation_info->index_list = std::move(index_info);

  OLOG("RelationInfo for rel_oid {}:\n{}", rel_oid, relation_info->ToString());

  optimizer_context->relation_info.insert({rel_oid, relation_info});

  return relation_info;
}

}  // namespace pgp