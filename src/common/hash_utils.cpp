#include <cstddef>

#include "common/hash_util.h"

extern "C" {
#include <nodes/bitmapset.h>
#include <nodes/pg_list.h>
#include <nodes/primnodes.h>
#include <utils/datum.h>
}

namespace pgp {

#define HASH_SCALAR_FIELD(fldname) hash = HashUtil::CombineHashes(hash, HashUtil::Hash(node->fldname))

#define HASH_NODE_FIELD(fldname) hash = HashUtil::CombineHashes(hash, HashUtil::HashNode(node->fldname))

#define HASH_BITMAPSET_FIELD(fldname) hash = HashUtil::CombineHashes(hash, BmsHash(node->fldname))

/* Hash a field that is a pointer to a C string, or perhaps nullptr */
#define HASH_STRING_FIELD(fldname) \
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(newnode->fldname = from->fldname ? node->fldname : 0))

//
// #define HASH_ARRAY_FIELD(fldname) std::memcpy(newnode->fldname, from->fldname, sizeof(newnode->fldname))

/* Hash a field that is a pointer to a simple palloc'd object of size sz */
#define HASH_POINTER_FIELD(fldname, sz) hash = HashUtil::CombineHashes(hash, HashUtil::HashBytes(node->fldname, sz))

#define BITMAPSET_SIZE(nwords) (offsetof(Bitmapset, words) + (nwords) * sizeof(bitmapword))

static hash_t BmsHash(const Bitmapset* a) {
  if (a == nullptr)
    return 0;

  auto size = BITMAPSET_SIZE(a->nwords);
  return HashUtil::HashBytes(reinterpret_cast<const std::byte*>(a), size);
}

#define BEGIN_HASH(TYPE)      \
  case T_##TYPE: {            \
    auto* node = (TYPE*)expr; \
    auto hash = HashUtil::Hash(node->xpr.type);

#define END_HASH \
  return hash;   \
  }

/*
 * Hash an expression tree.
 * CHILDREN will be processed by AbstractExpression, not here.
 */
hash_t HashUtil::HashNode(const void* expr) {
  if (expr == nullptr)
    return 0;

  if (IsA(expr, List) || IsA(expr, IntList) || IsA(expr, OidList) || IsA(expr, XidList)) {
    auto* node = (List*)expr;
    auto hash = HashUtil::Hash(node->type);
    const ListCell* lc;
    foreach (lc, node) {
      if (IsA(node, List))
        hash = HashUtil::CombineHashes(hash, HashUtil::HashNode(lfirst(lc)));
      else
        hash = HashUtil::CombineHashes(hash, HashUtil::Hash(lfirst_int(lc)));
    }
  }

  switch (nodeTag(expr)) {
    BEGIN_HASH(Var) {
      HASH_SCALAR_FIELD(varno);
      HASH_SCALAR_FIELD(varattno);
      HASH_SCALAR_FIELD(vartype);
      HASH_SCALAR_FIELD(vartypmod);
      HASH_SCALAR_FIELD(varcollid);
      HASH_BITMAPSET_FIELD(varnullingrels);
      HASH_SCALAR_FIELD(varlevelsup);
    }
    END_HASH

    BEGIN_HASH(ArrayExpr) {
      HASH_SCALAR_FIELD(array_typeid);
      HASH_SCALAR_FIELD(array_collid);
      HASH_SCALAR_FIELD(element_typeid);
      HASH_NODE_FIELD(elements);
      HASH_SCALAR_FIELD(multidims);
    }
    END_HASH

    BEGIN_HASH(Const) {
      HASH_SCALAR_FIELD(consttype);
      HASH_SCALAR_FIELD(consttypmod);
      HASH_SCALAR_FIELD(constcollid);
      HASH_SCALAR_FIELD(constlen);
      HASH_SCALAR_FIELD(constisnull);
      HASH_SCALAR_FIELD(constbyval);

      if (node->constisnull)
        hash = HashUtil::CombineHashes(hash, 0);
      else {
        if (node->constbyval)
          hash = HashUtil::CombineHashes(hash, HashUtil::Hash((node->constvalue)));
        else {
          auto size = datumGetSize(node->constvalue, node->constbyval, node->constlen);
          hash =
              HashUtil::CombineHashes(hash, HashUtil::HashBytes((std::byte*)DatumGetPointer(node->constvalue), size));
        }
      }
    }
    END_HASH

    BEGIN_HASH(ScalarArrayOpExpr) {
      HASH_SCALAR_FIELD(opno);
      HASH_SCALAR_FIELD(opfuncid);
      HASH_SCALAR_FIELD(hashfuncid);
      HASH_SCALAR_FIELD(negfuncid);
      HASH_SCALAR_FIELD(useOr);
      HASH_SCALAR_FIELD(inputcollid);
      HASH_NODE_FIELD(args);
    }
    END_HASH

    BEGIN_HASH(NullTest) {
      HASH_NODE_FIELD(arg);
      HASH_SCALAR_FIELD(nulltesttype);
      HASH_SCALAR_FIELD(argisrow);
    }
    END_HASH

    BEGIN_HASH(CoerceViaIO) {
      HASH_NODE_FIELD(arg);
      HASH_SCALAR_FIELD(resulttype);
      HASH_SCALAR_FIELD(resultcollid);
      HASH_SCALAR_FIELD(coerceformat);
    }
    END_HASH

    BEGIN_HASH(RelabelType) {
      HASH_NODE_FIELD(arg);
      HASH_SCALAR_FIELD(resulttype);
      HASH_SCALAR_FIELD(resulttypmod);
      HASH_SCALAR_FIELD(resultcollid);
      HASH_SCALAR_FIELD(relabelformat);
    }
    END_HASH

    BEGIN_HASH(Param) {
      HASH_SCALAR_FIELD(paramkind);
      HASH_SCALAR_FIELD(paramid);
      HASH_SCALAR_FIELD(paramtype);
      HASH_SCALAR_FIELD(paramtypmod);
      HASH_SCALAR_FIELD(paramcollid);
    }
    END_HASH

    BEGIN_HASH(OpExpr) {
      HASH_SCALAR_FIELD(opno);
      HASH_SCALAR_FIELD(opfuncid);
      HASH_SCALAR_FIELD(opresulttype);
      HASH_SCALAR_FIELD(opretset);
      HASH_SCALAR_FIELD(opcollid);
      HASH_SCALAR_FIELD(inputcollid);
      HASH_NODE_FIELD(args);
    }
    END_HASH

    BEGIN_HASH(FuncExpr) {
      HASH_SCALAR_FIELD(funcid);
      HASH_SCALAR_FIELD(funcresulttype);
      HASH_SCALAR_FIELD(funcretset);
      HASH_SCALAR_FIELD(funcvariadic);
      HASH_SCALAR_FIELD(funcformat);
      HASH_SCALAR_FIELD(funccollid);
      HASH_SCALAR_FIELD(inputcollid);
      HASH_NODE_FIELD(args);
    }
    END_HASH

    BEGIN_HASH(CaseExpr) {
      HASH_SCALAR_FIELD(casetype);
      HASH_SCALAR_FIELD(casecollid);
      HASH_NODE_FIELD(arg);
      HASH_NODE_FIELD(args);
      HASH_NODE_FIELD(defresult);
    }
    END_HASH

    BEGIN_HASH(CaseWhen) {
      HASH_NODE_FIELD(expr);
      HASH_NODE_FIELD(result);
    }
    END_HASH

    BEGIN_HASH(CaseTestExpr) {
      HASH_SCALAR_FIELD(typeId);
      HASH_SCALAR_FIELD(typeMod);
      HASH_SCALAR_FIELD(collation);
    }
    END_HASH

    BEGIN_HASH(BoolExpr) {
      HASH_SCALAR_FIELD(boolop);
      HASH_NODE_FIELD(args);
    }
    END_HASH

    case T_Invalid:
    default:
      return 0;
  }
}

}  // namespace pgp