#pragma once

#include <cstdint>
#include <memory>

#include "common/hash_util.h"
#include "pg_optimizer/colref.h"
#include "postgres_ext.h"

extern "C" {
#include <c.h>

#include <catalog/pg_type_d.h>
#include <nodes/primnodes.h>
#include <utils/lsyscache.h>
}

struct SortGroupClause;

namespace pgp {
struct ItemExpr;

enum class ExpressionKind {
  Invalid,

  EopScalarCmp,
  EopScalarIsDistinctFrom,
  EopScalarIdent,
  EopScalarParam,
  EopScalarProjectElement,
  EopScalarConst,
  EopScalarBoolOp,
  EopScalarFunc,
  EopScalarAggFunc,
  EopScalarOp,
  NullTest,
  CaseExpr,
  CaseTestExpr,
  EopScalarCast,
  EopScalarCoerceToDomain,
  EopScalarCoerceViaIO,
  EopScalarArrayCoerceExpr,
  EopScalarCoalesce,
  EopScalarArray,
  EopScalarArrayCmp,
  EopScalarSortGroupClause,
};

using ItemExprPtr = std::shared_ptr<ItemExpr>;
using ExprArray = std::vector<ItemExprPtr>;

struct ItemExpr {
  ExpressionKind kind;

  ExprArray children;

  DISALLOW_COPY(ItemExpr)

  explicit ItemExpr(ExpressionKind kind) : kind(kind){};

  virtual ~ItemExpr() = default;

  virtual Oid ExprReturnType() const = 0;

  ColRefSet DeriveUsedColumns();

  virtual hash_t Hash() const;

  virtual bool operator==(const ItemExpr &other) const;

  bool operator!=(const ItemExpr &other) const { return !(*this == other); }

  void AddChild(ItemExprPtr child) { children.emplace_back(std::move(child)); }

  const ItemExprPtr &GetChild(int index) const { return children[index]; }

  const ExprArray &GetChildren() const { return children; }

  std::string ToString() const;

  template <class TARGET>
  TARGET &Cast() {
    if (TARGET::TYPE != ExpressionKind::Invalid && kind != TARGET::TYPE) {
      throw std::runtime_error("Failed to cast itemexpr to type - itemexpr type mismatch");
    }
    return reinterpret_cast<TARGET &>(*this);
  }

  template <class TARGET>
  const TARGET &Cast() const {
    if (TARGET::TYPE != ExpressionKind::Invalid && kind != TARGET::TYPE) {
      throw std::runtime_error("Failed to cast itemexpr to type - itemexpr type mismatch");
    }
    return reinterpret_cast<const TARGET &>(*this);
  }

  template <class TARGET>
  bool NodeIs() const {
    return kind == TARGET::TYPE;
  }
};

struct ItemAggref : public ItemExpr {
  constexpr static ExpressionKind TYPE = ExpressionKind::EopScalarAggFunc;

  ItemAggref(Oid aggfnoid, Oid aggtype, bool distinct, char aggkind, List *argtypes)
      : ItemExpr(ExpressionKind::EopScalarAggFunc),
        aggfnoid(aggfnoid),
        aggtype(aggtype),
        distinct(distinct),
        aggkind(aggkind),
        argtypes(argtypes) {}

  Oid aggfnoid;
  Oid aggtype;
  bool distinct;
  char aggkind;
  List *argtypes;

  ExprArray aggdirectargs;
  ExprArray aggorder;
  ExprArray aggdistinct;

  Aggref *ToAggref() const;

  hash_t Hash() const override;

  bool operator==(const ItemExpr &other) const override;

  Oid ExprReturnType() const override { return aggtype; }
};

struct ItemConst : public ItemExpr {
  constexpr static ExpressionKind TYPE = ExpressionKind::EopScalarConst;

  Const *value;

  explicit ItemConst(Const *datum) : ItemExpr(ExpressionKind::EopScalarConst), value(datum) {}

  hash_t Hash() const override;

  bool operator==(const ItemExpr &other) const override;

  Oid ExprReturnType() const override { return value->consttype; }
};

struct ItemArrayExpr : public ItemExpr {
  constexpr static ExpressionKind TYPE = ExpressionKind::EopScalarArray;

  Oid element_typeid;
  Oid array_typeid;
  bool multidims;

  ItemArrayExpr(Oid element_typeid, Oid array_typeid, bool multidims)
      : ItemExpr(ExpressionKind::EopScalarArray),
        element_typeid(element_typeid),
        array_typeid(array_typeid),
        multidims(multidims) {}

  ArrayExpr *ToArrayExpr() const;

  hash_t Hash() const override;

  bool operator==(const ItemExpr &other) const override;

  Oid ExprReturnType() const override { return array_typeid; }
};

struct ItemArrayCmp : public ItemExpr {
  constexpr static ExpressionKind TYPE = ExpressionKind::EopScalarArrayCmp;

  Oid opno;
  Oid opfuncid;
  bool use_or;
  bool op_strict;

  ItemArrayCmp(Oid opno, Oid opfuncid, bool use_or)
      : ItemExpr(ExpressionKind::EopScalarArrayCmp), opno(opno), opfuncid(opfuncid), use_or(use_or) {}

  ScalarArrayOpExpr *ToScalarArrayOpExpr() const;

  hash_t Hash() const override;

  bool operator==(const ItemExpr &other) const override;

  Oid ExprReturnType() const override { return BOOLOID; }
};

struct ItemBoolExpr : public ItemExpr {
  constexpr static ExpressionKind TYPE = ExpressionKind::EopScalarBoolOp;

  BoolExprType boolop;

  explicit ItemBoolExpr(BoolExprType eboolop) : ItemExpr(ExpressionKind::EopScalarBoolOp), boolop(eboolop) {}

  BoolExpr *ToBoolExpr() const;

  hash_t Hash() const override;

  bool operator==(const ItemExpr &other) const override;

  Oid ExprReturnType() const override { return BOOLOID; }
};

struct ItemCaseTest : public ItemExpr {
  constexpr static ExpressionKind TYPE = ExpressionKind::CaseTestExpr;

  Oid typeId;
  int32 typeMod;
  Oid collation;
  ItemCaseTest(Oid type_id, int32 type_mod, Oid collation)
      : ItemExpr(ExpressionKind::CaseTestExpr), typeId(type_id), typeMod(type_mod), collation(collation) {}

  CaseTestExpr *ToCaseTestExpr() const;

  hash_t Hash() const override;

  bool operator==(const ItemExpr &other) const override;

  Oid ExprReturnType() const override { return typeId; }
};

struct ItemCastExpr : public ItemExpr {
  constexpr static ExpressionKind TYPE = ExpressionKind::EopScalarCast;

  Oid resulttype;

  Oid funcid;

  ItemCastExpr(Oid resulttype, Oid funcid)
      : ItemExpr(ExpressionKind::EopScalarCast), resulttype(resulttype), funcid(funcid) {}

  hash_t Hash() const override;

  Expr *ToExpr() const;

  bool operator==(const ItemExpr &other) const override;

  Oid ExprReturnType() const override { return resulttype; }
};

struct ItemCmpExpr : public ItemExpr {
  constexpr static ExpressionKind TYPE = ExpressionKind::EopScalarCmp;

  Oid opno;

  // does operator return NULL on NULL input?
  bool op_strict;

  explicit ItemCmpExpr(Oid opno) : ItemExpr(ExpressionKind::EopScalarCmp), opno(opno) {}

  hash_t Hash() const override;

  bool operator==(const ItemExpr &other) const override;

  OpExpr *ToOpExpr() const;

  Oid ExprReturnType() const override { return BOOLOID; }
};

struct ItemIsDistinctFrom : public ItemCmpExpr {
  constexpr static ExpressionKind TYPE = ExpressionKind::EopScalarIsDistinctFrom;

  explicit ItemIsDistinctFrom(Oid mdid_op) : ItemCmpExpr(mdid_op) { kind = ExpressionKind::EopScalarIsDistinctFrom; }
};

struct ItemOpExpr : public ItemExpr {
  constexpr static ExpressionKind TYPE = ExpressionKind::EopScalarOp;

  Oid opno;

  Oid opresulttype;

  bool op_strict;

  ItemOpExpr(Oid opno, Oid opresulttype)
      : ItemExpr(ExpressionKind::EopScalarOp), opno(opno), opresulttype(opresulttype) {}

  OpExpr *ToOpExpr() const;

  hash_t Hash() const override;

  bool operator==(const ItemExpr &other) const override;

  Oid ExprReturnType() const override { return opresulttype != InvalidOid ? opresulttype : get_op_rettype(opno); }
};

struct ItemCoalesce : public ItemExpr {
  constexpr static ExpressionKind TYPE = ExpressionKind::EopScalarCoalesce;

  Oid coalescetype;

  explicit ItemCoalesce(Oid coalescetype) : ItemExpr(ExpressionKind::EopScalarCoalesce), coalescetype(coalescetype) {}

  hash_t Hash() const override;

  CoalesceExpr *ToCoalesceExpr() const;

  bool operator==(const ItemExpr &other) const override;

  Oid ExprReturnType() const override { return coalescetype; }
};

struct ItemFuncExpr : public ItemExpr {
  constexpr static ExpressionKind TYPE = ExpressionKind::EopScalarFunc;

  Oid funcid;

  Oid funcresulttype;

  bool funcvariadic;

  // does operator return NULL on NULL input?
  bool op_strict;

  ItemFuncExpr(Oid funcid, Oid funcresulttype, bool funcvariadic)
      : ItemExpr(ExpressionKind::EopScalarFunc),
        funcid(funcid),
        funcresulttype(funcresulttype),
        funcvariadic(funcvariadic) {}

  hash_t Hash() const override;

  bool operator==(const ItemExpr &other) const override;

  FuncExpr *ToFuncExpr() const;

  Oid ExprReturnType() const override { return funcresulttype; }
};

struct ItemIdent : public ItemExpr {
  constexpr static ExpressionKind TYPE = ExpressionKind::EopScalarIdent;

  ColRef *colref;

  explicit ItemIdent(ColRef *colref) : ItemExpr(ExpressionKind::EopScalarIdent), colref(colref) {}

  hash_t Hash() const override;

  bool operator==(const ItemExpr &other) const override;

  Oid ExprReturnType() const override { return colref->RetrieveType(); }
};

struct ItemCaseExpr : public ItemExpr {
  constexpr static ExpressionKind TYPE = ExpressionKind::CaseExpr;

  Oid casetype;

  Oid casecollid;

  bool case_arg_exist{false};

  bool default_arg_exist{false};

  explicit ItemCaseExpr(Oid casetype, Oid casecollid, bool case_arg_exist, bool default_arg_exist)
      : ItemExpr(ExpressionKind::CaseExpr),
        casetype(casetype),
        casecollid(casecollid),
        case_arg_exist(case_arg_exist),
        default_arg_exist(default_arg_exist) {}

  hash_t Hash() const override;

  bool operator==(const ItemExpr &other) const override;

  Oid ExprReturnType() const override { return casetype; }

  static ItemExprPtr FromPg(CaseExpr *case_expr);

  CaseExpr *ToCaseExpr() const;
};

struct ItemNullTest : public ItemExpr {
  constexpr static ExpressionKind TYPE = ExpressionKind::NullTest;

  ItemNullTest() : ItemExpr(ExpressionKind::NullTest) {}

  Oid ExprReturnType() const override { return BOOLOID; }
};

struct ItemParam : public ItemExpr {
  constexpr static ExpressionKind TYPE = ExpressionKind::EopScalarParam;

  int32_t paramid;

  Oid paramtype;

  int32_t paramtypmod;

  ItemParam(int32_t paramid, Oid paramtype, int32_t paramtypmod)
      : ItemExpr(ExpressionKind::EopScalarParam), paramid(paramid), paramtype(paramtype), paramtypmod(paramtypmod) {}

  hash_t Hash() const override { return HashUtil::Hash(paramid); }

  Oid ExprReturnType() const override { return paramtype; }

  bool operator==(const ItemExpr &other) const override;

  static ItemExprPtr FromPg(Param *param);
};

struct ItemProjectElement : public ItemExpr {
  constexpr static ExpressionKind TYPE = ExpressionKind::EopScalarProjectElement;

  ColRef *colref;

  explicit ItemProjectElement(ColRef *colref) : ItemExpr(ExpressionKind::EopScalarProjectElement), colref(colref) {}

  hash_t Hash() const override;

  bool operator==(const ItemExpr &other) const override;

  Oid ExprReturnType() const override { return colref->RetrieveType(); }
};

struct ItemSortGroupClause : public ItemExpr {
  constexpr static ExpressionKind TYPE = ExpressionKind::EopScalarSortGroupClause;

  SortGroupClause *expr;

  explicit ItemSortGroupClause(SortGroupClause *expr)
      : ItemExpr(ExpressionKind::EopScalarSortGroupClause), expr(expr) {}

  bool operator==(const ItemExpr &other) const override;

  Oid ExprReturnType() const override { return InvalidOid; }
};

struct ItemCoerceBase : public ItemExpr {
  constexpr static ExpressionKind TYPE = ExpressionKind::Invalid;

  Oid resulttype;

  int32_t resulttypmod;

  CoercionForm coerceformat;

  ItemCoerceBase(Oid resulttype, int32_t resulttypmod, CoercionForm coerceformat, ExpressionKind kind)
      : ItemExpr(kind), resulttype(resulttype), resulttypmod(resulttypmod), coerceformat(coerceformat) {}

  bool operator==(const ItemExpr &other) const override;

  Oid ExprReturnType() const override { return resulttype; }
};

struct ItemArrayCoerceExpr : public ItemCoerceBase {
  constexpr static ExpressionKind TYPE = ExpressionKind::EopScalarArrayCoerceExpr;

  ItemArrayCoerceExpr(Oid resulttype, int32_t resulttypmod, CoercionForm coerceformat)
      : ItemCoerceBase(resulttype, resulttypmod, coerceformat, ExpressionKind::EopScalarArrayCoerceExpr) {}
};

struct ItemCoerceViaIO : public ItemCoerceBase {
  constexpr static ExpressionKind TYPE = ExpressionKind::EopScalarCoerceViaIO;

  ItemCoerceViaIO(Oid resulttype, int32_t resulttypmod, CoercionForm coerceformat)
      : ItemCoerceBase(resulttype, resulttypmod, coerceformat, ExpressionKind::EopScalarCoerceViaIO) {}
};

struct ItemCoerceToDomain : public ItemCoerceBase {
  constexpr static ExpressionKind TYPE = ExpressionKind::EopScalarCoerceToDomain;

  ItemCoerceToDomain(Oid resulttype, int32_t resulttypmod, CoercionForm coerceformat)
      : ItemCoerceBase(resulttype, resulttypmod, coerceformat, ExpressionKind::EopScalarCoerceToDomain) {}
};

inline bool operator==(const ExprArray &p1, const ExprArray &p2) {
  if (p1.size() != p2.size())
    return false;

  for (auto [e1, e2] : std::views::zip(p1, p2))
    if (*e1 != *e2)
      return false;

  return true;
}

}  // namespace pgp