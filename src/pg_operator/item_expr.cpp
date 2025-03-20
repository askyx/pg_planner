#include "pg_operator/item_expr.h"

#include <format>
#include <string>

#include "common/hash_util.h"
#include "pg_catalog/catalog.h"
#include "pg_optimizer/colref.h"

extern "C" {
#include "catalog/pg_type_d.h"
#include "nodes/nodes.h"
#include "nodes/primnodes.h"
#include "postgres_ext.h"
#include "utils/lsyscache.h"
}
namespace pgp {

hash_t ItemExpr::Hash() const {
  hash_t hash = 0;
  for (const auto &child : children)
    hash = HashUtil::CombineHashes(hash, child->Hash());
  return hash;
}

ColRefSet ItemExpr::DeriveUsedColumns() {
  if (kind == ExpressionKind::Ident) {
    return ColRefSet({Cast<ItemIdent>().colref});
  }

  ColRefSet used_cols;
  for (const auto &child : children)
    used_cols = ColRefSetUnion(used_cols, child->DeriveUsedColumns());
  return used_cols;
}

bool ItemExpr::operator==(const ItemExpr &other) const {
  if (kind == other.kind) {
    for (auto [child1, child2] : std::views::zip(children, other.children))
      if (*child1 != *child2)
        return false;
  }
  return true;
}

std::string ItemExpr::ToString() const {
  switch (kind) {
    case pgp::ExpressionKind::Aggref: {
      const auto &agg = Cast<ItemAggref>();
      auto *agg_name = get_func_name(agg.aggfnoid);
      std::string args;
      for (const auto &child : children)
        args += child->ToString() + " ";

      if (!args.empty())
        args.pop_back();
      return std::format("{}({})", agg_name, args);
    }

    case pgp::ExpressionKind::Const: {
      return "Constant";
    }
    case pgp::ExpressionKind::ArrayExpr: {
      std::string args;
      for (const auto &child : children)
        args += child->ToString() + " ";

      if (!args.empty())
        args.pop_back();
      return std::format("Array[{}]", args);
    }
    case pgp::ExpressionKind::ScalarArrayOpExpr: {
      const auto &array_cmp = Cast<ItemArrayOpExpr>();
      return std::format("({} {} {})", children[0]->ToString(), array_cmp.use_or ? "ANY" : "ALL",
                         children[1]->ToString());
    }
    case pgp::ExpressionKind::BoolExpr: {
      const auto &bool_op = Cast<ItemBoolExpr>();
      if (bool_op.boolop == NOT_EXPR)
        return std::format("NOT {}", children[0]->ToString());

      std::string op = bool_op.boolop == AND_EXPR ? "AND" : "OR";
      std::string args;
      for (const auto &child : children)
        args += child->ToString() + ",";

      if (!args.empty())
        args.pop_back();
      return std::format("({} ({}))", op, args);
    }

    case pgp::ExpressionKind::CaseTestExpr:
      return "CaseTest";

    case pgp::ExpressionKind::RelabelType:
      return "Cast";

    case pgp::ExpressionKind::IsDistinctFrom:
    case pgp::ExpressionKind::OpExpr: {
      const auto &scalar_op = Cast<ItemOpExpr>();
      std::string args;
      for (const auto &child : children)
        args += child->ToString() + " ";

      if (!args.empty())
        args.pop_back();
      return std::format("({} {})", scalar_op.opno, args);
    }

    case pgp::ExpressionKind::CoalesceExpr:
      return "Coalesce";

    case pgp::ExpressionKind::FuncExpr: {
      const auto &scalar_func = Cast<ItemFuncExpr>();
      std::string args;
      for (const auto &child : children)
        args += child->ToString() + " ";

      if (!args.empty())
        args.pop_back();
      return std::format("({} {})", scalar_func.funcid, args);
    }

    case pgp::ExpressionKind::Ident: {
      const auto *ident = Cast<ItemIdent>().colref;
      return ident->CrefName();
    }
    case pgp::ExpressionKind::CaseExpr:
      return "Switch";

    case pgp::ExpressionKind::ProjectElement: {
      const auto &project_element = Cast<ItemProjectElement>();
      std::string args;
      for (const auto &child : children)
        args += child->ToString() + " ";

      if (!args.empty())
        args.pop_back();
      return std::format("Pj({} {})", project_element.colref->CrefName(), args);
    }

    default:
      return "unknown scalar operator";
  }
}

hash_t ItemAggref::Hash() const {
  auto hash = ItemExpr::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(aggfnoid));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(distinct));
  return hash;
}

Aggref *ItemAggref::ToAggref() const {
  Aggref *aggref = makeNode(Aggref);
  aggref->aggfnoid = aggfnoid;
  aggref->aggdistinct = NIL;
  aggref->agglevelsup = 0;
  aggref->aggkind = 'n';
  aggref->location = -1;
  aggref->aggtranstype = InvalidOid;
  aggref->aggargtypes = argtypes;
  aggref->aggtype = aggtype;
  aggref->aggsplit = AGGSPLIT_SIMPLE;
  aggref->aggkind = aggkind;
  aggref->aggcollid = get_typcollation(aggref->aggtype);

  return aggref;
}

bool ItemAggref::operator==(const ItemExpr &other) const {
  if (ItemExpr::operator==(other)) {
    const auto &agg = Cast<ItemAggref>();

    return (agg.distinct == distinct && aggfnoid == agg.aggfnoid);
  }

  return false;
}

hash_t ItemArrayExpr::Hash() const {
  auto hash = ItemExpr::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(element_typeid));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(array_typeid));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(multidims));
  return hash;
}

bool ItemArrayExpr::operator==(const ItemExpr &other) const {
  if (ItemExpr::operator==(other)) {
    const auto &array = Cast<ItemArrayExpr>();

    return (array.element_typeid == element_typeid && array_typeid == array.array_typeid &&
            multidims == array.multidims);
  }

  return false;
}

ArrayExpr *ItemArrayExpr::ToArrayExpr() const {
  auto *array_expr = makeNode(ArrayExpr);
  array_expr->element_typeid = element_typeid;
  array_expr->array_typeid = array_typeid;
  array_expr->array_collid = get_typcollation(array_typeid);
  array_expr->multidims = multidims;
  return array_expr;
}

hash_t ItemConst::Hash() const {
  return HashUtil::CombineHashes(ItemExpr::Hash(), HashUtil::HashNode(value));
}

bool ItemConst::operator==(const ItemExpr &other) const {
  if (ItemExpr::operator==(other)) {
    const auto &psconst = Cast<ItemConst>();
    return equal(value, psconst.value);
  }

  return false;
}

hash_t ItemArrayOpExpr::Hash() const {
  auto hash = ItemExpr::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(opno));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(opfuncid));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(use_or));
  return hash;
}

ScalarArrayOpExpr *ItemArrayOpExpr::ToScalarArrayOpExpr() const {
  ScalarArrayOpExpr *array_op_expr = makeNode(ScalarArrayOpExpr);
  array_op_expr->opno = opno;
  array_op_expr->opfuncid = opfuncid;
  array_op_expr->useOr = use_or;
  return array_op_expr;
}

bool ItemArrayOpExpr::operator==(const ItemExpr &other) const {
  if (ItemExpr::operator==(other)) {
    const auto &array_cmp = Cast<ItemArrayOpExpr>();

    return (opno == array_cmp.opno && opfuncid == array_cmp.opfuncid && use_or == array_cmp.use_or);
  }

  return false;
}

hash_t ItemBoolExpr::Hash() const {
  auto hash = ItemExpr::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(boolop));
  return hash;
}

bool ItemBoolExpr::operator==(const ItemExpr &other) const {
  if (ItemExpr::operator==(other)) {
    const auto &bool_expr = Cast<ItemBoolExpr>();

    return (boolop == bool_expr.boolop);
  }

  return false;
}

BoolExpr *ItemBoolExpr::ToBoolExpr() const {
  BoolExpr *scalar_bool_expr = makeNode(BoolExpr);
  scalar_bool_expr->boolop = boolop;

  return scalar_bool_expr;
}

hash_t ItemProjectElement::Hash() const {
  auto hash = ItemExpr::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(colref->Id()));
  return hash;
}

bool ItemProjectElement::operator==(const ItemExpr &other) const {
  if (ItemExpr::operator==(other)) {
    const auto &project_element = Cast<ItemProjectElement>();

    return colref->Id() == project_element.colref->Id();
  }

  return false;
}

bool ItemSortGroupClause::operator==(const ItemExpr &other) const {
  if (ItemExpr::operator==(other)) {
    const auto &sort_group_clause = Cast<ItemSortGroupClause>();

    return equal(expr, sort_group_clause.expr);
  }

  return false;
}

bool ItemParam::operator==(const ItemExpr &other) const {
  if (ItemExpr::operator==(other)) {
    const auto &param = Cast<ItemParam>();

    return paramid == param.paramid;
  }

  return false;
}

ItemExprPtr ItemParam::FromPg(Param *param) {
  return std::make_shared<ItemParam>(param->paramid, param->paramtype, param->paramtypmod);
}

hash_t ItemCaseTest::Hash() const {
  auto hash = HashUtil::Hash(typeId);
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(typeMod));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(collation));
  return hash;
}

bool ItemCaseTest::operator==(const ItemExpr &other) const {
  if (ItemExpr::operator==(other)) {
    const auto &sc_case_test = Cast<ItemCaseTest>();

    return typeId == sc_case_test.typeId && typeMod == sc_case_test.typeMod && collation == sc_case_test.collation;
  }

  return false;
}

CaseTestExpr *ItemCaseTest::ToCaseTestExpr() const {
  CaseTestExpr *case_test_expr = makeNode(CaseTestExpr);
  case_test_expr->typeId = typeId;
  case_test_expr->typeMod = typeMod;
  case_test_expr->collation = collation;
  return case_test_expr;
}

hash_t ItemCastExpr::Hash() const {
  auto hash = ItemExpr::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(resulttype));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(funcid));
  return hash;
}

bool ItemCastExpr::operator==(const ItemExpr &other) const {
  if (ItemExpr::operator==(other)) {
    const auto &cast_expr = Cast<ItemCastExpr>();

    return (cast_expr.resulttype == resulttype && cast_expr.funcid == funcid);
  }

  return false;
}

Expr *ItemCastExpr::ToExpr() const {
  if (funcid != InvalidOid) {
    FuncExpr *func_expr = makeNode(FuncExpr);
    func_expr->funcid = funcid;
    func_expr->funcretset = get_func_retset(funcid);
    func_expr->funcformat = COERCE_IMPLICIT_CAST;
    func_expr->funcresulttype = resulttype;
    func_expr->funccollid = get_typcollation(func_expr->funcresulttype);

    return (Expr *)func_expr;
  }
  RelabelType *relabel_type = makeNode(RelabelType);
  relabel_type->resulttype = resulttype;
  relabel_type->resulttypmod = -1;
  relabel_type->location = -1;
  relabel_type->relabelformat = COERCE_IMPLICIT_CAST;

  return (Expr *)relabel_type;
}

hash_t ItemCoalesce::Hash() const {
  auto hash = ItemExpr::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(coalescetype));
  return hash;
}

bool ItemCoalesce::operator==(const ItemExpr &other) const {
  if (ItemExpr::operator==(other)) {
    const auto &coalesce = Cast<ItemCoalesce>();

    return (coalescetype == coalesce.coalescetype);
  }

  return false;
}

CoalesceExpr *ItemCoalesce::ToCoalesceExpr() const {
  CoalesceExpr *coalesce = makeNode(CoalesceExpr);
  coalesce->coalescetype = coalescetype;
  coalesce->coalescecollid = get_typcollation(coalesce->coalescetype);

  return coalesce;
}

bool ItemCoerceBase::operator==(const ItemExpr &other) const {
  if (ItemExpr::operator==(other)) {
    const auto &coerce_base = Cast<ItemCoerceBase>();

    return (resulttype == coerce_base.resulttype && coerceformat == coerce_base.coerceformat &&
            resulttypmod == coerce_base.resulttypmod);
  }

  return false;
}

hash_t ItemIdent::Hash() const {
  auto hash = ItemExpr::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(colref->Id()));
  return hash;
}

bool ItemIdent::operator==(const ItemExpr &other) const {
  if (ItemExpr::operator==(other)) {
    const auto &ident = Cast<ItemIdent>();

    return colref->Id() == ident.colref->Id();
  }

  return false;
}

hash_t ItemFuncExpr::Hash() const {
  auto hash = ItemExpr::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(funcid));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(funcresulttype));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(funcvariadic));
  return hash;
}

bool ItemFuncExpr::operator==(const ItemExpr &other) const {
  if (ItemExpr::operator==(other)) {
    const auto &func_expr = Cast<ItemFuncExpr>();
    return (funcid == func_expr.funcid && funcresulttype == func_expr.funcresulttype &&
            funcvariadic == func_expr.funcvariadic);
  }
  return false;
}

FuncExpr *ItemFuncExpr::ToFuncExpr() const {
  FuncExpr *func_expr = makeNode(FuncExpr);
  func_expr->funcid = funcid;
  func_expr->funcretset = Catalog::GetFuncRetSet(funcid);
  func_expr->funcformat = COERCE_EXPLICIT_CALL;
  func_expr->funcresulttype = funcresulttype;
  func_expr->funcvariadic = funcvariadic;
  func_expr->funccollid = get_typcollation(funcresulttype);

  return func_expr;
}

hash_t ItemCaseExpr::Hash() const {
  auto hash = ItemExpr::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(casetype));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(case_arg_exist));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(default_arg_exist));
  return hash;
}

bool ItemCaseExpr::operator==(const ItemExpr &other) const {
  if (ItemExpr::operator==(other)) {
    const auto &case_node = Cast<ItemCaseExpr>();

    return casetype == case_node.casetype && case_arg_exist == case_node.case_arg_exist &&
           default_arg_exist == case_node.default_arg_exist;
  }

  return false;
}

ItemExprPtr ItemCaseExpr::FromPg(CaseExpr *case_expr) {
  return std::make_shared<ItemCaseExpr>(case_expr->casetype, case_expr->casecollid, case_expr->arg != nullptr,
                                        case_expr->defresult != nullptr);
}

CaseExpr *ItemCaseExpr::ToCaseExpr() const {
  CaseExpr *case_expr = makeNode(CaseExpr);
  case_expr->casetype = casetype;
  case_expr->casecollid = casecollid;
  return case_expr;
}

hash_t ItemOpExpr::Hash() const {
  auto hash = ItemExpr::Hash();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(opno));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(opresulttype));
  return hash;
}

bool ItemOpExpr::operator==(const ItemExpr &other) const {
  if (ItemExpr::operator==(other)) {
    const auto &op_expr = Cast<ItemOpExpr>();

    return (op_expr.opno == opno && op_expr.opresulttype == opresulttype);
  }

  return false;
}

OpExpr *ItemOpExpr::ToOpExpr() const {
  OpExpr *op_expr = makeNode(OpExpr);
  op_expr->opno = opno;
  op_expr->opfuncid = Catalog::GetOpCode(op_expr->opno);

  if (opresulttype != InvalidOid)
    op_expr->opresulttype = opresulttype;
  else
    op_expr->opresulttype = get_func_rettype(op_expr->opfuncid);

  op_expr->opretset = get_func_retset(Catalog::GetOpCode(op_expr->opno));

  op_expr->opcollid = get_typcollation(op_expr->opresulttype);

  return op_expr;
}

}  // namespace pgp