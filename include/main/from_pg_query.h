#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "common/hash_util.h"
#include "common/macros.h"
#include "pg_operator/item_expr.h"
#include "pg_operator/operator_node.h"
#include "pg_optimizer/colref.h"
#include "pg_optimizer/optimizer_context.h"
#include "pg_optimizer/property.h"

extern "C" {
#include <nodes/parsenodes.h>
#include <nodes/primnodes.h>
}

namespace pgp {
class Catalog;

struct VarToColRefMap {
  struct TranslateAttrInfo {
    uint32_t query_level;
    int32_t varno;
    int32_t attno;
  };

  struct AttInfoPtrHash {
    std::size_t operator()(const TranslateAttrInfo &s) const {
      auto hash = HashUtil::Hash(s.query_level);
      hash = HashUtil::CombineHashes(hash, HashUtil::Hash(s.varno));
      hash = HashUtil::CombineHashes(hash, HashUtil::Hash(s.attno));
      return hash;
    }
  };

  struct AttInfoPtrEq {
    bool operator()(const TranslateAttrInfo &t1, const TranslateAttrInfo &t2) const {
      return (t1.query_level == t2.query_level && t1.varno == t2.varno && t1.attno == t2.attno);
    }
  };

  std::unordered_map<TranslateAttrInfo, ColRef *, AttInfoPtrHash, AttInfoPtrEq> colinfo_mapping;

  void Insert(uint32_t query_level, int32_t var_no, int32_t attrnum, ColRef *colref) {
    TranslateAttrInfo col{query_level, var_no, attrnum};

    colinfo_mapping[col] = colref;
  }

  ColRef *GetColRefByVar(uint32_t current_query_level, const Var *var) const {
    TranslateAttrInfo attrinfo{current_query_level - var->varlevelsup, var->varno, var->varattno};

    if (colinfo_mapping.contains(attrinfo))
      return colinfo_mapping.at(attrinfo);

    throw std::runtime_error("No variable");
  }
};

class TranslatorQuery {
 private:
  VarToColRefMap var_to_colid_map_;

  Query *query_;

  uint32_t query_level_{0};

  OptimizerContext *optimizer_context_;

  ColRefArray col_ref_;

  std::shared_ptr<PropertySet> property_set_{std::make_shared<PropertySet>()};

 public:
  DISALLOW_COPY_AND_MOVE(TranslatorQuery)

  TranslatorQuery(OptimizerContext *ctx, Query *query) : query_(query), optimizer_context_(ctx) {}

  TranslatorQuery(OptimizerContext *ctx, VarToColRefMap var_colid_mapping, Query *query, uint32_t query_level)
      : var_to_colid_map_(std::move(var_colid_mapping)),
        query_(query),
        query_level_(query_level),
        optimizer_context_(ctx) {}

  OperatorNode *TranslateQuery();

  OperatorNode *TranslateSelect();

  OperatorNode *TranslateNode(Node *node);

  OperatorNode *TranslateNode(FromExpr *from_expr);
  OperatorNode *TranslateNode(RangeTblRef *node);
  OperatorNode *TranslateNode(JoinExpr *join_expr);

  ItemExprPtr TranslateNode(SubLink *sublink, OperatorNode **root, bool under_not);

  ItemExprPtr TranslateExprToProject(Expr *expr, OperatorNode **root, const char *alias_name);

  ItemExprPtr TranslateExpr(const Expr *expr, OperatorNode **root);

  ColRefArray GetQueryOutputCols() const { return col_ref_; }

  std::shared_ptr<PropertySet> GetPropertySet() const { return property_set_; }
};
}  // namespace pgp
