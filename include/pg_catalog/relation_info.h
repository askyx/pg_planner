#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/exception.h"
#include "pg_optimizer/colref.h"
#include "pg_optimizer/order_spec.h"
extern "C" {
#include <access/sdir.h>
}
namespace pgp {

struct IndexInfo {
  Oid relam;
  ColRefArray index_cols;
  ColRefArray index_include;
  std::vector<Oid> sortopfamily;
  std::vector<Oid> opcintype;
  std::vector<bool> reverse_sort;
  std::vector<bool> null_first;

  ScanDirection GetScanDirection(const std::shared_ptr<OrderSpec> &order_spec) const;

  std::shared_ptr<OrderSpec> ConstructOrderSpec() const;

  ColRefSet GetIndexOutCols() const {
    ColRefSetWapper result{index_cols};
    result.AddColRef(index_include);
    return result.Get();
  }

  std::string ToString() const;
};

struct RelationInfo {
  ColRefArray output_columns;
  std::unordered_map<Oid, IndexInfo> relation_indexes;

  const IndexInfo &GetIndexInfo(Oid index_oid) const {
    PGP_ASSERT(relation_indexes.contains(index_oid), "index not found");
    return relation_indexes.at(index_oid);
  }

  std::string ToString() const;
};

using RelationInfoPtr = std::shared_ptr<RelationInfo>;
using RelationInfoMap = std::unordered_map<Oid, RelationInfoPtr>;

}  // namespace pgp