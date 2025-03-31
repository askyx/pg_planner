#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "pg_optimizer/colref.h"
#include "pg_optimizer/order_spec.h"

namespace pgp {

enum class ScanDirection {
  Invalid,
  Forward,
  Backward,
};

struct IndexInfo {
  Oid index;
  Oid relam;
  ColRefArray index_cols;
  ColRefArray index_include;
  std::vector<Oid> sortopfamily;
  std::vector<Oid> opcintype;
  std::vector<bool> reverse_sort;
  std::vector<bool> null_first;

  ScanDirection GetScanDirection(const std::shared_ptr<OrderSpec> &order_spec) const;

  std::string ToString() const;
};

struct RelationInfo {
  ColRefArray output_columns;
  std::vector<IndexInfo> index_list;

  std::string ToString() const;
};

using RelationInfoPtr = std::shared_ptr<RelationInfo>;
using RelationInfoMap = std::unordered_map<Oid, RelationInfoPtr>;

}  // namespace pgp