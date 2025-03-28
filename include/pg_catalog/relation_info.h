#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "pg_optimizer/colref.h"
#include "postgres_ext.h"
namespace pgp {

// a desc
struct RelationInfo {
  struct IndexInfo {
    Oid index;
    ColRefArray index_cols;
    ColRefArray index_include;
  };
  ColRefArray output_columns;
  std::vector<IndexInfo> index_list;
};

using RelationInfoPtr = std::shared_ptr<RelationInfo>;
using RelationInfoMap = std::unordered_map<Oid, RelationInfoPtr>;

}  // namespace pgp