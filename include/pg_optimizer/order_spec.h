#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "common/hash_util.h"
#include "common/macros.h"
#include "pg_optimizer/colref.h"

namespace pgp {

class OrderSpec {
 private:
  struct SortElement {
    Oid sort_op;
    ColRef *colref;
    bool nulls_first;
    bool operator==(const SortElement &other) const {
      return sort_op == other.sort_op && colref == other.colref && nulls_first == other.nulls_first;
    }
    uint32_t Hash() const {
      auto hash = HashUtil::Hash(sort_op);
      hash = HashUtil::CombineHashes(hash, colref->ref_id);
      hash = HashUtil::CombineHashes(hash, HashUtil::Hash(nulls_first));
      return hash;
    }
  };

  std::vector<SortElement> sort_array_;

 public:
  DISALLOW_COPY(OrderSpec)

  OrderSpec() = default;

  size_t SortSize() const { return sort_array_.size(); }

  const auto &GetSortArray() const { return sort_array_; }

  void AddSortElement(SortElement ele) { sort_array_.push_back(ele); }

  std::shared_ptr<OrderSpec> Copy() const {
    auto copy = std::make_shared<OrderSpec>();
    copy->sort_array_ = sort_array_;
    return copy;
  }

  ColRefSet GetUsedColumns() const;

  bool operator==(const OrderSpec &other) const { return sort_array_ == other.sort_array_; }

  bool Satisfies(const std::shared_ptr<OrderSpec> &pos) const;

  uint32_t Hash() const;
};

}  // namespace pgp
