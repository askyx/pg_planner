#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/hash_util.h"
#include "common/macros.h"
#include "pg_optimizer/colref.h"

namespace pgp {

enum class NullsOrder {
  EnullsLast,
  EnullsFirst,
};

class OrderSpec {
 private:
  struct SortElement {
    Oid sort_op;
    ColRef *colref;
    NullsOrder nulls_order;
    bool operator==(const SortElement &other) const {
      return sort_op == other.sort_op && colref == other.colref && nulls_order == other.nulls_order;
    }
    uint32_t Hash() const {
      auto hash = HashUtil::Hash(sort_op);
      hash = HashUtil::CombineHashes(hash, colref->Id());
      hash = HashUtil::CombineHashes(hash, static_cast<uint32_t>(nulls_order));
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

  OrderSpec *Copy() const {
    auto *copy = new OrderSpec();
    copy->sort_array_ = sort_array_;
    return copy;
  }

  ColRefSet GetUsedColumns() const;

  bool operator==(const OrderSpec &other) const { return sort_array_ == other.sort_array_; }

  bool Satisfies(const OrderSpec *pos) const;

  uint32_t Hash() const;
};

}  // namespace pgp
