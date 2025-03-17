#include "pg_optimizer/order_spec.h"

#include <ranges>

#include "common/hash_util.h"
#include "pg_optimizer/colref.h"

namespace pgp {

// this > pos
bool OrderSpec::Satisfies(const OrderSpec *pos) const {
  if (sort_array_.size() < pos->sort_array_.size())
    return false;

  for (auto [s1, s2] : std::views::zip(sort_array_, pos->sort_array_)) {
    if (s1 != s2)
      return false;
  }

  return true;
}

uint32_t OrderSpec::Hash() const {
  uint32_t hash = 0;
  for (auto sort : sort_array_)
    hash = HashUtil::CombineHashes(hash, sort.Hash());

  return hash;
}

ColRefSet OrderSpec::GetUsedColumns() const {
  ColRefSet pcrs;
  for (auto sort : sort_array_) {
    pcrs.insert(sort.colref);
  }

  return pcrs;
}
}  // namespace pgp
