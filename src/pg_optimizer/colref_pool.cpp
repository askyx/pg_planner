#include "pg_optimizer/colref_pool.h"

namespace pgp {

ColRefPool::~ColRefPool() {
  for (auto &colref : col_ref_map_) {
    delete colref.second;
  }
}

ColRef *ColRefPool::PcrCreate(Oid type, int32_t mod) {
  return PcrCreate(type, mod, std::format("ColRef_{}", colid_counter_ - 1));
}

ColRef *ColRefPool::PcrCreate(Oid type, int32_t mod, const std::string &name) {
  auto *colref = new ColRef(type, mod, colid_counter_++, name);

  col_ref_map_[colref->Id()] = colref;

  return colref;
}

ColRef *ColRefPool::PcrCreate(Oid type, int32_t mod, bool mark_as_used, bool nullable, const std::string &name,
                              uint32_t width) {
  auto *colref = new ColRef(type, mod, colid_counter_++, name, nullable, width);

  col_ref_map_[colref->Id()] = colref;

  return colref;
}

}  // namespace pgp