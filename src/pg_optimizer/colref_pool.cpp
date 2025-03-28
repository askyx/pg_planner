#include "pg_optimizer/colref_pool.h"

namespace pgp {

ColRefPool::~ColRefPool() {
  for (auto &colref : col_ref_map_) {
    delete colref.second;
  }
}

ColRef *ColRefPool::PcrCreate(Oid type, int32_t mod, const std::string &name) {
  auto *colref = new ColRef{
      .ref_id = colid_counter_++,
      .type = type,
      .modifier = mod,
      .name = name,
  };

  col_ref_map_[colref->ref_id] = colref;

  return colref;
}

ColRef *ColRefPool::PcrCreate(Oid type, int32_t mod, bool mark_as_used, bool nullable, const std::string &name,
                              uint32_t width) {
  auto *colref = new ColRef{
      .ref_id = colid_counter_++,
      .type = type,
      .modifier = mod,
      .name = name,
      .nullable = nullable,
      .width = width,
  };

  col_ref_map_[colref->ref_id] = colref;

  return colref;
}

}  // namespace pgp