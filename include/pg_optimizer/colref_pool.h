#pragma once

#include <unordered_map>

#include "common/macros.h"
#include "pg_optimizer/colref.h"

namespace pgp {

class ColRefPool {
 private:
  uint32_t colid_counter_{0};

  std::unordered_map<uint32_t, ColRef *> col_ref_map_;

 public:
  DISALLOW_COPY(ColRefPool)

  ColRefPool() = default;

  ~ColRefPool();

  ColRef *PcrCreate(Oid type, int32_t mod);

  ColRef *PcrCreate(Oid type, int32_t mod, const std::string &name);

  ColRef *PcrCreate(Oid type, int32_t mod, bool mark_as_used, bool nullable, const std::string &name, uint32_t width);

  ColRef *PcrCreate(const ColRef *colref) { return PcrCreate(colref->RetrieveType(), colref->TypeModifier()); }
};
}  // namespace pgp
