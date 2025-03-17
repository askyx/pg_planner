#pragma once

#include "common/macros.h"

extern "C" {
#include <utils/relcache.h>
}
namespace pgp {

// TODO: catch error by cpp mod
class Catalog {
 public:
  DISALLOW_COPY_AND_MOVE(Catalog)

  Catalog() = default;

  static char *GetFuncName(Oid func_oid);
  static char *GetOperName(Oid func_oid);

  static Oid GetFuncRetType(Oid func_oid);

  static bool GetFuncRetSet(Oid func_oid);

  static bool FuncStrict(Oid func_oid);

  static Oid GetCastFuncId(Oid src_type, Oid dest_type);

  static Oid GetInverseOp(Oid op_oid);

  static bool OpStrict(Oid op_oid);

  static Oid GetOpCode(Oid op_oid);

  static Oid GetAggSortOp(Oid aggfnoid);

  static bool TypeByVal(Oid type_oid);

  static Oid GetAggTranstype(Oid aggfnoid);
};
}  // namespace pgp
