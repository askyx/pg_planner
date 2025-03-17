#pragma once

#include "common/macros.h"

namespace pgp {

template <class Constructor, class Destructor>
struct ScopedDefer {
  Destructor destructor;

  ScopedDefer(Constructor f1, Destructor f2) : destructor(f2) { f1(); }

  ~ScopedDefer() { destructor(); }

  DISALLOW_COPY_AND_MOVE(ScopedDefer)
};

}  // namespace pgp