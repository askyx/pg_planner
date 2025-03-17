#pragma once

#include <memory>
extern "C" {
#include <postgres.h>

#include <utils/elog.h>
}

#include "common/exception.h"

namespace optimizer {

template <typename Func>
inline auto pg_guard(Func func) ->  // NOLINT
    typename std::conditional<std::is_same_v<decltype(func()), void>, void, decltype(func())>::type {
  auto *prev_exception_stack = PG_exception_stack;
  auto *prev_error_context_stack = error_context_stack;
  auto *oldcontext = CurrentMemoryContext;

  sigjmp_buf local_sigjmp_buf;
  if (auto jump_value = sigsetjmp(local_sigjmp_buf, 0); jump_value == 0) {
    PG_exception_stack = &local_sigjmp_buf;

    if constexpr (std::is_same<decltype(func()), void>::value) {
      func();
      PG_exception_stack = prev_exception_stack;
      error_context_stack = prev_error_context_stack;
      return;
    } else {
      auto re = func();

      PG_exception_stack = prev_exception_stack;
      error_context_stack = prev_error_context_stack;
      return re;
    }
  }

  // switch back to the caller memory context
  MemoryContextSwitchTo(oldcontext);

  PG_exception_stack = prev_exception_stack;
  error_context_stack = prev_error_context_stack;

  auto edata = std::unique_ptr<ErrorData, void (*)(ErrorData *)>{CopyErrorData(),
                                                                 [](ErrorData *edata) { FreeErrorData(edata); }};

  throw pgp::Exception(edata->filename, edata->lineno, edata->funcname, edata->message);
}

#define PG_GUARD(func) optimizer::pg_guard([&]() { return func; })
#define PG_GUARD_VOID(func) optimizer::pg_guard([&]() { func; })

}  // namespace optimizer