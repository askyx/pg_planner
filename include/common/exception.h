#pragma once

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <format>
#include <stdexcept>

extern "C" {
#include <postgres.h>

#include <utils/palloc.h>
}

#ifdef NDEBUG
#define ALWAYS_INLINE __attribute__((always_inline))
#else
#ifndef ALWAYS_INLINE
#define ALWAYS_INLINE
#endif
#endif

namespace pgp {

struct Exception : public std::runtime_error {
  const char *filename;
  const char *function;
  int32_t line;

  Exception(const char *filename, int32_t line, const char *function, const char *meg)
      : std::runtime_error(meg), filename(filename), function(function), line(line) {}
};

}  // namespace pgp

#define OptException(meg) pgp::Exception(__FILE__, __LINE__, __func__, meg)

#if PLAN_DEBUG
#define PGP_ASSERT(condition, msg) \
  do {                              \
    if (!(condition)) {             \
      throw OptException(msg);      \
    }                               \
  } while (0)
#else
#define PGP_ASSERT(condition, msg) ((void)0)
#endif
