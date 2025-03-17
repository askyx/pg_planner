#pragma once

#include <cassert>
#include <format>

extern "C" {
#include <postgres.h>

#include <nodes/nodes.h>
}

//===--------------------------------------------------------------------===//
// attributes
//===--------------------------------------------------------------------===//

#define UNUSED_ATTRIBUTE __attribute__((unused))
#define RESTRICT __restrict__
#define NEVER_INLINE __attribute__((noinline))
#define PACKED __attribute__((packed))
// NOLINTNEXTLINE
#define FALLTHROUGH [[fallthrough]]
#define NORETURN __attribute((noreturn))

#ifdef NDEBUG
#define ALWAYS_INLINE __attribute__((always_inline))
#else
#ifndef ALWAYS_INLINE
#define ALWAYS_INLINE
#endif
#endif

#ifndef UNREACHABLE
#define UNREACHABLE __builtin_unreachable()
#endif

//===--------------------------------------------------------------------===//
// ALWAYS_ASSERT
//===--------------------------------------------------------------------===//

#ifdef NDEBUG
#define CASCADES_ASSERT(expr, message) ((void)0)
#else
#define CASCADES_ASSERT(expr, message) assert((expr) && (message))
#endif

//===----------------------------------------------------------------------===//
// Handy macros to hide move/copy class constructors
//===----------------------------------------------------------------------===//

// Macros to disable copying and moving
#ifndef DISALLOW_COPY
#define DISALLOW_COPY(cname)     \
  /* Delete copy constructor. */ \
  cname(const cname &) = delete; \
  /* Delete copy assignment. */  \
  cname &operator=(const cname &) = delete;

#define DISALLOW_MOVE(cname)     \
  /* Delete move constructor. */ \
  cname(cname &&) = delete;      \
  /* Delete move assignment. */  \
  cname &operator=(cname &&) = delete;

/**
 * Disable copy and move.
 */
#define DISALLOW_COPY_AND_MOVE(cname) \
  DISALLOW_COPY(cname)                \
  DISALLOW_MOVE(cname)

/** Disallow instantiation of the class. This should be used for classes that only have static functions. */
#define DISALLOW_INSTANTIATION(cname) \
  /* Prevent instantiation. */        \
  cname() = delete;

#endif

#if PLAN_DEBUG
#define OLOG(...)                                   \
  do {                                              \
    printf("%s", std::format(__VA_ARGS__).c_str()); \
    fflush(stdout);                                 \
  } while (0)

#define OLOGLN(...)                                   \
  do {                                                \
    printf("%s\n", std::format(__VA_ARGS__).c_str()); \
    fflush(stdout);                                   \
  } while (0)
#else
#define OLOG(...) (void(0))
#define OLOGLN(...) (void(0))
#endif