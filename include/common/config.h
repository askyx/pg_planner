#pragma once

extern "C" {
#include <postgres.h>

#include <nodes/bitmapset.h>
}

#ifndef OPT_CONFIG
#define OPT_CONFIG(v) pgp::OptConfig::GetInstance().v
#endif

namespace pgp {

// NOLINTBEGIN
struct OptConfig {
  static OptConfig &GetInstance() {
    static OptConfig instance;
    return instance;
  }

  bool enable_optimizer{true};

  bool enable_test_flag{false};

  int32_t task_execution_timeout{6000};
};
}  // namespace pgp

// NOLINTEND