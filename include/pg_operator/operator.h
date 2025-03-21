#pragma once

#include <string>

#include "common/hash_util.h"
#include "common/macros.h"
#include "postgres_ext.h"

namespace pgp {

class Operator;

enum class OperatorType {
  Invalid,

  LEAF,

  LogicalGet,
  LogicalFilter,
  LogicalGbAgg,
  LogicalLimit,
  LogicalProject,
  LogicalApply,
  LogicalJoin,

  PhysicalScan,
  PhysicalFilter,
  PhysicalFullMergeJoin,
  PhysicalNLJoin,
  PhysicalApply,
  PhysicalHashJoin,
  PhysicalHashAgg,
  PhysicalStreamAgg,
  PhysicalScalarAgg,
  PhysicalSort,
  PhysicalLimit,
  PhysicalComputeScalar,
};

class Operator {
 public:
  OperatorType kind;

  DISALLOW_COPY(Operator)

  explicit Operator(OperatorType kind) : kind(kind) {}

  virtual ~Operator() = default;

  std::string ToString() const;

  virtual bool Logical() const { return false; }

  virtual bool Physical() const { return false; }

  virtual hash_t Hash() const { return HashUtil::Hash(kind); }

  virtual bool operator==(const Operator &other) const { return kind == other.kind; }

  virtual bool operator!=(const Operator &other) const { return !(*this == other); }

  template <class TARGET>
  TARGET &Cast() {
    if (TARGET::TYPE != OperatorType::Invalid && kind != TARGET::TYPE) {
      throw std::runtime_error("Failed to cast itemexpr to type - itemexpr type mismatch");
    }
    return reinterpret_cast<TARGET &>(*this);
  }

  template <class TARGET>
  const TARGET &Cast() const {
    if (TARGET::TYPE != OperatorType::Invalid && kind != TARGET::TYPE) {
      throw std::runtime_error("Failed to cast itemexpr to type - itemexpr type mismatch");
    }
    return reinterpret_cast<const TARGET &>(*this);
  }
};

}  // namespace pgp
