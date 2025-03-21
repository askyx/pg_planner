#pragma once

#include <ranges>
#include <sstream>
#include <string>
#include <vector>

#include "common/macros.h"
#include "pg_operator/operator_node.h"

namespace pgp {

template <class Obj>
class Format {
 private:
  bool first_ = true;
  std::stringstream ss_;
  std::vector<std::string> prefix_;

  std::string FormatOperatorTree(const Obj &obj, bool is_last = true) {
    for (const auto &prefix : prefix_)
      ss_ << prefix;

    if (first_)
      first_ = false;
    else {
      if (is_last) {
        ss_ << "└── ";
        prefix_.emplace_back("    ");
      } else {
        ss_ << "├── ";
        prefix_.emplace_back("│   ");
      }
    }

    if constexpr (std::is_same_v<Obj, pgp::OperatorNodePtr>) {
      ss_ << obj->ToString() << '\n';
      auto child_size = obj->children.size();
      for (const auto &[i, child] : std::views::enumerate(obj->children)) {
        FormatOperatorTree(child, i == child_size - 1);
      }
    }

    if (!prefix_.empty())
      prefix_.pop_back();

    return ss_.str();
  }

  std::string ToStringInternal(const Obj &obj) {
    ss_.str("");
    return FormatOperatorTree(obj);
  }

 public:
  Format() = default;
  DISALLOW_COPY_AND_MOVE(Format)

  static std::string ToString(const Obj &obj) {
    Format formator;
    return formator.ToStringInternal(obj);
  }
};

}  // namespace pgp