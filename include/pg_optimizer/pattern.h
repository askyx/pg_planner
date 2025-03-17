#pragma once

#include <vector>

#include "common/hash_util.h"
#include "pg_operator/operator.h"
#include "pg_optimizer/group.h"

namespace pgp {

class Group;

class Pattern {
 public:
  explicit Pattern(OperatorType op) : type_(op) {}

  ~Pattern() {
    for (auto *child : children_) {
      delete child;
    }
  }

  void AddChild(Pattern *child) { children_.push_back(child); }

  const std::vector<Pattern *> &Children() const { return children_; }

  size_t GetChildPatternsSize() const { return children_.size(); }

  OperatorType Type() const { return type_; }

  void SetMultiLeaf() { multi_leaf_ = true; }

  bool IsMultiLeaf() const { return multi_leaf_; }

 private:
  OperatorType type_;

  std::vector<Pattern *> children_;

  bool multi_leaf_{false};
};

class LeafOperator : public Operator {
 public:
  constexpr static OperatorType TYPE = OperatorType::LEAF;

  explicit LeafOperator(Group *group) : Operator(TYPE), origin_group_(group) {}

  Group *GetOriginGroup() const { return origin_group_; }

  hash_t Hash() const override { return HashUtil::Hash(origin_group_->GetGroupId()); }

  bool operator==(const Operator &other) const override {
    if (Operator::operator==(other)) {
      const auto &leaf = Cast<LeafOperator>();
      return leaf.origin_group_->GetGroupId() == origin_group_->GetGroupId();
    }

    return false;
  }

 private:
  Group *origin_group_;
};

}  // namespace pgp