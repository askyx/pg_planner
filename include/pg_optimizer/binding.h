#pragma once

#include <memory>

#include "pg_operator/operator_node.h"
#include "pg_optimizer/group.h"
#include "pg_optimizer/memo.h"
#include "pg_optimizer/pattern.h"

namespace pgp {

class BindingIterator {
 public:
  explicit BindingIterator(const Memo &memo) : memo(memo) {}

  virtual ~BindingIterator() = default;

  virtual bool HasNext() = 0;

  virtual OperatorNodePtr Next() = 0;

 protected:
  const Memo &memo;
};

/**
 * GroupBindingIterator is an implementation of the BindingIterator abstract
 * class that is specialized for trying to bind a group against a pattern.
 */
class GroupBindingIterator : public BindingIterator {
 public:
  GroupBindingIterator(const Memo &memo, Group *group, Pattern *pattern)
      : BindingIterator(memo),
        pattern_(pattern),
        target_group_(group),
        num_group_items_(target_group_->GetLogicalExpressions().size()) {
    catch_multiple_leaf_ = pattern_ == nullptr;
  }

  bool HasNext() override;

  OperatorNodePtr Next() override;

 private:
  Pattern *pattern_;

  Group *target_group_;

  size_t num_group_items_;

  size_t current_item_index_{};

  std::unique_ptr<BindingIterator> current_iterator_;

  bool catch_multiple_leaf_{false};
};

/**
 * GroupExprBindingIterator is an implementation of the BindingIterator abstract
 * class that is specialized for trying to bind a GroupExpression against a pattern.
 */
class GroupExprBindingIterator : public BindingIterator {
 public:
  GroupExprBindingIterator(const Memo &memo, GroupExpression *gexpr, Pattern *pattern);

  bool HasNext() override;

  OperatorNodePtr Next() override { return current_binding_; }

 private:
  GroupExpression *gexpr_;

  bool first_;

  bool has_next_;

  OperatorNodePtr current_binding_;

  std::vector<OperatorNodeArray> children_bindings_;

  std::vector<size_t> children_bindings_pos_;
};

}  // namespace pgp
