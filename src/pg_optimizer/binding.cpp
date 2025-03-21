#include "pg_optimizer/binding.h"

#include <memory>
#include <vector>

#include "pg_operator/operator.h"
#include "pg_operator/operator_node.h"

namespace pgp {

bool GroupBindingIterator::HasNext() {
  if (catch_multiple_leaf_ || pattern_->Type() == OperatorType::LEAF) {
    return current_item_index_ == 0;
  }

  if (current_iterator_) {
    // Check if still have bindings in current item
    if (!current_iterator_->HasNext()) {
      current_iterator_.reset(nullptr);
      current_item_index_++;
    }
  }

  if (current_iterator_ == nullptr) {
    // Keep checking item iterators until we find a match
    while (current_item_index_ < num_group_items_) {
      auto *gexpr = target_group_->GetLogicalExpressions()[current_item_index_];
      auto *gexpr_it = new GroupExprBindingIterator(memo_, gexpr, pattern_);
      current_iterator_.reset(gexpr_it);

      if (current_iterator_->HasNext()) {
        break;
      }

      current_iterator_.reset(nullptr);
      current_item_index_++;
    }
  }
  return current_iterator_ != nullptr;
}

OperatorNodePtr GroupBindingIterator::Next() {
  if (catch_multiple_leaf_ || pattern_->Type() == OperatorType::LEAF) {
    current_item_index_ = num_group_items_;
    auto result = MakeOperatorNode(std::make_shared<LeafOperator>(target_group_));
    result->operator_properties = target_group_->GroupProperties();

    return result;
  }

  return current_iterator_->Next();
}

GroupExprBindingIterator::GroupExprBindingIterator(const Memo &memo, GroupExpression *gexpr, Pattern *pattern)
    : BindingIterator(memo), gexpr_(gexpr), first_(true), has_next_(false), current_binding_(nullptr) {
  if (gexpr->Pop()->kind != pattern->Type()) {
    // Check root node type
    return;
  }

  (void)gexpr_;

  const auto &child_groups = gexpr->GetChildGroup();
  const std::vector<Pattern *> &child_patterns = pattern->Children();

  if (child_groups.size() != child_patterns.size() && !pattern->IsMultiLeaf()) {
    // Check make sure sizes are equal
    return;
  }

  // Find all bindings for children
  children_bindings_.resize(child_groups.size());
  children_bindings_pos_.resize(child_groups.size(), 0);

  // Get first level children
  OperatorNodeArray children;

  for (size_t i = 0; i < child_groups.size(); ++i) {
    // Try to find a match in the given group
    auto &child_bindings = children_bindings_[i];
    GroupBindingIterator iterator(memo_, child_groups[i], pattern->IsMultiLeaf() ? nullptr : child_patterns[i]);

    // Get all bindings
    while (iterator.HasNext()) {
      child_bindings.emplace_back(iterator.Next());
    }

    if (child_bindings.empty()) {
      // Child binding failed
      return;
    }

    // Push a copy
    children.emplace_back(child_bindings[0]);
  }

  has_next_ = true;
  current_binding_ = MakeOperatorNode(gexpr->Pop(), children);
}

bool GroupExprBindingIterator::HasNext() {
  if (has_next_ && first_) {
    first_ = false;
    return true;
  }

  if (has_next_) {
    // The first child to be modified
    int first_modified_idx = static_cast<int>(children_bindings_pos_.size()) - 1;
    for (; first_modified_idx >= 0; --first_modified_idx) {
      const auto &child_binding = children_bindings_[first_modified_idx];

      // Try to increment idx from the back
      size_t new_pos = ++children_bindings_pos_[first_modified_idx];
      if (new_pos >= child_binding.size()) {
        children_bindings_pos_[first_modified_idx] = 0;
      } else {
        break;
      }
    }

    if (first_modified_idx < 0) {
      // We have explored all combinations of the child bindings
      has_next_ = false;
    } else {
      OperatorNodeArray children;
      for (size_t idx = 0; idx < children_bindings_pos_.size(); ++idx) {
        const auto &child_binding = children_bindings_[idx];
        children.emplace_back(child_binding[children_bindings_pos_[idx]]);
      }

      current_binding_ = MakeOperatorNode(gexpr_->Pop(), children);
    }
  }

  return has_next_;
}

}  // namespace pgp
