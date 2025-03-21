#include "pg_optimizer/optimizer_context.h"

#include "pg_operator/operator.h"
#include "pg_optimizer/group.h"
#include "pg_optimizer/pattern.h"
#include "pg_optimizer/rule.h"

namespace pgp {

OptimizerContext *OptimizerContext::optimizer_context = nullptr;

GroupExpression *OptimizerContext::MakeGroupExpression(const OperatorNodePtr &node) {
  std::vector<Group *> child_groups;
  for (const auto &child : node->children) {
    if (child->content->kind == OperatorType::LEAF) {
      // Special case for LEAF
      auto &leaf = child->Cast<LeafOperator>();
      auto *child_group = leaf.GetOriginGroup();
      child_groups.push_back(child_group);
    } else {
      // Create a GroupExpression for the child
      auto *gexpr = MakeGroupExpression(child);

      // Insert into the memo (this allows for duplicate detection)
      auto *mexpr = memo_.InsertExpression(child, gexpr, false);
      if (mexpr == nullptr) {
        // Delete if need to (see InsertExpression spec)
        child_groups.push_back(gexpr->GetGroup());
        delete gexpr;
      } else {
        child_groups.push_back(mexpr->GetGroup());
      }
    }
  }
  return new GroupExpression(node->content, child_groups);
}

}  // namespace pgp