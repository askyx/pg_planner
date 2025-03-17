#pragma once

#include <unordered_set>
#include <vector>

#include "common/macros.h"
#include "pg_operator/operator_node.h"
#include "pg_optimizer/group_expression.h"

namespace pgp {
class Group;

class Memo {
 private:
  std::vector<Group *> groups_;

  std::unordered_set<GroupExpression *, GroupExpressionHasher, GroupExpressionEqualer> memo_expressions_;

 public:
  DISALLOW_COPY_AND_MOVE(Memo)

  Memo() = default;

  ~Memo();

  std::string ToString() const;

  GroupExpression *InsertExpression(OperatorNode *node, GroupExpression *gexpr, Group *target_group, bool enforced);

  GroupExpression *InsertExpression(OperatorNode *node, GroupExpression *gexpr, bool enforced) {
    return InsertExpression(node, gexpr, nullptr, enforced);
  }

  Group *AddNewGroup(OperatorNode *node, GroupExpression *gexpr);
};

}  // namespace pgp
