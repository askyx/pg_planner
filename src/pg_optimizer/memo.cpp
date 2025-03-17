#include "pg_optimizer/memo.h"

#include <cstdint>

#include "common/exception.h"
#include "pg_operator/operator.h"
#include "pg_optimizer/colref.h"
#include "pg_optimizer/group.h"
#include "pg_optimizer/operator_prop.h"
#include "pg_optimizer/optimization_context.h"
#include "pg_optimizer/pattern.h"

namespace pgp {

#define GPOPT_MEMO_HT_BUCKETS 50000

Memo::~Memo() {
  for (auto *group : groups_)
    delete group;
}

GroupExpression *Memo::InsertExpression(OperatorNode *node, GroupExpression *gexpr, Group *target_group,
                                        bool enforced) {
  // If leaf, then just return
  if (gexpr->Pop()->kind == OperatorType::LEAF) {
    const auto &leaf = gexpr->Pop()->Cast<LeafOperator>();
    gexpr->SetGroup(leaf.GetOriginGroup());

    // Let the caller delete!
    // Caller needs the origin_group
    return nullptr;
  }

  gexpr->SetGroup(target_group);
  // Lookup in hash table
  auto it = memo_expressions_.find(gexpr);
  if (it != memo_expressions_.end()) {
    PGP_ASSERT(*gexpr == *(*it), "GroupExpression should be equal");
    delete gexpr;
    return *it;
  }

  memo_expressions_.insert(gexpr);

  // New expression, so try to insert into an existing group or
  // create a new group if none specified
  Group *group;
  if (target_group == nullptr) {
    group = AddNewGroup(node, gexpr);
  } else {
    group = target_group;
  }

  group->AddExpression(gexpr, enforced);
  return gexpr;
}

Group *Memo::AddNewGroup(OperatorNode *node, GroupExpression *gexpr) {
  auto new_group_id = (int32_t)groups_.size();

  auto *group = new Group(new_group_id, node->PdpDerive());
  groups_.push_back(group);
  return group;
}

std::string Memo::ToString() const {
  auto print_group = [](const Group *group) {
    std::string str_group = std::format("G{}: output {} \n", group->GetGroupId(),
                                        ColRefContainerToString(group->GroupProperties()->GetOutputColumns()));
    for (const auto &expr : group->ToString()) {
      str_group += "  " + expr + "\n";
    }
    return str_group;
  };

  std::string s{"memo:\n"};
  for (auto *group : groups_)
    s += print_group(group);

  return s;
}
}  // namespace pgp