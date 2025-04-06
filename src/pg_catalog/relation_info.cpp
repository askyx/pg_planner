#include "pg_catalog/relation_info.h"

#include <bitset>

#include "access/sdir.h"
#include "common/exception.h"
#include "pg_optimizer/colref.h"

extern "C" {
#include <access/stratnum.h>
#include <utils/lsyscache.h>
#include <utils/typcache.h>
}

namespace pgp {
ScanDirection IndexInfo::GetScanDirection(const std::shared_ptr<OrderSpec> &order_spec) const {
  // if satisfy order clause, return  scan direction, backward or forward, else return Invalid
  std::bitset<2> order_drection_bitset("00");
  std::bitset<2> index_drection_bitset("00");
  PGP_ASSERT(reverse_sort.size() == null_first.size(), "reverse_sort and null_first size not equal");
  for (const auto &[x, y] : std::views::zip(reverse_sort, null_first)) {
    if (x)
      order_drection_bitset.set(1);  // desc
    if (y)
      order_drection_bitset.set(0);  // null first
  }

  for (const auto &sort_ele : order_spec->GetSortArray()) {
    auto *typeentry = lookup_type_cache(sort_ele.colref->type, TYPECACHE_GT_OPR);
    if (typeentry->gt_opr == sort_ele.sort_op)
      index_drection_bitset.set(1);  // desc

    if (sort_ele.nulls_first)
      index_drection_bitset.set(0);  // null first
  }

  if (order_drection_bitset == index_drection_bitset)
    return ForwardScanDirection;
  if ((order_drection_bitset ^ index_drection_bitset) == 3)
    return BackwardScanDirection;
  return NoMovementScanDirection;
}

std::shared_ptr<OrderSpec> IndexInfo::ConstructOrderSpec() const {
  auto order_spec = std::make_shared<OrderSpec>();
  for (auto [colref, opfamily, optype, reverse_sort, null_first] :
       std::views::zip(index_cols, sortopfamily, opcintype, reverse_sort, null_first)) {
    auto sortop =
        get_opfamily_member(opfamily, optype, optype, reverse_sort ? BTGreaterStrategyNumber : BTLessStrategyNumber);
    order_spec->AddSortElement({sortop, colref, null_first});
  }
  return order_spec;
}

std::string IndexInfo::ToString() const {
  std::string result = "index: " + std::to_string(index);
  result += " relam: " + std::to_string(relam);
  result += " index_cols: [";
  result += ColRefContainerToString(index_cols);
  result += "]";
  result += " index_include: [";
  result += ColRefContainerToString(index_include);
  result += "]";
  result += " sortopfamily: [";
  for (auto sortop : sortopfamily) {
    result += std::to_string(sortop) + " ";
  }
  result += "]";
  result += " opcintype: [";
  for (auto opcintype : opcintype) {
    result += std::to_string(opcintype) + " ";
  }
  result += "]";
  result += " reverse_sort: [";
  for (auto reverse_sort : reverse_sort) {
    result += reverse_sort ? "true " : "false ";
  }
  result += "]";
  result += " null_first: [";
  for (auto null_first : null_first) {
    result += null_first ? "true " : "false ";
  }
  result += "]";

  return result;
}
std::string RelationInfo::ToString() const {
  std::string result = "output_columns: [";
  result += ColRefContainerToString(output_columns);
  result += "]\n";
  result += "index_list: [\n";
  for (const auto &index : index_list) {
    result += "\t" + index.ToString();
    result += "\n";
  }
  result += "]";
  return result;
}

}  // namespace pgp