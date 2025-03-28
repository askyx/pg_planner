#pragma once

#include <cstdint>
#include <format>
#include <ranges>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/hash_util.h"

extern "C" {
#include <access/attnum.h>
}
namespace pgp {

struct ColRef {
  uint32_t ref_id;

  Oid type;
  int32_t modifier;
  std::string name;
  bool nullable;
  uint32_t width;

  // for basetable
  AttrNumber attnum;

  std::string ToString() const { return std::format("{}#{}", name, ref_id); }

  bool operator==(const ColRef &cr) const { return ref_id == cr.ref_id; }
};

struct ColRefHash {
  size_t operator()(const ColRef *cr) const { return HashUtil::Hash(cr->ref_id); }
};

struct ColRefEqual {
  bool operator()(const ColRef *cr1, const ColRef *cr2) const { return cr1->ref_id == cr2->ref_id; }
};

using ColRefArray = std::vector<ColRef *>;
using ColRef2DArray = std::vector<ColRefArray>;

using ColRefSet = std::unordered_set<ColRef *, ColRefHash, ColRefEqual>;

inline ColRefSet &ColRefSetUnion(ColRefSet &cr1, const ColRefSet &cr2) {
  cr1.insert(cr2.begin(), cr2.end());
  return cr1;
}

inline ColRefSet &ColRefSetDifference(ColRefSet &cr1, const ColRefSet &cr2) {
  for (auto it = cr1.begin(); it != cr1.end();) {
    if (cr2.find(*it) != cr2.end())
      it = cr1.erase(it);
    else
      it++;
  }
  return cr1;
}

inline ColRefSet &ColRefSetIntersection(ColRefSet &cr1, const ColRefSet &cr2) {
  for (auto it = cr1.begin(); it != cr1.end();) {
    if (cr2.find(*it) == cr2.end())
      it = cr1.erase(it);
    else
      it++;
  }
  return cr1;
}

inline bool ColRefSetIntersects(const ColRefSet &cr1, const ColRefSet &cr2) {
  if (cr1.empty() || cr2.empty())
    return false;
  for (auto *colref : cr1)
    if (cr2.find(colref) != cr2.end())
      return true;

  return false;
}

inline bool ColRefSetIsDisjoint(const ColRefSet &cr1, const ColRefSet &cr2) {
  return !ColRefSetIntersects(cr1, cr2);
}

/*
 * cr1 contains all columns in cr2
 */
template <typename C>
concept ColRefConcept = (std::same_as<C, ColRefSet> || std::same_as<C, ColRefArray>);

template <ColRefConcept CON>
inline bool ContainsAll(const ColRefSet &cr1, const CON &cr2) {
  if (cr2.empty())  // empty set is always contained
    return true;
  for (auto *colref : cr2)
    if (cr1.find(colref) == cr1.end())
      return false;

  return true;
}

template <ColRefConcept CON>
inline void AddColRef(ColRefSet &cr, const CON &cr1) {
  cr.insert(cr1.begin(), cr1.end());
}

inline ColRefSet ColRefArrayToSet(const ColRefArray &cr) {
  ColRefSet result;
  for (auto *col_ref : cr) {
    result.insert(col_ref);
  }
  return result;
}

inline ColRefArray ColRefSetToArray(const ColRefSet &cr) {
  ColRefArray result;
  for (auto *col_ref : cr) {
    result.push_back(col_ref);
  }
  return result;
}

// TODO, this is real colrefset
struct ColRefSetWapper {
  ColRefSet col_refs;

  ColRefSetWapper() = default;

  ColRefSetWapper(ColRef *col_ref) : col_refs{col_ref} {}  // NOLINT

  ColRefSetWapper(ColRefSet col_ref_set) : col_refs{std::move(col_ref_set)} {}  // NOLINT

  ColRefSetWapper(ColRefArray col_ref_set) : col_refs{ColRefArrayToSet(col_ref_set)} {}  // NOLINT

  ColRefSetWapper &Union(const ColRefSetWapper &cr) {
    ColRefSetUnion(col_refs, cr.Get());
    return *this;
  }

  ColRefSetWapper &Difference(const ColRefSetWapper &cr) {
    ColRefSetDifference(col_refs, cr.Get());
    return *this;
  }

  ColRefSetWapper &Intersection(const ColRefSetWapper &cr) {
    ColRefSetIntersection(col_refs, cr.Get());
    return *this;
  }

  ColRefSetWapper &AddColRef(ColRef *col_ref) {
    col_refs.insert(col_ref);
    return *this;
  }

  template <ColRefConcept CON>
  ColRefSetWapper &AddColRef(const CON &cr1) {
    col_refs.insert(cr1.begin(), cr1.end());
    return *this;
  }

  const ColRefSet &Get() const { return col_refs; }

  ColRefArray ToArray() const { return ColRefSetToArray(col_refs); }
};

template <ColRefConcept CON>
inline std::string ColRefContainerToString(const CON &col_set) {
  std::string result;
  for (auto *col_ref : col_set) {
    std::string str = col_ref->ToString();
    result += str + ", ";
  }
  return result.substr(0, result.size() - 2);
}

template <ColRefConcept CON>
inline hash_t ColRefContainerHash(const CON &cr) {
  hash_t hash = 0;
  for (auto *col_ref : cr) {
    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(col_ref->ref_id));
  }
  return hash;
}

inline bool operator==(const ColRefArray &cr1, const ColRefArray &cr2) {
  if (cr1.size() != cr2.size()) {
    return false;
  }
  for (auto [c1, c2] : std::views::zip(cr1, cr2))
    if (c1->ref_id != c2->ref_id)
      return false;

  return true;
}

inline bool operator==(const ColRef2DArray &cr1, const ColRef2DArray &cr2) {
  if (cr1.size() != cr2.size()) {
    return false;
  }
  for (auto [arr1, arr2] : std::views::zip(cr1, cr2))
    if (arr1 != arr2)
      return false;

  return true;
}

}  // namespace pgp
