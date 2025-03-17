#include <cstdint>
#include <iostream>

#include "common/exception.h"
#include "common/pg_guard.h"
#include "pg_test.h"

extern "C" {
#include <postgres.h>

#include <nodes/bitmapset.h>
}

#include "pg_optimizer/colref.h"

namespace pg_test {

TEST(CommonTest, test_exception) {  // NOLINT

  int32_t a = 1;

  auto b = PG_GUARD(a * 2);

  EXPECT_EQ(b, 2);

  try {
    PG_GUARD_VOID(elog(ERROR, "test exception..."));
  } catch (const pgp::Exception& e) {
    EXPECT_STREQ(e.what(), "test exception...");
  }

  // jump out from the scope of PG_GUARD, the exception will be thrown
  PG_GUARD_VOID(elog(ERROR, "test exception..."));
}

TEST(CommonTest, test_bitmapset) {  // NOLINT

  Bitmapset* bitmapset = bms_make_singleton(1);

  EXPECT_TRUE(bms_is_member(1, bitmapset));

  bitmapset = bms_add_member(bitmapset, 2);
  bitmapset = bms_add_member(bitmapset, 5);
  bitmapset = bms_add_member(bitmapset, 4);
  bitmapset = bms_add_member(bitmapset, 5);

  std::cout << "bitmapset: " << bmsToString(bitmapset) << std::endl;

  std::cout << "bitmapset size: " << bms_num_members(bitmapset) << std::endl;

  std::cout << "bitmapset index of 5: " << bms_member_index(bitmapset, 5) << std::endl;
  std::cout << "bitmapset index of 4: " << bms_member_index(bitmapset, 4) << std::endl;
}

TEST(CommonTest, test_colref_set) {  // NOLINT

  auto* c1 = new pgp::ColRef(1, 1, 1, "a");
  auto* c2 = new pgp::ColRef(2, 2, 2, "b");
  auto* c3 = new pgp::ColRef(3, 3, 3, "c");

  pgp::ColRefSet col_set{c1, c2, c3};
  std::cout << "col_set: " << pgp::ColRefContainerToString(col_set) << std::endl;

  auto* a2 = new pgp::ColRef(2, 2, 2, "b");
  auto* a3 = new pgp::ColRef(3, 3, 3, "c");
  auto* a4 = new pgp::ColRef(4, 4, 4, "d");

  pgp::ColRefSet col_set_x{a2, a3, a4};
  std::cout << "col_set_x: " << pgp::ColRefContainerToString(col_set_x) << std::endl;
  EXPECT_TRUE(ColRefSetIntersects(col_set, col_set_x));

  auto col_set_copy = col_set;

  pgp::ColRefSetUnion(col_set, col_set_x);
  std::cout << "union_set: " << pgp::ColRefContainerToString(col_set) << std::endl;
  EXPECT_TRUE(ContainsAll(col_set, col_set_x));
  EXPECT_TRUE(ContainsAll(col_set, col_set_copy));
  pgp::ColRefSetDifference(col_set, col_set_x);
  std::cout << "diff_set: " << pgp::ColRefContainerToString(col_set) << std::endl;
  EXPECT_TRUE(ColRefSetIsDisjoint(col_set, col_set_x));
  pgp::ColRefSetIntersection(col_set, col_set_x);
  std::cout << "inter_set: " << pgp::ColRefContainerToString(col_set) << std::endl;

  // test empty set
  pgp::ColRefSet empty_1;
  pgp::ColRefSet empty_2;
  // empty set contains nothing, return false
  EXPECT_FALSE(ContainsAll(empty_1, col_set_copy));
  // except empty set, return true
  EXPECT_TRUE(ContainsAll(empty_1, empty_2));
  // contained nothing, return true
  EXPECT_TRUE(ContainsAll(col_set_copy, empty_2));

  // empty set intersects with empty set, return false
  EXPECT_FALSE(ColRefSetIntersects(empty_1, empty_2));
  EXPECT_FALSE(ColRefSetIntersects(col_set_copy, empty_2));
  EXPECT_FALSE(ColRefSetIntersects(empty_1, col_set_copy));

  // empty set ColRefSetIsDisjoint with any set, return true
  EXPECT_TRUE(ColRefSetIsDisjoint(empty_1, empty_2));
  EXPECT_TRUE(ColRefSetIsDisjoint(col_set_copy, empty_2));
  EXPECT_TRUE(ColRefSetIsDisjoint(empty_1, col_set_copy));
}

}  // namespace pg_test