#pragma once

#include <sys/types.h>

extern "C" {
#include <postgres.h>

#include <nodes/pg_list.h>
#include <utils/memutils.h>
}

#include <string_view>

#include "common/exception.h"

namespace pg_test {

constexpr const char* BaseName(std::string_view path) {
  std::size_t pos = path.find_last_of("/\\");
  if (pos != std::string_view::npos) {
    return path.substr(pos + 1).data();
  }
  return path.data();
}

#define TEST_STRINGIFY_HELPER_(name, ...) #name
#define TEST_STRINGIFY_(...) TEST_STRINGIFY_HELPER_(__VA_ARGS__, )

#define TEST(test_suite_name, test_name)                                                            \
  static_assert(sizeof(TEST_STRINGIFY_(test_suite_name)) > 1, "test_suite_name must not be empty"); \
  static_assert(sizeof(TEST_STRINGIFY_(test_name)) > 1, "test_name must not be empty");             \
  struct test_name : public pg_test::PGTestSuite {                                                  \
    const char* __test_name = TEST_STRINGIFY_(test_suite_name.test_name);                           \
    pg_test::PGTestStatus status{                                                                   \
        .success = true,                                                                            \
        .name = __test_name,                                                                        \
        .status = "Passed",                                                                         \
        .details = "",                                                                              \
    };                                                                                              \
    pg_test::PGTestStatus& Status() override {                                                      \
      return status;                                                                                \
    }                                                                                               \
    test_name() {                                                                                   \
      pg_test::PGTest::PGTestInstance().AddTestSuite(this);                                         \
    }                                                                                               \
                                                                                                    \
    void Run() override;                                                                            \
  };                                                                                                \
  static test_name test_suite_name##test_name;                                                      \
                                                                                                    \
  void test_name::Run()

#ifdef __INTEL_COMPILER
#define GTEST_AMBIGUOUS_ELSE_BLOCKER_
#else
#define GTEST_AMBIGUOUS_ELSE_BLOCKER_ \
  switch (0)                          \
  case 0:                             \
  default:  // NOLINT
#endif

#define GTEST_ASSERT_(expression, on_failure)                \
  GTEST_AMBIGUOUS_ELSE_BLOCKER_                              \
  if (const ::pg_test::PGTestStatus gtest_ar = (expression)) \
    ;                                                        \
  else {                                                     \
    on_failure(gtest_ar) return;                             \
  }

#define GTEST_TEST_BOOLEAN_(expression, text, actual, expected, fail)                    \
  GTEST_AMBIGUOUS_ELSE_BLOCKER_                                                          \
  if (const ::pg_test::PGTestStatus gtest_ar_{.success = (expression), .details = text}) \
    ;                                                                                    \
  else {                                                                                 \
    fail(gtest_ar_) return;                                                              \
  }

#define GTEST_NONFATAL_FAILURE_(message)             \
  status = {.success = false,                        \
            .name = __test_name,                     \
            .status = "Failed",                      \
            .details = message.details,              \
            .location = pg_test::BaseName(__FILE__), \
            .line = __LINE__};

#define FAIL(msg)                                      \
  {                                                    \
    status = {.success = false,                        \
              .name = __test_name,                     \
              .status = "Failed",                      \
              .details = msg,                          \
              .location = pg_test::BaseName(__FILE__), \
              .line = __LINE__};                       \
    return;                                            \
  }

#define GTEST_PRED_FORMAT2_(pred_format, v1, v2, op, on_failure) \
  GTEST_ASSERT_(pred_format(TEST_STRINGIFY_(v1 op v2), v1, v2), on_failure)

#define EXPECT_PRED_FORMAT2(pred_format, v1, v2, op) \
  GTEST_PRED_FORMAT2_(pred_format, v1, v2, op, GTEST_NONFATAL_FAILURE_)

#define GTEST_EXPECT_TRUE(condition) GTEST_TEST_BOOLEAN_(condition, #condition, false, true, GTEST_NONFATAL_FAILURE_)
#define GTEST_EXPECT_FALSE(condition) \
  GTEST_TEST_BOOLEAN_(!(condition), #condition, true, false, GTEST_NONFATAL_FAILURE_)

#define EXPECT_EQ(val1, val2) EXPECT_PRED_FORMAT2(::pg_test::EqHelper::Compare, val1, val2, ==)
#define EXPECT_NE(val1, val2) EXPECT_PRED_FORMAT2(::pg_test::CmpHelperNE, val1, val2, !=)
#define EXPECT_LE(val1, val2) EXPECT_PRED_FORMAT2(::pg_test::CmpHelperLE, val1, val2, <=)
#define EXPECT_LT(val1, val2) EXPECT_PRED_FORMAT2(::pg_test::CmpHelperLT, val1, val2, <)
#define EXPECT_GE(val1, val2) EXPECT_PRED_FORMAT2(::pg_test::CmpHelperGE, val1, val2, >=)
#define EXPECT_GT(val1, val2) EXPECT_PRED_FORMAT2(::pg_test::CmpHelperGT, val1, val2, >)

#define EXPECT_TRUE(condition) GTEST_EXPECT_TRUE(condition)
#define EXPECT_FALSE(condition) GTEST_EXPECT_FALSE(condition)

// #define EXPECT_FLOAT_EQ(val1, val2)
//   EXPECT_PRED_FORMAT2(::testing::internal::CmpHelperFloatingPointEQ<float>, val1, val2)

// #define EXPECT_DOUBLE_EQ(val1, val2)
//   EXPECT_PRED_FORMAT2(::testing::internal::CmpHelperFloatingPointEQ<double>, val1, val2)

// #define EXPECT_NEAR(val1, val2, abs_error)
//   EXPECT_PRED_FORMAT3(::testing::internal::DoubleNearPredFormat, val1, val2, abs_error)

// #define ASSERT_NEAR(val1, val2, abs_error)
//   ASSERT_PRED_FORMAT3(::testing::internal::DoubleNearPredFormat, val1, val2, abs_error)

#define EXPECT_STREQ(s1, s2) EXPECT_PRED_FORMAT2(::pg_test::CmpHelperSTREQ, s1, s2, ==)
#define EXPECT_STRNE(s1, s2) EXPECT_PRED_FORMAT2(::pg_test::CmpHelperSTRNE, s1, s2, !=)
#define EXPECT_STRCASEEQ(s1, s2) EXPECT_PRED_FORMAT2(::pg_test::CmpHelperSTRCASEEQ, s1, s2, ==)
#define EXPECT_STRCASENE(s1, s2) EXPECT_PRED_FORMAT2(::pg_test::CmpHelperSTRCASENE, s1, s2, !=)

struct PGTestStatus {
  explicit operator bool() const { return success; }
  bool success;
  const char* name;
  const char* status;
  // strdup
  const char* details;
  const char* location;
  u_int32_t line;
};

#define OK                \
  pg_test::PGTestStatus { \
    .success = true       \
  }

#define NOTOK(msg)                   \
  pg_test::PGTestStatus {            \
    .success = false, .details = msg \
  }

#define GTEST_IMPL_CMP_HELPER_(op_name, op)                                                          \
  template <typename T1, typename T2>                                                                \
  ::pg_test::PGTestStatus CmpHelper##op_name(const char* expr_str, const T1& val1, const T2& val2) { \
    if (val1 op val2) {                                                                              \
      return OK;                                                                                     \
    }                                                                                                \
    return NOTOK(expr_str);                                                                          \
  }

// INTERNAL IMPLEMENTATION - DO NOT USE IN A USER PROGRAM.

// Implements the helper function for {ASSERT|EXPECT}_NE
GTEST_IMPL_CMP_HELPER_(NE, !=)
// Implements the helper function for {ASSERT|EXPECT}_LE
GTEST_IMPL_CMP_HELPER_(LE, <=)
// Implements the helper function for {ASSERT|EXPECT}_LT
GTEST_IMPL_CMP_HELPER_(LT, <)
// Implements the helper function for {ASSERT|EXPECT}_GE
GTEST_IMPL_CMP_HELPER_(GE, >=)
// Implements the helper function for {ASSERT|EXPECT}_GT
GTEST_IMPL_CMP_HELPER_(GT, >)

#undef GTEST_IMPL_CMP_HELPER_

#define GTEST_STR_HELPER_INTERNAL_(op_type, type, cmp_func)       \
  inline bool op_type##Equals(const type* lhs, const type* rhs) { \
    if (lhs == nullptr)                                           \
      return rhs == nullptr;                                      \
                                                                  \
    if (rhs == nullptr)                                           \
      return false;                                               \
                                                                  \
    return cmp_func(lhs, rhs) == 0;                               \
  }

GTEST_STR_HELPER_INTERNAL_(CString, char, strcmp)
GTEST_STR_HELPER_INTERNAL_(WideCString, wchar_t, wcscmp)
GTEST_STR_HELPER_INTERNAL_(CaseInsensitiveCString, char, strcasecmp)

#define GTEST_IMPL_CMP_STR_HELPER_(op_name, str_cmp_func, op, op_type)                             \
  inline ::pg_test::PGTestStatus CmpHelperSTR##op_name(const char* expression, const op_type* lhs, \
                                                       const op_type* rhs) {                       \
    if (str_cmp_func(lhs, rhs) op true) {                                                          \
      return OK;                                                                                   \
    }                                                                                              \
                                                                                                   \
    return NOTOK(expression);                                                                      \
  }

// The helper function for {ASSERT|EXPECT}_STREQ.
GTEST_IMPL_CMP_STR_HELPER_(EQ, CStringEquals, ==, char)
// The helper function for {ASSERT|EXPECT}_STREQ on wide strings.
GTEST_IMPL_CMP_STR_HELPER_(EQ, WideCStringEquals, ==, wchar_t)
// The helper function for {ASSERT|EXPECT}_STRNE.
GTEST_IMPL_CMP_STR_HELPER_(NE, CStringEquals, !=, char)
// The helper function for {ASSERT|EXPECT}_STRNE on wide strings.
GTEST_IMPL_CMP_STR_HELPER_(NE, WideCStringEquals, !=, wchar_t)
// The helper function for {ASSERT|EXPECT}_STRCASEEQ.
GTEST_IMPL_CMP_STR_HELPER_(CASEEQ, CaseInsensitiveCStringEquals, ==, char)
// The helper function for {ASSERT|EXPECT}_STRCASENE.
GTEST_IMPL_CMP_STR_HELPER_(CASENE, CaseInsensitiveCStringEquals, !=, char)

#undef GTEST_IMPL_CMP_STR_HELPER_
#undef GTEST_STR_HELPER_INTERNAL_

template <typename T1, typename T2>
pg_test::PGTestStatus CmpHelperEQ(const char* cmp_expression, const T1& lhs, const T2& rhs) {
  if (lhs == rhs) {
    return pg_test::PGTestStatus{.success = true};
  }

  return pg_test::PGTestStatus{.success = false, .details = cmp_expression};
}

class EqHelper {
 public:
  // This templatized version is for the general case.
  template <typename T1, typename T2,
            // Disable this overload for cases where one argument is a pointer
            // and the other is the null pointer constant.
            typename std::enable_if<!std::is_integral<T1>::value || !std::is_pointer<T2>::value>::type* = nullptr>
  static pg_test::PGTestStatus Compare(const char* cmp_expression, const T1& lhs, const T2& rhs) {
    return CmpHelperEQ(cmp_expression, lhs, rhs);
  }

  template <typename T>
  static pg_test::PGTestStatus Compare(const char* cmp_expression,
                                       // Handle cases where '0' is used as a null pointer literal.
                                       std::nullptr_t, T* rhs) {
    // We already know that 'lhs' is a null pointer.
    return CmpHelperEQ(cmp_expression, static_cast<T*>(nullptr), rhs);
  }
};

class PGTestSuite {
 public:
  PGTestSuite() = default;
  virtual ~PGTestSuite() = default;

  void Test() {
    try {
      Run();
    } catch (pgp::Exception& e) {
      Status().success = false;
      Status().status = "Failed";
      Status().details = strdup(e.what());
      Status().line = e.line;
      Status().location = e.filename;
      FlushErrorState();
    } catch (std::exception& e) {
      Status().success = false;
      Status().status = "Failed";
      Status().details = e.what();
      FlushErrorState();
    } catch (...) {
      Status().success = false;
      Status().details = "Unknown exception";
      FlushErrorState();
    }
  }
  virtual void Run() = 0;
  // TODO: return a vector of test results, support expect/assert failure
  virtual PGTestStatus& Status() = 0;
};

class PGTest {
 private:
  List* test_suites_;

 public:
  PGTest() = default;
  ~PGTest() { list_free(test_suites_); }

  static PGTest& PGTestInstance() {
    static PGTest instance;
    return instance;
  }

  List* GetTestSuites() { return test_suites_; }

  void AddTestSuite(PGTestSuite* suite) {
    auto* old_ctx = MemoryContextSwitchTo(TopMemoryContext);
    test_suites_ = lappend(test_suites_, suite);
    MemoryContextSwitchTo(old_ctx);
  }
};

}  // namespace pg_test