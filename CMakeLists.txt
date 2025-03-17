cmake_minimum_required(VERSION 3.20)
project(pg_planner LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS on)
set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)

enable_testing()

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "RelWithDebInfo")
endif(NOT CMAKE_BUILD_TYPE)
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

# grcov . -b build/Coverage -s . -o coverage -t html --branch --ignore-not-existing
option(ENABLE_COVERAGE "Enable coverage reporting" OFF)
option(ENABLE_UNITY_BUILD "Enable unity build" ON)
option(ENABLE_BUILDIN_TEST "Enable build-in test" OFF)

# set(CMAKE_BUILD_PARALLEL_LEVEL 15)

find_package(PostgreSQL REQUIRED)

if(NOT ${PostgreSQL_VERSION_STRING} STREQUAL "PostgreSQL 17beta1")
  message(FATAL_ERROR "Developing, Only PostgreSQL 17beta1 is supported, other versions are not supported now.")
endif()

set(PostgreSQL_COMPILE_OPTIONS
  ${PostgreSQL_COMPILE_OPTIONS}
    -Werror
    -Wall
    -fvisibility=hidden
    -fvisibility-inlines-hidden
)

if(ENABLE_COVERAGE)
  if(NOT "${CMAKE_BUILD_TYPE}" STREQUAL "Debug")                      # If coverage is required on non-DEBUG builds,
    message(FATAL_ERROR "Coverage requires a debug build type!")      # Then error out.
  endif()
  if(${ENABLE_UNITY_BUILD})
    message(FATAL_ERROR "Coverage requires a non-unity build!")
  endif()
  set(PostgreSQL_COMPILE_OPTIONS ${PostgreSQL_COMPILE_OPTIONS} --coverage)
  set(PostgreSQL_LINK_OPTIONS ${PostgreSQL_LINK_OPTIONS} --coverage)
endif()
message(STATUS "Coverage ${ENABLE_COVERAGE}")

# std::format need locale support
set(PostgreSQL_COMPILE_DEFINE ${PostgreSQL_COMPILE_DEFINE} ENABLE_NLS)
if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  set(PostgreSQL_COMPILE_DEFINE ${PostgreSQL_COMPILE_DEFINE} PLAN_DEBUG=1)
else()
  set(PostgreSQL_COMPILE_DEFINE ${PostgreSQL_COMPILE_DEFINE} PLAN_DEBUG=0)
endif()

file(
  GLOB_RECURSE
  PLANNER_CORE_SRC
  CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp 
)

file(
  GLOB_RECURSE
  PLANNER_TEST_SRC
  CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/test/test/*.cpp
)

if(NOT ${ENABLE_BUILDIN_TEST})
  set(PLANNER_TEST_SRC "")
endif()

add_postgresql_extension(
  pg_planner
    VERSION 1.0
    SOURCES ${PLANNER_CORE_SRC}
    HEADERS
      ${CMAKE_CURRENT_SOURCE_DIR}/include
      ${CMAKE_CURRENT_SOURCE_DIR}/test/test_utils
    SCRIPTS
      src/pg_planner--1.0.sql
    TEST_SOURCE ${PLANNER_TEST_SRC}
)

set_target_properties(
  pg_planner
    PROPERTIES
      UNITY_BUILD ${ENABLE_UNITY_BUILD}
)

# format
add_custom_target(format
  COMMAND git diff --name-only --diff-filter=d --cached '*.cpp' '*.h' | xargs clang-format -i
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
)


# clang-tidy
add_custom_target(check-tidy
  COMMAND run-clang-tidy -p build/Debug -config-file=./.clang-tidy
  COMMENT "Running Clang-Tidy"
  VERBATIM
)

# check-format
add_custom_target(check-format
  COMMAND clang-format --style=file --dry-run -Werror -i ${PLANNER_CORE_SRC}
  VERBATIM
)