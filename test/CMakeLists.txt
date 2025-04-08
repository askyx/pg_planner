# CMake test configuration for code coverage tests

# Add the test directory to the build
cmake_minimum_required(VERSION 3.10)

# Add tests for the code coverage functionality
add_test(
  NAME test_grcov_coverage
  COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test_grcov_coverage.sh
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)

add_test(
  NAME test_codecov_integration
  COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test_codecov_integration.sh
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)

# Make the test scripts executable
file(CHMOD ${CMAKE_CURRENT_SOURCE_DIR}/test_grcov_coverage.sh
     PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
file(CHMOD ${CMAKE_CURRENT_SOURCE_DIR}/test_codecov_integration.sh
     PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)