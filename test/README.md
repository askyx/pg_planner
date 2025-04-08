# Code Coverage Tests

This directory contains tests for the code coverage functionality added to the GitHub Actions workflow.

## Test Files

- `test_grcov_coverage.sh`: Tests the generation of coverage reports using grcov
- `test_codecov_integration.sh`: Tests the integration with Codecov for uploading coverage reports

## Running the Tests

The tests can be run using CMake's test framework:

```bash
cd build/Coverage
ctest -R test_grcov_coverage
ctest -R test_codecov_integration
```

Or run all tests:

```bash
ctest
```

## Test Details

### grcov Coverage Test
This test verifies that grcov can correctly generate a coverage report in LCOV format from the build artifacts.

### Codecov Integration Test
This test verifies that the generated coverage file has the correct format for uploading to Codecov and that the Codecov GitHub Action is properly configured.