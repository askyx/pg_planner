#!/bin/bash
set -e

# Unit test for grcov coverage file generation

# Setup
echo "Setting up test environment..."
TEST_DIR="$(mktemp -d)"
trap 'rm -rf "$TEST_DIR"' EXIT

# Create mock build directory structure
mkdir -p "$TEST_DIR/build/Coverage"
touch "$TEST_DIR/build/Coverage/test_file.gcda"
touch "$TEST_DIR/build/Coverage/test_file.gcno"

cd "$TEST_DIR"

# Verify grcov is installed
if ! command -v grcov &> /dev/null; then
    echo "ERROR: grcov is not installed"
    exit 1
fi

# Test grcov command (similar to what's in the workflow)
echo "Testing grcov coverage file generation..."
grcov . -b build/Coverage -s . -o coverage.info -t lcov --branch --ignore-not-existing 2>/dev/null || {
    echo "ERROR: Failed to generate coverage file"
    exit 1
}

# Verify the coverage file was created
if [ ! -f "coverage.info" ]; then
    echo "ERROR: coverage.info file was not created"
    exit 1
fi

# Verify the coverage file has expected LCOV format
if ! grep -q "TN:" "coverage.info"; then
    echo "ERROR: coverage.info doesn't have the expected LCOV format"
    exit 1
fi

echo "PASS: grcov successfully generated coverage file in LCOV format"