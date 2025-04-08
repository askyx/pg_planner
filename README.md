# pg_planner (PGP) - Cascades-based Query Optimizer Extension for PostgreSQL

[![PostgreSQL Version](https://img.shields.io/badge/PostgreSQL-17beta1+-blue.svg)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![codecov](https://codecov.io/gh/askyx/pg_planner/graph/badge.svg?token=X3EZ36DF3V)](https://codecov.io/gh/askyx/pg_planner)

A non-intrusive, cascades framework-based query optimizer extension for PostgreSQL that enhances query planning capabilities while maintaining full compatibility with native PostgreSQL features.


- **Cascades Optimization Framework**  
- **Modern C++ & Build System**
- **PostgreSQL Non-Intrusive Extension**

## Installation

### Prerequisites
- PostgreSQL 17beta1+ development files
- CMake 3.15+
- C++23 compatible compiler

### Build & Install
```bash
cmake --preset Debug
cmake --build build --target install
cmake --build build --target test
```

[PG_TPCH](https://github.com/askyx/pg_tpch.git) extension is required (available in my repository):

## Usage

Choose one of these activation methods:

1. **Runtime Loading (Recommended)**
   ```sql
   LOAD 'pg_planner';
   SET pg_planner.enable_planner TO true;
   -- Your SQL queries will use pg_planner automatically, only tpch query is supported for now.
   ```

2. **Preload Configuration**  
   Add to postgresql.conf:
   ```ini
   shared_preload_libraries = 'pg_planner'
   ```
   *Note: NOT RECOMMENDED. THIS REPOSTORY IS DEVELOPING AND NOT STABLE YET.*

## Roadmap

### Phase 1: Core Optimization
- [x] Cascades framework integration
- [x] Basic relational operator support
- [ ] TPCH benchmark completion
  - [ ] Enhanced scan methods (Index Scan, Index Only Scan, Bitmap Scan...)
  - [ ] Join method expansion (Hash Join, Merge Join, Nested Loop Join)
  - [ ] Cost model integration with postgres statistics

### Phase 2: Advanced Features
- [ ] TPCDS/TPCC benchmark support
- [ ] Parallel query optimization
- [ ] Adaptive optimization techniques

### Phase 3: Full Compatibility
- [ ] 100% PostgreSQL test suite pass rate
- [ ] Automatic plan correction
- [ ] Multi-version PostgreSQL support
