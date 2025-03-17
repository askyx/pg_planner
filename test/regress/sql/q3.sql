select query from tpch_queries(3); \gset

select transform_query(query) from tpch_queries(3);

set pg_planner.enable_planner to on;


explain verbose :query
:query

set pg_planner.enable_planner to off;

explain verbose :query
:query
