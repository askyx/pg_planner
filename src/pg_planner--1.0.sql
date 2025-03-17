-- load 'pg_planner.so';
-- 为什么有的插件直接安装就可以直接使用，而有的还需要重启

CREATE FUNCTION transform_query(text)
RETURNS cstring
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- Test | Status | Details 
CREATE FUNCTION planner_test(
  OUT "Test" TEXT,
  OUT "Status" TEXT,
  OUT "Details" TEXT
) RETURNS SETOF record AS 'MODULE_PATHNAME',
'planner_test' LANGUAGE C STRICT IMMUTABLE;

CREATE VIEW planner_test AS
SELECT
  "Test",
  "Status",
  "Details"
FROM
  planner_test();