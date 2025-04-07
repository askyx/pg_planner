----------------------------
-- test for btree index
----------------------------
drop table if exists t1;
create table t1 (a int, b int, c int);
create index idx on t1 (a);                 -- eq to (a asc nulls last);
create index idx_asc_1 on t1 (a asc);       -- eq to (a asc nulls last);
create index idx_asc_2 on t1 (a asc nulls first);
create index idx_asc_3 on t1 (a asc nulls last) include (b);
create index idx_desc_1 on t1 (a desc);      -- eq to (a desc nulls first);
create index idx_desc_2 on t1 (a desc nulls first);
create index idx_desc_3 on t1 (a desc nulls last) include (c);
create index idx_asc_nulls_first on t1 (a nulls first) include(c,b); -- eq to (a asc nulls first);
create index idx_asc_nulls_last on t1 (a nulls last);                -- eq to (a asc nulls last);
\d+ t1
insert into t1 values (1, 2, 3), (null, 5, 6), (7, 8, 9);
insert into t1 values (null, 11, 12), (13, 14, 15), (null, 17, 18);

set pg_planner.enable_planner to on;

-- ~(idx | idx_asc_1 | idx_asc_3 | idx_asc_nulls_last)
explain select * from t1 order by a;
-- ~(idx_desc_1 | idx_desc_2) 
explain select * from t1 order by a desc;
-- ~(idx_desc_1 | idx_desc_2) 
explain select * from t1 order by a desc nulls first;
explain select * from t1 order by a desc nulls last;
explain select * from t1 order by a asc;
explain select * from t1 order by a asc nulls first;
explain select * from t1 order by a asc nulls last;
explain select * from t1 order by a nulls first;
explain select * from t1 order by a nulls last;

select * from t1 order by a;
select * from t1 order by a desc;
select * from t1 order by a desc nulls first;
select * from t1 order by a desc nulls last;
select * from t1 order by a asc;
select * from t1 order by a asc nulls first;
select * from t1 order by a asc nulls last;
select * from t1 order by a nulls first;
select * from t1 order by a nulls last;

set pg_planner.enable_planner to off;

-- ~(idx | idx_asc_1 | idx_asc_3 | idx_asc_nulls_last)
explain select * from t1 order by a;
-- ~(idx_desc_1 | idx_desc_2) 
explain select * from t1 order by a desc;
-- ~(idx_desc_1 | idx_desc_2) 
explain select * from t1 order by a desc nulls first;
explain select * from t1 order by a desc nulls last;
explain select * from t1 order by a asc;
explain select * from t1 order by a asc nulls first;
explain select * from t1 order by a asc nulls last;
explain select * from t1 order by a nulls first;
explain select * from t1 order by a nulls last;

select * from t1 order by a;
select * from t1 order by a desc;
select * from t1 order by a desc nulls first;
select * from t1 order by a desc nulls last;
select * from t1 order by a asc;
select * from t1 order by a asc nulls first;
select * from t1 order by a asc nulls last;
select * from t1 order by a nulls first;
select * from t1 order by a nulls last;




drop table if exists t1;
create table t1 (a int, b int, c int, d int, e boolean);

create index idxx1 on t1 (a, b);
create index idxx2 on t1 (e);
insert into t1 values (1, 2, 3, 4, true), (5, 6, 7, 8, false), (9, 10, 11, 12, true), (13, 14, 15, 16, false);

-- index scan
explain select * from t1 where a = 1 and b = 2;

-- index scan
explain select * from t1 order by a;
explain select * from t1 order by a, b;
-- Incremental sort
explain select * from t1 order by a, b, c;
-- not index scan
explain select * from t1 order by b, a;
explain select * from t1 order by b;

explain select * from t1 where a = 1 and b = 2 order by a, b;
explain select * from t1 where a = 1 and b = 2 order by b, a;
explain select * from t1 where e;


-- index scan
select * from t1 where a = 1 and b = 2;

-- index scan
select * from t1 order by a;
select * from t1 order by a, b;
-- Incremental sort
select * from t1 order by a, b, c;
-- not index scan
select * from t1 order by b, a;
select * from t1 order by b;

select * from t1 where a = 1 and b = 2 order by a, b;
select * from t1 where a = 1 and b = 2 order by b, a;
select * from t1 where e;

set pg_planner.enable_planner to on;

-- index scan
explain select * from t1 where a = 1 and b = 2;

-- index scan
explain select * from t1 order by a;
explain select * from t1 order by a, b;
-- Incremental sort
explain select * from t1 order by a, b, c;
-- not index scan
explain select * from t1 order by b, a;
explain select * from t1 order by b;

explain select * from t1 where a = 1 and b = 2 order by a, b;
explain select * from t1 where a = 1 and b = 2 order by b, a;
explain select * from t1 where e;


-- index scan
select * from t1 where a = 1 and b = 2;

-- index scan
select * from t1 order by a;
select * from t1 order by a, b;
-- Incremental sort
select * from t1 order by a, b, c;
-- not index scan
select * from t1 order by b, a;
select * from t1 order by b;

select * from t1 where a = 1 and b = 2 order by a, b;
select * from t1 where a = 1 and b = 2 order by b, a;
select * from t1 where e;