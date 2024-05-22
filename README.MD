## PG_TPCH, A PLUG-AND-PLAY TPC-H EXTENSION FOR POSTGRES

A plug-and-play postgres extension for testing tpch, inspired by [duckdb](https://github.com/duckdb/duckdb.git) and [hyrise](https://github.com/hyrise/hyrise.git), to avoid the cumbersome configuration in tpch. Only need to install the extension, and then you can run the test.

### REQUIREMENTS

- PostgreSQL
    this is a extension of PostgreSQL, you need to install it first.
- gcc 13 or higher

### SETUP

make sure you have installed the postgres and the `pg_config` is in your PATH.

```
git clone https://github.com/asky/pg_tpch.git
git submodule update
cd pg_tpch
cmake -Bbuild
cmake --build build --target install
```

### BASIC USAGE

#### INTERNAL FUNCTIONS

```
* dbgen 
  * args
    - sf (scale factor), is a double value
    - overwrite (default false), if it is true, before generate data set, do truncate TPCH table first and run stats
  * desc
    - generate data set for tpch, according to scale factor, and load it into tables
    - data set has checed with duckdb
    - TODO: async data generation

* tpch
  * args
    - qid (query id), is a integer list, exec query by ids
  * desc
    - 这个函数接受一个qid的列表，执行对应的tpch查询，并返回其统计信息
    - 如果你没有传递参数，则会测试全部的22条语句，并且如果执行时间比之前的更优，则会替换之前的记录
    - 如果你传递了参数，则只会测试指定的语句，并且不会记录之前的记录，只会进行比较

* tpch_queries
  * args
    - qid (query id), is a integer, query by id
  * desc
    - 这个函数接受一个qid，返回对应的tpch查询语句
    - 如果你没有传递参数或者指定的参数是0，则会返回全部的22条语句

* tpch_cleanup
  * args
    - clean_stats (default false)
  * desc
    - 清理tpch表，如果clean_stats为true，则清理之前收集的统计信息

```

```sql
-- create extension pg_tpch;
create extension pg_tpch;

-- generate data set for scale factor 0.1
[local]:15432 asky@regress=# select * from dbgen(0.1);
   tab    | row_count
----------+-----------
 customer |     15000
 nation   |        25
 orders   |    150000
 lineitem |    600572
 part     |     20000
 partsupp |     80000
 region   |         5
 supplier |      1000
(8 rows)

-- run all 22 queries, you can get detail result like following,
[local]:15432 asky@regress=# select * from tpch;
 Qid  | Stable(ms)  | Current(ms) | Diff(%) | Result
------+-------------+-------------+---------+--------
  01  |             |      182.32 |         | true
  02  |             |       19.02 |         | true
  03  |             |       47.69 |         | true
  04  |             |       85.34 |         | true
  05  |             |       27.71 |         | true
  06  |             |       28.51 |         | true
  07  |             |       38.26 |         | true
  08  |             |       54.74 |         | true
  09  |             |       80.62 |         | true
  10  |             |       50.79 |         | true
  11  |             |       18.29 |         | true
  12  |             |       62.37 |         | true
  13  |             |       69.12 |         | true
  14  |             |       33.77 |         | true
  15  |             |       61.18 |         | true
  16  |             |       33.02 |         | true
  17  |             |    20082.87 |         | true
  18  |             |      712.46 |         | true
  19  |             |       45.79 |         | true
  20  |             |    96022.79 |         | true
  21  |             |       56.94 |         | true
  22  |             |       35.35 |         | true
 ---- | ----------- | ----------- | ------- |
 Sum  |             |   117848.96 |         |
(24 rows)

-- or run queries which you want by id, 1 to 99
[local]:15432 asky@regress=# select * from tpch(1,2,3,5,8);
 Qid  | Stable(ms)  | Current(ms) | Diff(%) | Result
------+-------------+-------------+---------+--------
  01  |      182.32 |      175.95 |   -3.50 | true
  02  |       19.02 |       20.89 |   +9.84 | true
  03  |       47.69 |       45.14 |   -5.34 | true
  05  |       27.71 |       31.94 |  +15.27 | true
  08  |       54.74 |       56.16 |   +2.58 | true
 ---- | ----------- | ----------- | ------- |
 Sum  |      331.47 |      330.07 |   -0.42 |
(7 rows)
-- get all queries
select * from tpch_queries();

-- get query by id, from 1 to 99
select * from tpch_queries(1);

-- you can set query to a parameter
select query from tpch_queries(1); \gset

-- and exec it
:query

-- or cleanup all tables
select tpch_cleanup();

-- and regenerated data set for other scale factor 
select dbgen(2);

-- drop extension pg_tpch will auto remove all tables and functions
drop extension pg_tpch;

```

### CUSTOMIZATION

* 22 SQL statements have been provided here in advance. you can modify the SQL statement before installing the extension,  the file located at `src/tpch/queries`.
* Standard table creation statements are provided here for your configuration. The file is located at `src/tpch/schema`.
   * You can directly modify the table creation statements, such as specifying primary keys.
   * Alternatively, you can configure index creation statements in the file `src/post_prepare.sql`, which will be invoked after table creation


### TODO

- [ ] check query result correctness
- [ ] support postgres 12 or higher
- [ ] async dsdgen, need provide a async function to generate data set, or use dblink
    - Avoid relying on other extensions, we can do it ourselves.