## PG_TPCH, A PLUG-AND-PLAY TPC-H EXTENSION FOR POSTGRES

A plug-and-play postgres extension for testing tpch, inspired by [duckdb](https://github.com/duckdb/duckdb.git) and [hyrise](https://github.com/hyrise/hyrise.git), to avoid the cumbersome configuration in tpch. Only need to install the extension, and then you can run the test.

### Highlights

- A TPC-H benchmark extension based on PostgreSQL, supporting all 22 TPC-H queries and providing data generation funciton.
- Automatically generates TPC-H datasets and loads them into PostgreSQL.
- Automatically runs and records the execution time and results of all 22 TPC-H queries.
- Async load data set.

### REQUIREMENTS

- PostgreSQL
    this is a extension of PostgreSQL, you need to install it first.
- gcc 12 or higher
- cmake

### SETUP

make sure you have installed the postgres and the `pg_config` is in your PATH.

```shell
git clone https://github.com/asky/pg_tpch.git
cd pg_tpch
cmake -Bbuild
# install extension to postgres
cmake --build build --target install
# you can also run test
cmake --build build --target test
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

-- generate data set for scale factor 1
postgres=# select * from dbgen(1);
   tab    | row_count
----------+-----------
 region   |         5
 nation   |        25
 supplier |     10000
 customer |    150000
 part     |    200000
 partsupp |    800000
 orders   |   1500000
 lineitem |   6001215
(8 rows)

-- run all 22 queries, you can get detail result like following,
[local]:15432 asky@regress=# select * from tpch;
 Qid  | Stable(ms)  | Current(ms) | Diff(%) | Result
------+-------------+-------------+---------+--------
  01  |     7205.02 |     7304.42 |   +1.38 | true
  02  |     3387.55 |     3188.97 |   -5.86 | true
  03  |     2703.25 |     2781.70 |   +2.90 | true
  04  |      745.93 |      748.44 |   +0.34 | true
  05  |     1350.80 |     1328.60 |   -1.64 | true
  06  |      866.87 |      931.66 |   +7.48 | true
  07  |     1855.41 |     2279.28 |  +22.85 | true
  08  |     2606.11 |     2588.94 |   -0.66 | true
  09  |    13436.01 |    12221.57 |   -9.04 | true
  10  |     2958.73 |     3158.99 |   +6.77 | true
  11  |      721.30 |     1302.36 |  +80.56 | true
  12  |     1653.34 |     1763.60 |   +6.67 | true
  13  |    12294.78 |    15920.73 |  +29.49 | true
  14  |     1075.40 |     1107.85 |   +3.02 | true
  15  |     7237.04 |     7142.93 |   -1.30 | true
  16  |     1132.08 |     1098.27 |   -2.99 | true
  17  |     9370.88 |     9590.21 |   +2.34 | true
  18  |    17670.13 |    17229.55 |   -2.49 | true
  19  |      291.94 |      259.29 |  -11.18 | true
  20  |     7173.15 |     7066.40 |   -1.49 | true
  21  |     2177.83 |     2397.98 |  +10.11 | true
  22  |      367.69 |      382.32 |   +3.98 | true
 ---- | ----------- | ----------- | ------- |
 Sum  |    98281.23 |   101794.06 |   +3.57 |
(24 rows)

-- or run queries which you want by id, 1 to 99
[local]:15432 292864 asky@regress=# select * from tpch(4,4,6,6);
 Qid  | Stable(ms)  | Current(ms) | Diff(%) | Result
------+-------------+-------------+---------+--------
  04  |      745.93 |      713.70 |   -4.32 | true
  04  |      745.93 |      737.73 |   -1.10 | true
  06  |      866.87 |      899.95 |   +3.82 | true
  06  |      866.87 |      909.38 |   +4.90 | true
 ---- | ----------- | ----------- | ------- |
 Sum  |     3225.60 |     3260.76 |   +1.09 |
(6 rows)

-- get all queries
select * from tpch_queries();

-- get query by id, from 1 to 99
select * from tpch_queries(1);

-- you can set query to a parameter
select query from tpch_queries(1); \gset

-- and exec it
:query

-- or cleanup all tables
-- if parameter is true, will also clean up statistics
select tpch_cleanup(true);

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

- batch insert for data generation
- parallel reindex and analyze
  * dbgen 10 is `Time: 369922.457 ms (06:09.922)`