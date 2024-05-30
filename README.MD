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
  * desc
    - 根据你指定的 sf 生成数据，并且直接加载到表中
    - data set has checed with duckdb
    - 当你安装这个插件的时候，安装程序会自动检测当前主机的 core 数量，自动进行任务的划分，然后启动并行加载
      - 当前各表权重配置在 `tpch.tpch_tables` 表中，可以在 `src/pre_prepare.sql` 中找到，可以按自己的需求修改各表的load 权重
    - 加载完成后，会自动进行 analyze
    - 不会自动清理之前的数据， 每次使用之前， 需要执行下 tpch_cleanup

* tpch
  * args
    - qid (query id), is a integer list, exec query by ids
  * desc
    - 这个函数接受一个qid的列表，执行对应的tpch查询，并返回其统计信息
    - 如果你没有传递参数，则会测试全部的22条语句，并且如果执行时间比之前的更优，则会替换之前的记录
    - 如果你传递了参数，则只会测试指定的语句，并且不会记录之前的记录，只会进行比较输出

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

Time: 36599.711 ms (00:36.600)

-- run all 22 queries, you can get detail result like following,
postgres=# set work_mem TO '32GB';
SET
Time: 0.171 ms
postgres=# set max_parallel_workers TO 16;
SET
Time: 0.152 ms
postgres=# set max_parallel_workers_per_gather TO 16;
SET
Time: 0.158 ms
postgres=# select * from tpch;
 Qid  | Stable(ms)  | Current(ms) | Diff(%) | Result
------+-------------+-------------+---------+--------
  01  |             |     1258.36 |         | true
  02  |             |      265.84 |         | true
  03  |             |      260.80 |         | true
  04  |             |      130.41 |         | true
  05  |             |      434.27 |         | true
  06  |             |      188.53 |         | true
  07  |             |      522.96 |         | true
  08  |             |      200.34 |         | true
  09  |             |     1256.32 |         | true
  10  |             |      540.34 |         | true
  11  |             |      107.34 |         | true
  12  |             |      324.49 |         | true
  13  |             |     1423.72 |         | true
  14  |             |       97.65 |         | true
  15  |             |      738.49 |         | true
  16  |             |      207.93 |         | true
  17  |             |      846.95 |         | true
  18  |             |     3153.02 |         | true
  19  |             |       40.45 |         | true
  20  |             |      271.12 |         | true
  21  |             |      328.47 |         | true
  22  |             |       79.64 |         | true
 ---- | ----------- | ----------- | ------- |
 Sum  |             |    12677.41 |         |
(24 rows)

Time: 12686.567 ms (00:12.687)

-- run again, and compare with the best previous result,
postgres=# select * from tpch;
 Qid  | Stable(ms)  | Current(ms) | Diff(%) | Result
------+-------------+-------------+---------+--------
  01  |     1258.36 |     1376.73 |   +9.41 | true
  02  |      265.84 |      252.44 |   -5.04 | true
  03  |      260.80 |      277.13 |   +6.26 | true
  04  |      130.41 |      130.86 |   +0.35 | true
  05  |      434.27 |      452.32 |   +4.16 | true
  06  |      188.53 |      221.24 |  +17.35 | true
  07  |      522.96 |      532.74 |   +1.87 | true
  08  |      200.34 |      204.05 |   +1.85 | true
  09  |     1256.32 |     1296.64 |   +3.21 | true
  10  |      540.34 |      593.44 |   +9.83 | true
  11  |      107.34 |      107.41 |   +0.06 | true
  12  |      324.49 |      333.34 |   +2.73 | true
  13  |     1423.72 |     1373.88 |   -3.50 | true
  14  |       97.65 |       98.11 |   +0.47 | true
  15  |      738.49 |      746.11 |   +1.03 | true
  16  |      207.93 |      157.63 |  -24.19 | true
  17  |      846.95 |      855.39 |   +1.00 | true
  18  |     3153.02 |     3220.33 |   +2.13 | true
  19  |       40.45 |       40.16 |   -0.72 | true
  20  |      271.12 |      215.25 |  -20.61 | true
  21  |      328.47 |      301.26 |   -8.28 | true
  22  |       79.64 |       70.83 |  -11.06 | true
 ---- | ----------- | ----------- | ------- |
 Sum  |    12677.41 |    12857.28 |   +1.42 |
(24 rows)

Time: 12865.071 ms (00:12.865)

-- or run queries which you want by id, 1 to 99
postgres=# select * from tpch(4,4,6,6);
 Qid  | Stable(ms)  | Current(ms) | Diff(%) | Result
------+-------------+-------------+---------+--------
  04  |      130.41 |      130.32 |   -0.07 | true
  04  |      130.41 |      127.47 |   -2.26 | true
  06  |      188.53 |      206.08 |   +9.31 | true
  06  |      188.53 |      196.78 |   +4.37 | true
 ---- | ----------- | ----------- | ------- |
 Sum  |      637.89 |      660.65 |   +3.57 |
(6 rows)

Time: 662.110 ms

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