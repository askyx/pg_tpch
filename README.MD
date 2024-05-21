## TPC-DS FOR POSTGRES

A plug-and-play postgres extension for testing tpcds, inspired by [duckdb](https://github.com/duckdb/duckdb.git) and [hyrise](https://github.com/hyrise/hyrise.git), to avoid the cumbersome configuration in tpcds. Only need to install the extension, and then you can run the test.

### REQUIREMENTS

- PostgreSQL
    this is a extension of PostgreSQL, you need to install it first.
- gcc 13 or higher

### SETUP

make sure you have installed the postgres and the `pg_config` is in your PATH.

```
git clone https://github.com/asky/pg_tpcds.git
git submodule update
cd pg_tpcds
cmake -Bbuild
cmake --build build --target install
```

### BASIC USAGE

```sql
-- create extension pg_tpcds;
create extension pg_tpcds;

-- generate data set for scale factor 1
select dsdgen(1);

-- run all 99 queries, you can get detail result like following,
--   NOTE: this is base on empty table
select * from tpcds;  
 Qid  | Stable(ms)  | Current(ms) | Diff(%) | Result
------+-------------+-------------+---------+--------
  01  |        0.55 |        1.09 |  +98.89 | true
  02  |        0.33 |        0.35 |   +6.29 | true
  03  |        0.11 |        0.11 |   +0.13 | true
  04  |        1.02 |        1.12 |   +9.80 | true
  05  |        0.59 |        0.62 |   +5.23 | true
....
  94  |        0.29 |        0.29 |   +0.70 | true
  95  |        0.37 |        0.37 |   +0.18 | true
  96  |        0.10 |        0.10 |   +0.20 | true
  97  |        0.17 |        0.17 |   -0.52 | true
  98  |        0.14 |        0.14 |   +0.83 | true
  99  |        0.23 |        0.23 |   +0.39 | true
 ---- | ----------- | ----------- | ------- |
 Sum  |       57.04 |       59.85 |   +4.92 |
(101 rows)

-- or run queries which you want by id, 1 to 99
select * from tpcds(1,2,3,5,8);
 Qid  | Stable(ms)  | Current(ms) | Diff(%) | Result
------+-------------+-------------+---------+--------
  01  |        0.55 |        0.45 |  -18.10 | true
  02  |        0.33 |        0.32 |   -0.28 | true
  03  |        0.11 |        0.10 |   -9.35 | true
  05  |        0.59 |        0.60 |   +2.40 | true
  08  |        0.40 |        0.42 |   +4.03 | true
 ---- | ----------- | ----------- | ------- |
 Sum  |        1.97 |        1.89 |   -4.07 |
(7 rows)
-- get all queries
select * from tpcds_queries();

-- get query by id, from 1 to 99
select * from tpcds_queries(1);

-- you can set query to a parameter
select query from tpcds_queries(1); \gset

-- and exec it
:query

-- or cleanup all tables
select tpcds_cleanup();

-- and regenerated data set for other scale factor 
select dsdgen(2);

-- drop extension pg_tpcds will auto remove all tables and functions
drop extension pg_tpcds;

```

### CUSTOMIZATION

* 99 SQL statements have been provided here in advance. you can modify the SQL statement before installing the extension,  the file located at `src/tpcds/queries`.
* Standard table creation statements are provided here for your configuration. The file is located at `src/tpcds/schema`.
   * You can directly modify the table creation statements, such as specifying primary keys.
   * Alternatively, you can configure index creation statements in the file `src/post_prepare.sql`, which will be invoked after table creation


### TEST

NEED ?

### TODO

- [ ] check query result correctness
- [ ] support postgres 12 or higher
- [ ] async dsdgen, need provide a async function to generate data set, or use dblink
    - Avoid relying on other extensions, we can do it ourselves.