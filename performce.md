# Performance

## Summary

| Scenario | `pg_tpch` Total Time | Concurrent `COPY` Total Time | Relative Result |
| --- | ---: | ---: | --- |
| No constraints | 59.09 s | 169.13 s | `pg_tpch` is about 2.9x faster |
| Primary keys + `NOT NULL` | 71.13 s | 249.46 s | `pg_tpch` is about 3.5x faster |
| Secondary indexes enabled | 166.89 s | 1142.27 s | `pg_tpch` is about 6.8x faster |

These results show a consistent pattern:

- `pg_tpch` outperforms concurrent `COPY` in all three scenarios.
- The best overall throughput comes from loading into plain heap tables with no constraints or indexes.
- Primary keys and `NOT NULL` constraints add overhead, but the extension still maintains a clear advantage.
- Secondary indexes are by far the most expensive part of the load path for both approaches, especially on large tables such as `lineitem`.


## Test Environment and Methodology

The benchmark focuses on three scenarios:

1. No constraints at all.
2. Primary keys and `NOT NULL` constraints.
3. Secondary indexes enabled.

The extension-side benchmark uses parallel loading through `dblink`, so `dblink` was installed in advance.

Environment:

```text
OS: Ubuntu 25.04 plucky (on Windows Subsystem for Linux)
Kernel: x86_64 Linux 6.6.87.1-microsoft-standard-WSL2
CPU: Intel Core Ultra 9 285H @ 16x 3.686GHz
```

```text
PostgreSQL 18.1 on x86_64-linux, compiled by gcc-15.0.1, 64-bit
```

TPC-H CSV data was generated with `tpchgen-cli`:

```bash
tpchgen-cli -s 10 -o sf10 -f csv
```

The `COPY` baseline was measured with concurrent `psql` sessions:

```bash
time bash -lc 'set -e; \
psql -c "COPY nation FROM '\''sf10/nation.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY region FROM '\''sf10/region.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY part FROM '\''sf10/part.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY supplier FROM '\''sf10/supplier.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY customer FROM '\''sf10/customer.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY orders FROM '\''sf10/orders.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY lineitem FROM '\''sf10/lineitem.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY partsupp FROM '\''sf10/partsupp.csv'\'' WITH (FORMAT csv, HEADER true);" & wait'
```

## Scenario 1: No Constraints

For this scenario, the table definitions in `tpch.tpch_table_metadata` were updated to remove primary keys and `NOT NULL` constraints before recreating the tables.

```sql
SELECT * FROM drop_tpch_tables();

UPDATE tpch.tpch_table_metadata
SET table_def = 'CREATE TABLE region (
    r_regionkey integer,
    r_name char(25),
    r_comment varchar(152)
)'
WHERE table_name = 'region';

UPDATE tpch.tpch_table_metadata
SET table_def = 'CREATE TABLE nation (
    n_nationkey integer,
    n_name char(25),
    n_regionkey integer,
    n_comment varchar(152)
)'
WHERE table_name = 'nation';

UPDATE tpch.tpch_table_metadata
SET table_def = 'CREATE TABLE supplier (
    s_suppkey integer,
    s_name char(25),
    s_address varchar(40),
    s_nationkey integer,
    s_phone char(15),
    s_acctbal decimal(15, 2),
    s_comment varchar(101)
)'
WHERE table_name = 'supplier';

UPDATE tpch.tpch_table_metadata
SET table_def = 'CREATE TABLE customer (
    c_custkey integer,
    c_name varchar(25),
    c_address varchar(40),
    c_nationkey integer,
    c_phone char(15),
    c_acctbal decimal(15, 2),
    c_mktsegment char(10),
    c_comment varchar(117)
)'
WHERE table_name = 'customer';

UPDATE tpch.tpch_table_metadata
SET table_def = 'CREATE TABLE part (
    p_partkey integer,
    p_name varchar(55),
    p_mfgr char(25),
    p_brand char(10),
    p_type varchar(25),
    p_size integer,
    p_container char(10),
    p_retailprice decimal(15, 2),
    p_comment varchar(23)
)'
WHERE table_name = 'part';

UPDATE tpch.tpch_table_metadata
SET table_def = 'CREATE TABLE partsupp (
    ps_partkey integer,
    ps_suppkey integer,
    ps_availqty integer,
    ps_supplycost decimal(15, 2),
    ps_comment varchar(199)
)'
WHERE table_name = 'partsupp';

UPDATE tpch.tpch_table_metadata
SET table_def = 'CREATE TABLE orders (
    o_orderkey integer,
    o_custkey integer,
    o_orderstatus char(1),
    o_totalprice decimal(15, 2),
    o_orderdate date,
    o_orderpriority char(15),
    o_clerk char(15),
    o_shippriority integer,
    o_comment varchar(79)
)'
WHERE table_name = 'orders';

UPDATE tpch.tpch_table_metadata
SET table_def = 'CREATE TABLE lineitem (
    l_orderkey integer,
    l_partkey integer,
    l_suppkey integer,
    l_linenumber integer,
    l_quantity decimal(15, 2),
    l_extendedprice decimal(15, 2),
    l_discount decimal(15, 2),
    l_tax decimal(15, 2),
    l_returnflag char(1),
    l_linestatus char(1),
    l_shipdate date,
    l_commitdate date,
    l_receiptdate date,
    l_shipinstruct char(25),
    l_shipmode char(10),
    l_comment varchar(44)
)'
WHERE table_name = 'lineitem';

SELECT * FROM create_tpch_tables(false);
```

`pg_tpch` results:

```sql
\timing
SELECT * FROM tpch_dbgen(10) ORDER BY 2;
 table_name |   rows   | heap_time_ms | reindex_time_ms
------------+----------+--------------+-----------------
 region     |        5 |         0.06 |            0.00
 nation     |       25 |         0.35 |            0.00
 supplier   |   100000 |        94.58 |            0.00
 customer   |  1500000 |      1421.47 |            0.00
 part       |  2000000 |      1962.74 |            0.00
 partsupp   |  8000000 |      5505.06 |            0.00
 orders     | 15000000 |     13252.95 |            0.00
 lineitem   | 59986052 |     58051.58 |            0.00
(8 rows)

Time: 59086.977 ms (00:59.087)
```

Concurrent `COPY` baseline:

```bash
time bash -lc 'set -e; \
psql -c "COPY nation FROM '\''/home/asky/pg_tpchrs/sf10/nation.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY region FROM '\''/home/asky/pg_tpchrs/sf10/region.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY part FROM '\''/home/asky/pg_tpchrs/sf10/part.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY supplier FROM '\''/home/asky/pg_tpchrs/sf10/supplier.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY customer FROM '\''/home/asky/pg_tpchrs/sf10/customer.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY orders FROM '\''/home/asky/pg_tpchrs/sf10/orders.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY lineitem FROM '\''/home/asky/pg_tpchrs/sf10/lineitem.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY partsupp FROM '\''/home/asky/pg_tpchrs/sf10/partsupp.csv'\'' WITH (FORMAT csv, HEADER true);" & wait'

COPY 5
COPY 25
COPY 100000
COPY 1500000
COPY 2000000
COPY 8000000
COPY 15000000
COPY 59986052
bash -lc   0.02s user 0.01s system 0% cpu 2:49.13 total
```

## Scenario 2: Primary Keys and `NOT NULL` Constraints

For this scenario, the extension was recreated to restore the default table definitions.

```sql
DROP EXTENSION pg_tpch;
CREATE EXTENSION pg_tpch;

SELECT * FROM drop_tpch_tables();
SELECT * FROM create_tpch_tables();
```

`pg_tpch` results:

```sql
SELECT * FROM tpch_dbgen(10) ORDER BY 2;
 table_name |   rows   | heap_time_ms | reindex_time_ms
------------+----------+--------------+-----------------
 region     |        5 |         1.27 |            3.47
 nation     |       25 |         0.28 |            1.71
 supplier   |   100000 |       103.58 |           41.19
 customer   |  1500000 |      1422.15 |          277.69
 part       |  2000000 |      1970.19 |          378.99
 partsupp   |  8000000 |      5425.06 |         1745.95
 orders     | 15000000 |     13294.44 |         2762.91
 lineitem   | 59986052 |     60296.24 |         9868.54
(8 rows)

Time: 71130.116 ms (01:11.130)
```

Concurrent `COPY` baseline:

```bash
time bash -lc 'set -e; \
psql -c "COPY nation FROM '\''/home/asky/pg_tpchrs/sf10/nation.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY region FROM '\''/home/asky/pg_tpchrs/sf10/region.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY part FROM '\''/home/asky/pg_tpchrs/sf10/part.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY supplier FROM '\''/home/asky/pg_tpchrs/sf10/supplier.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY customer FROM '\''/home/asky/pg_tpchrs/sf10/customer.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY orders FROM '\''/home/asky/pg_tpchrs/sf10/orders.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY lineitem FROM '\''/home/asky/pg_tpchrs/sf10/lineitem.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY partsupp FROM '\''/home/asky/pg_tpchrs/sf10/partsupp.csv'\'' WITH (FORMAT csv, HEADER true);" & wait'

COPY 25
COPY 5
COPY 100000
COPY 1500000
COPY 2000000
COPY 8000000
COPY 15000000
COPY 59986052
bash -lc   0.01s user 0.02s system 0% cpu 4:09.46 total
```

## Scenario 3: Secondary Indexes Enabled

For this scenario, secondary indexes were created based on the definitions stored in `tpch.tpch_table_metadata`.

```sql
SELECT * FROM cleanup_tpch_data();
SELECT * FROM create_tpch_indexes();
```

`pg_tpch` results:

```sql
SELECT * FROM tpch_dbgen(10) ORDER BY 2;
 table_name |   rows   | heap_time_ms | reindex_time_ms
------------+----------+--------------+-----------------
 region     |        5 |         0.07 |            2.45
 nation     |       25 |         0.09 |            2.44
 supplier   |   100000 |       101.14 |           80.41
 customer   |  1500000 |      1429.57 |          713.64
 part       |  2000000 |      2065.50 |          358.07
 partsupp   |  8000000 |      5996.37 |         5038.36
 orders     | 15000000 |     14177.42 |        10759.79
 lineitem   | 59986052 |     61169.99 |       104827.14
(8 rows)

Time: 166886.025 ms (02:46.886)
```

Concurrent `COPY` baseline:

```bash
time bash -lc 'set -e; \
psql -c "COPY nation FROM '\''/home/asky/pg_tpchrs/sf10/nation.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY region FROM '\''/home/asky/pg_tpchrs/sf10/region.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY part FROM '\''/home/asky/pg_tpchrs/sf10/part.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY supplier FROM '\''/home/asky/pg_tpchrs/sf10/supplier.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY customer FROM '\''/home/asky/pg_tpchrs/sf10/customer.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY orders FROM '\''/home/asky/pg_tpchrs/sf10/orders.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY lineitem FROM '\''/home/asky/pg_tpchrs/sf10/lineitem.csv'\'' WITH (FORMAT csv, HEADER true);" & \
psql -c "COPY partsupp FROM '\''/home/asky/pg_tpchrs/sf10/partsupp.csv'\'' WITH (FORMAT csv, HEADER true);" & wait'

COPY 25
COPY 5
COPY 100000
COPY 2000000
COPY 1500000
COPY 8000000
COPY 15000000
COPY 59986052
bash -lc   0.03s user 0.01s system 0% cpu 19:02.27 total
```
