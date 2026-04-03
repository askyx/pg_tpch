# pg_tpch

`pg_tpch` is a PostgreSQL extension built with `pgrx` for fast, in-database TPC-H data loading. It is inspired by [DuckDB](https://github.com/duckdb/duckdb.git) and [Hyrise](https://github.com/hyrise/hyrise.git), with the goal of making TPC-H setup inside PostgreSQL much simpler and more practical.

The older [C++-based](https://github.com/askyx/pg_tpch/tree/base_on_cpp) implementation has been retired. The current version is built on top of [tpchgen-rs](https://github.com/clflushopt/tpchgen-rs) and [pgrx](https://github.com/pgcentralfoundation/pgrx). Many thanks to both projects.

## Good Fit For

- Fast TPC-H dataset preparation inside PostgreSQL. See [performance notes](./performce.md).
- Benchmarking and performance experiments.
- Repeated local schema resets and reloads.
- Targeted optimization work on the TPC-H load path.

This project is intentionally specialized. It prioritizes speed and control for TPC-H workflows over generic multi-schema data management features.

## Version Support

- Minimum supported PostgreSQL version: `13`
- Tested PostgreSQL versions: `13`, `14`, `15`, `16`, `17`, `18`

## Installation

Prebuilt release archives are currently provided for:

- Ubuntu 22.04
- x86_64
- PostgreSQL 13 through 18

Each archive targets exactly one PostgreSQL major version, for example:

- `pg_tpch-pg13-ubuntu22.04-x86_64.tar.gz`
- `pg_tpch-pg18-ubuntu22.04-x86_64.tar.gz`

Make sure the archive matches your PostgreSQL major version exactly. If your environment is not covered by the prebuilt packages, you can build the extension yourself or open an issue requesting support.

### 1. Find your PostgreSQL extension directories

```bash
pg_config --pkglibdir
pg_config --sharedir
```

### 2. Extract the archive

```bash
tar -xzf pg_tpch-pg16-ubuntu22.04-x86_64.tar.gz
cd pg_tpch-pg16-ubuntu22.04-x86_64
```

### 3. Copy the extension files into PostgreSQL

```bash
cp lib/pg_tpch.so "$(pg_config --pkglibdir)/"
cp share/extension/pg_tpch.control "$(pg_config --sharedir)/extension/"
cp share/extension/pg_tpch--0.1.0.sql "$(pg_config --sharedir)/extension/"
```

### 4. Install the extension in PostgreSQL

```sql
CREATE EXTENSION pg_tpch;
```

### 5. Optional: install `dblink` for parallel `tpch_dbgen()`

`tpch_dbgen()` can use `dblink` to load tables in parallel. This is optional. The extension works normally even if `dblink` is not installed.

```sql
CREATE EXTENSION dblink;
```

## Basic Usage

```sql
-- create tables with secondary indexes
-- pass false to skip index creation
SELECT * FROM create_tpch_tables(true);

-- load data at scale factor 1
SELECT * FROM tpch_dbgen(1);

-- fetch query 1 into a psql variable
SELECT query AS q1 FROM tpch_queries(1) \gset

-- execute the query
:q1

-- remove all data and prepare for another scale factor
SELECT * FROM cleanup_tpch_data();
```

## Function Reference

### Management Functions

| Function | Purpose | Returns | Example |
| --- | --- | --- | --- |
| `create_tpch_tables(create_indexes bool default false)` | Creates the 8 standard TPC-H tables in the current schema. Optionally creates secondary indexes from metadata. | `text` status message | `SELECT create_tpch_tables(false);` |
| `create_tpch_indexes()` | Creates secondary indexes for the current schema using `tpch.tpch_table_metadata`. | `text` status message | `SELECT create_tpch_indexes();` |
| `cleanup_tpch_data()` | Truncates all TPC-H tables in the current schema without dropping table definitions or indexes. | `text` status message | `SELECT cleanup_tpch_data();` |
| `drop_tpch_tables()` | Drops all 8 TPC-H tables from the current schema. | `text` status message | `SELECT drop_tpch_tables();` |
| `tpch_queries(qid integer default null)` | Returns the built-in concrete TPC-H query texts from `tpch.tpch_query_metadata`. Returns all queries when `qid` is null, or one query when `qid` is specified. | `table(qid integer, query text)` | `SELECT * FROM tpch_queries();` |

### Single-Table Loader Functions

Each loader inserts data into one existing table and returns a single-row summary.

| Function | Target Table | Returns | Example |
| --- | --- | --- | --- |
| `generate_region(scale_factor double precision default 1.0)` | `region` | `(rows bigint, heap_time_ms double precision, reindex_time_ms double precision)` | `SELECT * FROM generate_region(1.0);` |
| `generate_nation(scale_factor double precision default 1.0)` | `nation` | `(rows bigint, heap_time_ms double precision, reindex_time_ms double precision)` | `SELECT * FROM generate_nation(1.0);` |
| `generate_part(scale_factor double precision default 1.0)` | `part` | `(rows bigint, heap_time_ms double precision, reindex_time_ms double precision)` | `SELECT * FROM generate_part(1.0);` |
| `generate_supplier(scale_factor double precision default 1.0)` | `supplier` | `(rows bigint, heap_time_ms double precision, reindex_time_ms double precision)` | `SELECT * FROM generate_supplier(1.0);` |
| `generate_customer(scale_factor double precision default 1.0)` | `customer` | `(rows bigint, heap_time_ms double precision, reindex_time_ms double precision)` | `SELECT * FROM generate_customer(1.0);` |
| `generate_partsupp(scale_factor double precision default 1.0)` | `partsupp` | `(rows bigint, heap_time_ms double precision, reindex_time_ms double precision)` | `SELECT * FROM generate_partsupp(1.0);` |
| `generate_orders(scale_factor double precision default 1.0)` | `orders` | `(rows bigint, heap_time_ms double precision, reindex_time_ms double precision)` | `SELECT * FROM generate_orders(1.0);` |
| `generate_lineitem(scale_factor double precision default 1.0)` | `lineitem` | `(rows bigint, heap_time_ms double precision, reindex_time_ms double precision)` | `SELECT * FROM generate_lineitem(1.0);` |

### Bulk Loader Function

| Function | Purpose | Returns | Example |
| --- | --- | --- | --- |
| `tpch_dbgen(scale_factor double precision default 1.0)` | Loads all 8 TPC-H tables in the current schema. Uses `dblink` for parallel per-table loading if available, otherwise loads serially. | `table(table_name text, rows bigint, heap_time_ms numeric(20,2), reindex_time_ms numeric(20,2))` | `SELECT * FROM tpch_dbgen(0.01);` |

## Customizing Table and Index Definitions

Table and secondary index definitions are stored in `tpch.tpch_table_metadata`, so you can inspect or modify them directly with SQL.

### Inspect the current metadata

```sql
SELECT table_name, table_def, table_indexes
FROM tpch.tpch_table_metadata
ORDER BY table_name;
```

### Inspect the built-in TPC-H queries

```sql
SELECT qid, query
FROM tpch.tpch_query_metadata
ORDER BY qid;
```

### Modify an index definition

```sql
UPDATE tpch.tpch_table_metadata
SET table_indexes = ARRAY[
    'CREATE INDEX idx_orders_custkey ON orders (o_custkey)'
]
WHERE table_name = 'orders';
```

### Notes

- `table_def` must contain a complete `CREATE TABLE ...` statement.
- Each element in `table_indexes` must contain a complete `CREATE INDEX ...` statement.
- Metadata changes affect future table creation and index creation.

## License

This project is licensed under the MIT License. See [LICENSE](./LICENSE).
