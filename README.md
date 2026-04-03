# pg_tpch

`pg_tpch` is a PostgreSQL extension built with `pgrx` for fast in-database TPC-H data loading.

The project is intentionally focused on the TPC-H workflow rather than generic ETL. It creates standard TPC-H tables, loads data directly inside PostgreSQL, and provides a convenient bulk entry point that can use `dblink` for parallel per-table loading when available.

## What This Project Does

`pg_tpch` provides three main capabilities:

- Create standard TPC-H tables in the current schema.
- Load TPC-H data directly into PostgreSQL without going through an external file import workflow.
- Optionally build secondary indexes after loading.

It also includes helper functions to truncate all TPC-H data or drop all TPC-H tables from the current schema.

## Highlights

- Built as a native PostgreSQL extension with `pgrx`.
- Uses `tpchgen` to generate TPC-H data.
- Avoids part of PostgreSQL's generic text/numeric/date input overhead by encoding TPC-H values more directly.
- Stores table and index definitions in `tpch.tpch_table_metadata`.
- Uses `dblink` automatically for parallel table loading when `dblink` is installed.
- Falls back to serial loading when `dblink` is not installed.
- Keeps table creation, data loading, index creation, truncation, and drop operations as separate steps.

## Metadata Table

When the extension is installed, it creates the following metadata table:

```sql
tpch.tpch_table_metadata (
    table_name    varchar primary key,
    table_def     text not null,
    table_indexes varchar[] not null
)
```

This table defines:

- The `CREATE TABLE` statement for each standard TPC-H table.
- The optional secondary index statements for each table.

The following functions use this metadata:

- `create_tpch_tables()`
- `create_tpch_indexes()`
- `cleanup_tpch_data()`
- `drop_tpch_tables()`
- `tpch_dbgen()`

The extension also ships a query metadata table:

```sql
tpch.tpch_query_metadata (
    qid   integer primary key,
    query text not null
)
```

It stores the 22 built-in TPC-H benchmark query texts.
These are concrete query texts with parameter values already substituted, not `:1`, `:2` style templates.

If you want to customize table definitions or secondary indexes, update `tpch.tpch_table_metadata` directly.

## Installation

### 1. Build and install the extension

```bash
cargo pgrx install
```

### 2. Install the extension in PostgreSQL

```sql
CREATE EXTENSION pg_tpch;
```

### 3. Optional: install `dblink` for parallel `tpch_dbgen()`

```sql
CREATE EXTENSION dblink;
```

## How Schema Selection Works

The extension does not take a schema name argument for table management or bulk loading.

Instead, it always operates on the current schema selected by `search_path`.

Example:

```sql
CREATE SCHEMA my_tpch;
SET search_path = my_tpch, public;

SELECT create_tpch_tables(false);
SELECT * FROM tpch_dbgen(0.01);
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

Each loader inserts data into one already-existing table and returns a single-row summary.

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

## Function Notes

### `create_tpch_tables()`

- Creates the standard TPC-H tables in the current schema.
- Reads `table_def` from `tpch.tpch_table_metadata`.
- If `create_indexes = true`, also executes `table_indexes`.
- Fails if same-name TPC-H tables already exist in the current schema.

### `create_tpch_indexes()`

- Creates only secondary indexes.
- Does not create tables.
- Reads index definitions from `tpch.tpch_table_metadata`.

### `cleanup_tpch_data()`

- Runs `TRUNCATE` across all TPC-H tables in the current schema.
- Keeps table definitions and indexes in place.
- Fails if the required TPC-H tables do not exist.

### `drop_tpch_tables()`

- Drops the standard TPC-H tables from the current schema.
- Intended to fully clean a TPC-H working schema.

### `generate_*()`

- Each function loads exactly one table.
- The target table must already exist in the current schema.
- The returned `heap_time_ms` and `reindex_time_ms` values are raw timing numbers from that table load.

### `tpch_queries()`

- Returns query text metadata, not execution results.
- When called without a parameter, returns all 22 TPC-H queries ordered by `qid`.
- When called with a `qid`, returns the matching query if it exists.
- Uses the built-in `tpch.tpch_query_metadata` table.
- Returns executable SQL with parameter values already substituted.

### `tpch_dbgen()`

- Loads all 8 TPC-H tables.
- Requires that the TPC-H tables already exist in the current schema.
- Does not create tables.
- Does not create secondary indexes.
- Returns one row per table.
- `heap_time_ms` and `reindex_time_ms` are returned as `numeric(20,2)`, so they display with two decimals and are right-aligned in `psql`.

## Typical Workflows

### Fastest common workflow

Create tables first, load data, then build indexes:

```sql
CREATE EXTENSION pg_tpch;
CREATE EXTENSION dblink;

CREATE SCHEMA my_tpch;
SET search_path = my_tpch, public;

SELECT create_tpch_tables(false);
SELECT * FROM tpch_dbgen(0.01);
SELECT create_tpch_indexes();
```

### Minimal workflow

If you only want tables and data:

```sql
CREATE SCHEMA my_tpch;
SET search_path = my_tpch, public;

SELECT create_tpch_tables(false);
SELECT * FROM tpch_dbgen(0.01);
```

### Reload data without dropping tables

```sql
SET search_path = my_tpch, public;

SELECT cleanup_tpch_data();
SELECT * FROM tpch_dbgen(0.01);
```

### Load one table only

```sql
SET search_path = my_tpch, public;

SELECT create_tpch_tables(false);
SELECT * FROM generate_lineitem(1.0);
```

## Customizing Table and Index Definitions

Because table definitions live in `tpch.tpch_table_metadata`, you can inspect or modify them with SQL.

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

- `table_def` must contain a full `CREATE TABLE ...` statement.
- Each element in `table_indexes` must contain a full `CREATE INDEX ...` statement.
- Changes to metadata affect future table creation and index creation.

## Standard TPC-H Tables Included

The extension ships metadata for these 8 TPC-H tables:

- `region`
- `nation`
- `supplier`
- `customer`
- `part`
- `partsupp`
- `orders`
- `lineitem`

## Testing

The project includes regression coverage for:

- value encoding
- table creation and cleanup
- single-table loading
- serial `tpch_dbgen()`
- `dblink`-based `tpch_dbgen()`

Run:

```bash
cargo check
cargo pgrx test
```

## Good Fit For

- fast TPC-H dataset preparation inside PostgreSQL
- benchmarking and performance experiments
- repeated local schema resets and reloads
- targeted optimization work on the TPC-H load path

This project is intentionally specialized. It prioritizes controllability and speed for TPC-H over generic multi-schema data management features.
