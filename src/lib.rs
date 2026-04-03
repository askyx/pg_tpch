use std::time::Duration;

use pgrx::{prelude::*, spi, Spi};
::pgrx::pg_module_magic!();

mod encoding;
mod loader;
mod schema;
mod tpch_queries;
mod utils;

extension_sql!(
    r#"
    CREATE SCHEMA IF NOT EXISTS tpch;

    CREATE TABLE tpch.tpch_table_metadata (
        table_name varchar PRIMARY KEY,
        table_def text NOT NULL,
        table_indexes varchar[] NOT NULL DEFAULT ARRAY[]::varchar[]
    );

    CREATE TABLE tpch.tpch_query_metadata (
        qid integer PRIMARY KEY,
        query text NOT NULL
    );

    INSERT INTO tpch.tpch_table_metadata (table_name, table_def, table_indexes)
    VALUES
    (
        'region',
        'CREATE TABLE region (
            r_regionkey integer not null,
            r_name char(25) not null,
            r_comment varchar(152),
            PRIMARY KEY (r_regionkey)
        )',
        ARRAY[]::varchar[]
    ),
    (
        'nation',
        'CREATE TABLE nation (
            n_nationkey integer not null,
            n_name char(25) not null,
            n_regionkey integer not null,
            n_comment varchar(152),
            PRIMARY KEY (n_nationkey)
        )',
        ARRAY[
            'CREATE INDEX idx_nation_regionkey ON nation (n_regionkey)'
        ]::varchar[]
    ),
    (
        'supplier',
        'CREATE TABLE supplier (
            s_suppkey integer not null,
            s_name char(25) not null,
            s_address varchar(40) not null,
            s_nationkey integer not null,
            s_phone char(15) not null,
            s_acctbal decimal(15, 2) not null,
            s_comment varchar(101) not null,
            PRIMARY KEY (s_suppkey)
        )',
        ARRAY[
            'CREATE INDEX idx_supplier_nation_key ON supplier (s_nationkey)'
        ]::varchar[]
    ),
    (
        'customer',
        'CREATE TABLE customer (
            c_custkey integer not null,
            c_name varchar(25) not null,
            c_address varchar(40) not null,
            c_nationkey integer not null,
            c_phone char(15) not null,
            c_acctbal decimal(15, 2) not null,
            c_mktsegment char(10) not null,
            c_comment varchar(117) not null,
            PRIMARY KEY (c_custkey)
        )',
        ARRAY[
            'CREATE INDEX idx_customer_nationkey ON customer (c_nationkey)'
        ]::varchar[]
    ),
    (
        'part',
        'CREATE TABLE part (
            p_partkey integer not null,
            p_name varchar(55) not null,
            p_mfgr char(25) not null,
            p_brand char(10) not null,
            p_type varchar(25) not null,
            p_size integer not null,
            p_container char(10) not null,
            p_retailprice decimal(15, 2) not null,
            p_comment varchar(23) not null,
            PRIMARY KEY (p_partkey)
        )',
        ARRAY[]::varchar[]
    ),
    (
        'partsupp',
        'CREATE TABLE partsupp (
            ps_partkey integer not null,
            ps_suppkey integer not null,
            ps_availqty integer not null,
            ps_supplycost decimal(15, 2) not null,
            ps_comment varchar(199) not null,
            PRIMARY KEY (ps_partkey, ps_suppkey)
        )',
        ARRAY[
            'CREATE INDEX idx_partsupp_partkey ON partsupp (ps_partkey)',
            'CREATE INDEX idx_partsupp_suppkey ON partsupp (ps_suppkey)'
        ]::varchar[]
    ),
    (
        'orders',
        'CREATE TABLE orders (
            o_orderkey integer not null,
            o_custkey integer not null,
            o_orderstatus char(1) not null,
            o_totalprice decimal(15, 2) not null,
            o_orderdate date not null,
            o_orderpriority char(15) not null,
            o_clerk char(15) not null,
            o_shippriority integer not null,
            o_comment varchar(79) not null,
            PRIMARY KEY (o_orderkey)
        )',
        ARRAY[
            'CREATE INDEX idx_orders_custkey ON orders (o_custkey)',
            'CREATE INDEX idx_orders_orderdate ON orders (o_orderdate)'
        ]::varchar[]
    ),
    (
        'lineitem',
        'CREATE TABLE lineitem (
            l_orderkey integer not null,
            l_partkey integer not null,
            l_suppkey integer not null,
            l_linenumber integer not null,
            l_quantity decimal(15, 2) not null,
            l_extendedprice decimal(15, 2) not null,
            l_discount decimal(15, 2) not null,
            l_tax decimal(15, 2) not null,
            l_returnflag char(1) not null,
            l_linestatus char(1) not null,
            l_shipdate date not null,
            l_commitdate date not null,
            l_receiptdate date not null,
            l_shipinstruct char(25) not null,
            l_shipmode char(10) not null,
            l_comment varchar(44) not null,
            PRIMARY KEY (l_orderkey, l_linenumber)
        )',
        ARRAY[
            'CREATE INDEX idx_lineitem_orderkey ON lineitem (l_orderkey)',
            'CREATE INDEX idx_lineitem_part_supp ON lineitem (l_partkey, l_suppkey)',
            'CREATE INDEX idx_lineitem_shipdate ON lineitem (l_shipdate, l_discount, l_quantity)'
        ]::varchar[]
    );
    "#,
    name = "create_tpch_table_metadata"
);

macro_rules! define_tpch_loader {
    ($fn_name:ident, $table:literal, $generator:path) => {
        #[pg_extern]
        fn $fn_name(
            scale_factor: default!(f64, 1.0),
        ) -> TableIterator<
            'static,
            (
                name!(rows, i64),
                name!(heap_time_ms, f64),
                name!(reindex_time_ms, f64),
            ),
        > {
            let result = loader::load_rows($table, <$generator>::new(scale_factor, 1, 1).iter());
            TableIterator::once((result.rows, result.heap_time_ms, result.reindex_time_ms))
        }
    };
}

define_tpch_loader!(
    generate_nation,
    "nation",
    tpchgen::generators::NationGenerator
);
define_tpch_loader!(
    generate_region,
    "region",
    tpchgen::generators::RegionGenerator
);
define_tpch_loader!(generate_part, "part", tpchgen::generators::PartGenerator);
define_tpch_loader!(
    generate_supplier,
    "supplier",
    tpchgen::generators::SupplierGenerator
);
define_tpch_loader!(
    generate_customer,
    "customer",
    tpchgen::generators::CustomerGenerator
);
define_tpch_loader!(
    generate_partsupp,
    "partsupp",
    tpchgen::generators::PartSuppGenerator
);
define_tpch_loader!(
    generate_orders,
    "orders",
    tpchgen::generators::OrderGenerator
);
define_tpch_loader!(
    generate_lineitem,
    "lineitem",
    tpchgen::generators::LineItemGenerator
);

const TPCH_DBGEN_TASKS: &[(&str, &str)] = &[
    ("region", "generate_region"),
    ("nation", "generate_nation"),
    ("part", "generate_part"),
    ("supplier", "generate_supplier"),
    ("customer", "generate_customer"),
    ("partsupp", "generate_partsupp"),
    ("orders", "generate_orders"),
    ("lineitem", "generate_lineitem"),
];

#[derive(Clone, Debug)]
struct TpchDbgenStat {
    table_name: String,
    rows: i64,
    heap_time_ms: f64,
    reindex_time_ms: f64,
}

#[pg_extern]
fn create_tpch_tables(create_indexes: default!(bool, false)) -> String {
    schema::create_tpch_tables(create_indexes)
}

#[pg_extern]
fn create_tpch_indexes() -> String {
    schema::create_tpch_indexes()
}

#[pg_extern]
fn drop_tpch_tables() -> String {
    schema::drop_tpch_tables()
}

#[pg_extern]
fn cleanup_tpch_data() -> String {
    schema::cleanup_tpch_data()
}

#[pg_extern]
fn tpch_queries(
    qid: default!(Option<i32>, "NULL"),
) -> TableIterator<'static, (name!(qid, i32), name!(query, String))> {
    TableIterator::new(schema::tpch_queries(qid).into_iter())
}

#[pg_extern]
fn tpch_dbgen(
    scale_factor: default!(f64, 1.0),
) -> TableIterator<
    'static,
    (
        name!(table_name, String),
        name!(rows, i64),
        name!(heap_time_ms, Numeric<20, 2>),
        name!(reindex_time_ms, Numeric<20, 2>),
    ),
> {
    let target_schema = schema::current_target_schema_name();
    schema::ensure_tpch_tables_exist();

    if let Some(dblink_schema) = extension_schema("dblink") {
        TableIterator::new(
            tpch_dbgen_parallel(scale_factor, &target_schema, &dblink_schema)
                .into_iter()
                .map(stat_to_row),
        )
    } else {
        TableIterator::new(
            load_all_tables_serial(scale_factor, &target_schema)
                .into_iter()
                .map(stat_to_row),
        )
    }
}

#[pg_extern]
fn hello_pg_tpch() -> &'static str {
    "Hello, pg_tpch"
}

fn stat_to_row(
    stat: TpchDbgenStat,
) -> (
    name!(table_name, String),
    name!(rows, i64),
    name!(heap_time_ms, Numeric<20, 2>),
    name!(reindex_time_ms, Numeric<20, 2>),
) {
    (
        stat.table_name,
        stat.rows,
        Numeric::<20, 2>::try_from(format!("{:.2}", stat.heap_time_ms).as_str()).unwrap(),
        Numeric::<20, 2>::try_from(format!("{:.2}", stat.reindex_time_ms).as_str()).unwrap(),
    )
}

fn load_all_tables_serial(scale_factor: f64, schema_name: &str) -> Vec<TpchDbgenStat> {
    Spi::run(&format!(
        "SET LOCAL search_path = {}, pg_catalog",
        spi::quote_identifier(schema_name)
    ))
    .unwrap();

    vec![
        collect_loader_stat("region", generate_region(scale_factor)),
        collect_loader_stat("nation", generate_nation(scale_factor)),
        collect_loader_stat("part", generate_part(scale_factor)),
        collect_loader_stat("supplier", generate_supplier(scale_factor)),
        collect_loader_stat("customer", generate_customer(scale_factor)),
        collect_loader_stat("partsupp", generate_partsupp(scale_factor)),
        collect_loader_stat("orders", generate_orders(scale_factor)),
        collect_loader_stat("lineitem", generate_lineitem(scale_factor)),
    ]
}

fn tpch_dbgen_parallel(
    scale_factor: f64,
    schema_name: &str,
    dblink_schema: &str,
) -> Vec<TpchDbgenStat> {
    let dblink_connect = spi::quote_qualified_identifier(dblink_schema, "dblink_connect");
    let dblink_disconnect = spi::quote_qualified_identifier(dblink_schema, "dblink_disconnect");
    let dblink_exec = spi::quote_qualified_identifier(dblink_schema, "dblink_exec");
    let dblink_send_query = spi::quote_qualified_identifier(dblink_schema, "dblink_send_query");
    let dblink_is_busy = spi::quote_qualified_identifier(dblink_schema, "dblink_is_busy");
    let dblink_get_result = spi::quote_qualified_identifier(dblink_schema, "dblink_get_result");
    let function_schema = extension_schema_for_function("generate_region");
    let conninfo_literal = spi::quote_literal(local_conninfo_string());

    let mut pending_connections = Vec::with_capacity(TPCH_DBGEN_TASKS.len());
    for (table_name, function_name) in TPCH_DBGEN_TASKS {
        let conn_name = format!("pg_tpch_tpch_dbgen_{table_name}");
        dblink_connect_named(&dblink_connect, &conn_name, &conninfo_literal);
        dblink_exec_named(
            &dblink_exec,
            &conn_name,
            &format!(
                "SET search_path = {}, pg_catalog",
                spi::quote_identifier(schema_name)
            ),
        );

        let qualified_function =
            spi::quote_qualified_identifier(function_schema.as_str(), *function_name);
        let query = format!("SELECT * FROM {}({})", qualified_function, scale_factor);
        let send_sql = format!(
            "SELECT {dblink_send_query}({}, {})",
            spi::quote_literal(&conn_name),
            spi::quote_literal(query)
        );
        let started = Spi::get_one::<i32>(&send_sql).unwrap().unwrap() != 0;
        assert!(started, "failed to start dblink query for {table_name}");
        pending_connections.push(conn_name);
    }

    let mut stats = Vec::with_capacity(TPCH_DBGEN_TASKS.len());

    while !pending_connections.is_empty() {
        let mut next_round = Vec::with_capacity(pending_connections.len());
        for conn_name in pending_connections.drain(..) {
            let busy_sql = format!(
                "SELECT {dblink_is_busy}({})",
                spi::quote_literal(&conn_name)
            );
            let busy = Spi::get_one::<i32>(&busy_sql).unwrap().unwrap() != 0;
            if busy {
                next_round.push(conn_name);
                continue;
            }

            let (table_name, rows, heap_time_ms, reindex_time_ms) =
                consume_remote_result(&dblink_get_result, &conn_name);
            stats.push(TpchDbgenStat {
                table_name,
                rows,
                heap_time_ms,
                reindex_time_ms,
            });
            dblink_disconnect_named(&dblink_disconnect, &conn_name);
        }

        pending_connections = next_round;
        if !pending_connections.is_empty() {
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    stats.sort_by(|left, right| left.table_name.cmp(&right.table_name));
    stats
}

fn collect_loader_stat(
    table_name: &str,
    result: TableIterator<
        'static,
        (
            name!(rows, i64),
            name!(heap_time_ms, f64),
            name!(reindex_time_ms, f64),
        ),
    >,
) -> TpchDbgenStat {
    let stats: Vec<_> = result.collect();
    let (rows, heap_time_ms, reindex_time_ms) = stats[0];
    TpchDbgenStat {
        table_name: table_name.to_string(),
        rows,
        heap_time_ms,
        reindex_time_ms,
    }
}

fn consume_remote_result(dblink_get_result: &str, conn_name: &str) -> (String, i64, f64, f64) {
    let (rows, heap_time_ms, reindex_time_ms) = Spi::get_three::<i64, f64, f64>(&format!(
        "SELECT * FROM {dblink_get_result}({}) AS t(rows bigint, heap_time_ms double precision, reindex_time_ms double precision)",
        spi::quote_literal(conn_name)
    ))
    .unwrap();

    (
        conn_name
            .trim_start_matches("pg_tpch_tpch_dbgen_")
            .to_string(),
        rows.unwrap_or_default(),
        heap_time_ms.unwrap_or_default(),
        reindex_time_ms.unwrap_or_default(),
    )
}

fn dblink_connect_named(dblink_connect: &str, conn_name: &str, conninfo_literal: &str) {
    Spi::run(&format!(
        "SELECT {dblink_connect}({}, {conninfo_literal})",
        spi::quote_literal(conn_name)
    ))
    .unwrap();
}

fn dblink_exec_named(dblink_exec: &str, conn_name: &str, sql: &str) {
    Spi::run(&format!(
        "SELECT {dblink_exec}({}, {})",
        spi::quote_literal(conn_name),
        spi::quote_literal(sql)
    ))
    .unwrap();
}

fn dblink_disconnect_named(dblink_disconnect: &str, conn_name: &str) {
    Spi::run(&format!(
        "SELECT {dblink_disconnect}({})",
        spi::quote_literal(conn_name)
    ))
    .unwrap();
}

fn extension_schema(extname: &str) -> Option<String> {
    Spi::get_one::<String>(&format!(
        "SELECT (
             SELECT n.nspname::text
             FROM pg_extension e
             JOIN pg_namespace n ON n.oid = e.extnamespace
             WHERE e.extname = {}
             LIMIT 1
         )",
        spi::quote_literal(extname)
    ))
    .unwrap()
}

fn extension_schema_for_function(function_name: &str) -> String {
    Spi::get_one::<String>(&format!(
        "SELECT (
             SELECT n.nspname::text
             FROM pg_proc p
             JOIN pg_namespace n ON n.oid = p.pronamespace
             WHERE p.proname = {}
             ORDER BY p.oid
             LIMIT 1
         )",
        spi::quote_literal(function_name)
    ))
    .unwrap()
    .unwrap()
}

fn local_conninfo_string() -> String {
    Spi::get_one::<String>(
        "SELECT format(
             'dbname=%s port=%s host=%s user=%s',
             current_database()::text,
             current_setting('port'),
             split_part(current_setting('unix_socket_directories'), ',', 1),
             current_user::text
         )",
    )
    .unwrap()
    .unwrap()
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use std::ffi::CString;

    use pgrx::prelude::*;
    use pgrx::spi;
    use tpchgen::generators::{
        ClerkName, CustomerGenerator, CustomerName, LineItemGenerator, OrderGenerator,
        PartBrandName, PartGenerator, PartManufacturerName, PartSuppGenerator, SupplierName,
    };
    use tpchgen::{dates::TPCHDate, decimal::TPCHDecimal};

    #[pg_test]
    fn test_hello_pg_tpch() {
        assert_eq!("Hello, pg_tpch", crate::hello_pg_tpch());
    }

    fn schema_literal(schema_name: &str) -> String {
        spi::quote_literal(schema_name)
    }

    fn count_tpch_tables(schema_name: &str) -> i64 {
        let schema_name = schema_literal(schema_name);
        Spi::get_one::<i64>(&format!(
            "SELECT count(*)
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = {schema_name}
               AND c.relkind = 'r'
               AND c.relname IN (
                 'region', 'nation', 'supplier', 'customer',
                 'part', 'partsupp', 'orders', 'lineitem'
               )"
        ))
        .unwrap()
        .unwrap()
    }

    fn count_tpch_constraints(schema_name: &str) -> i64 {
        let schema_name = schema_literal(schema_name);
        Spi::get_one::<i64>(&format!(
            "SELECT count(*)
             FROM pg_constraint c
             JOIN pg_class t ON t.oid = c.conrelid
             JOIN pg_namespace n ON n.oid = t.relnamespace
             WHERE n.nspname = {schema_name}
               AND c.conname IN (
                 'region_pkey', 'nation_pkey', 'supplier_pkey', 'customer_pkey',
                 'part_pkey', 'partsupp_pkey', 'orders_pkey', 'lineitem_pkey'
               )"
        ))
        .unwrap()
        .unwrap()
    }

    fn count_tpch_secondary_indexes(schema_name: &str) -> i64 {
        let schema_name = schema_literal(schema_name);
        Spi::get_one::<i64>(&format!(
            "SELECT count(*)
             FROM pg_index i
             JOIN pg_class idx ON idx.oid = i.indexrelid
             JOIN pg_class tbl ON tbl.oid = i.indrelid
             JOIN pg_namespace n ON n.oid = tbl.relnamespace
             LEFT JOIN pg_constraint c ON c.conindid = idx.oid
             WHERE n.nspname = {schema_name}
               AND tbl.relname IN (
                 'region', 'nation', 'supplier', 'customer',
                 'part', 'partsupp', 'orders', 'lineitem'
               )
               AND c.oid IS NULL"
        ))
        .unwrap()
        .unwrap()
    }

    fn table_count(schema_name: &str, table_name: &str) -> i64 {
        Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM {}.{}",
            spi::quote_identifier(schema_name),
            spi::quote_identifier(table_name)
        ))
        .unwrap()
        .unwrap()
    }

    fn use_test_schema(schema_name: &str) {
        Spi::run(&format!(
            "CREATE SCHEMA IF NOT EXISTS {};
             SET LOCAL search_path = {}, pg_catalog",
            spi::quote_identifier(schema_name),
            spi::quote_identifier(schema_name)
        ))
        .unwrap();
    }

    fn unique_schema_name(prefix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{prefix}_{nanos}")
    }

    fn parse_time_ms(value: &Numeric<20, 2>) -> f64 {
        value.as_anynumeric().clone().try_into().unwrap()
    }

    fn has_two_decimal_places(value: &Numeric<20, 2>) -> bool {
        value
            .to_string()
            .split_once('.')
            .map(|(_, frac)| frac.len() == 2)
            .unwrap_or(false)
    }

    fn dblink_available() -> bool {
        Spi::get_one::<bool>(
            "SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'dblink')",
        )
        .unwrap()
        .unwrap()
    }

    #[pg_test]
    fn test_create_tpch_tables_without_secondary_indexes() {
        let schema_name = "tpch_meta_no_idx";
        use_test_schema(schema_name);

        let result = crate::create_tpch_tables(false);

        assert!(result.contains("Created TPC-H tables"));
        assert!(result.contains(schema_name));
        assert_eq!(8, count_tpch_tables(schema_name));
        assert_eq!(8, count_tpch_constraints(schema_name));
        assert_eq!(0, count_tpch_secondary_indexes(schema_name));
    }

    #[pg_test]
    fn test_create_tpch_tables_with_secondary_indexes() {
        let schema_name = "tpch_meta_full";
        use_test_schema(schema_name);

        let result = crate::create_tpch_tables(true);

        assert!(result.contains("Created TPC-H tables and indexes"));
        assert!(result.contains(schema_name));
        assert_eq!(8, count_tpch_tables(schema_name));
        assert_eq!(8, count_tpch_constraints(schema_name));
        assert_eq!(10, count_tpch_secondary_indexes(schema_name));
    }

    #[pg_test]
    fn test_create_tpch_tables_fails_when_tables_already_exist() {
        let schema_name = "tpch_meta_existing";
        use_test_schema(schema_name);
        let _ = crate::create_tpch_tables(false);

        let panic = std::panic::catch_unwind(|| {
            let _ = crate::create_tpch_tables(false);
        });

        assert!(panic.is_err(), "second create_tpch_tables call should fail");
    }

    #[pg_test]
    fn test_drop_tpch_tables_removes_created_tables() {
        let schema_name = "tpch_meta_drop";
        use_test_schema(schema_name);
        let _ = crate::create_tpch_tables(true);

        let result = crate::drop_tpch_tables();

        assert!(result.contains("Dropped TPC-H tables"));
        assert!(result.contains(schema_name));
        assert_eq!(0, count_tpch_tables(schema_name));
    }

    #[pg_test]
    fn test_cleanup_tpch_data_truncates_all_tables() {
        let schema_name = "tpch_cleanup_data";
        use_test_schema(schema_name);
        let _ = crate::create_tpch_tables(false);
        let _: Vec<_> = crate::tpch_dbgen(0.01).collect();

        let result = crate::cleanup_tpch_data();

        assert!(result.contains("Truncated TPC-H data"));
        assert_eq!(0, table_count(schema_name, "region"));
        assert_eq!(0, table_count(schema_name, "nation"));
        assert_eq!(0, table_count(schema_name, "lineitem"));
    }

    #[pg_test]
    fn test_tpch_dbgen_serial_loads_data() {
        Spi::run("DROP EXTENSION IF EXISTS dblink").unwrap();

        let schema_name = "tpch_dbgen_serial";
        use_test_schema(schema_name);
        let _ = crate::create_tpch_tables(false);
        let scale_factor = 0.01;
        let expected_lineitems = LineItemGenerator::new(scale_factor, 1, 1).iter().count() as i64;

        let result: Vec<_> = crate::tpch_dbgen(scale_factor).collect();

        assert_eq!(8, result.len());
        assert!(result.iter().all(|row| parse_time_ms(&row.2) >= 0.0));
        assert!(result.iter().all(|row| parse_time_ms(&row.3) >= 0.0));
        assert!(result.iter().all(|row| has_two_decimal_places(&row.2)));
        assert!(result.iter().all(|row| has_two_decimal_places(&row.3)));
        assert_eq!(5, table_count(schema_name, "region"));
        assert_eq!(25, table_count(schema_name, "nation"));
        assert_eq!(expected_lineitems, table_count(schema_name, "lineitem"));
        assert_eq!(0, count_tpch_secondary_indexes(schema_name));
    }

    #[pg_test]
    fn test_tpch_dbgen_uses_dblink_when_available() {
        if !dblink_available() {
            return;
        }

        Spi::run("CREATE EXTENSION IF NOT EXISTS dblink").unwrap();

        let schema_name = unique_schema_name("tpch_dbgen_dblink");
        let dblink_schema = crate::extension_schema("dblink").unwrap();
        let dblink_connect =
            spi::quote_qualified_identifier(dblink_schema.as_str(), "dblink_connect");
        let dblink_disconnect =
            spi::quote_qualified_identifier(dblink_schema.as_str(), "dblink_disconnect");
        let dblink_exec = spi::quote_qualified_identifier(dblink_schema.as_str(), "dblink_exec");
        let conn_name = "pg_tpch_test_setup";
        let conninfo_literal = spi::quote_literal(crate::local_conninfo_string());
        crate::dblink_connect_named(&dblink_connect, conn_name, &conninfo_literal);
        crate::dblink_exec_named(
            &dblink_exec,
            conn_name,
            &format!("CREATE SCHEMA {}", spi::quote_identifier(&schema_name)),
        );
        crate::dblink_exec_named(
            &dblink_exec,
            conn_name,
            &format!(
                "SET search_path = {}, pg_catalog",
                spi::quote_identifier(&schema_name)
            ),
        );
        crate::dblink_exec_named(
            &dblink_exec,
            conn_name,
            &crate::schema::tpch_tables_batch_sql(schema_name.as_str()),
        );
        crate::dblink_disconnect_named(&dblink_disconnect, conn_name);
        Spi::run(&format!(
            "SET LOCAL search_path = {}, pg_catalog",
            spi::quote_identifier(&schema_name)
        ))
        .unwrap();
        let scale_factor = 0.01;
        let expected_orders = OrderGenerator::new(scale_factor, 1, 1).iter().count() as i64;

        let result: Vec<_> = crate::tpch_dbgen(scale_factor).collect();

        assert_eq!(8, result.len());
        assert!(result.iter().all(|row| parse_time_ms(&row.2) >= 0.0));
        assert!(result.iter().all(|row| parse_time_ms(&row.3) >= 0.0));
        assert!(result.iter().all(|row| has_two_decimal_places(&row.2)));
        assert!(result.iter().all(|row| has_two_decimal_places(&row.3)));
        assert_eq!(5, table_count(schema_name.as_str(), "region"));
        assert_eq!(expected_orders, table_count(schema_name.as_str(), "orders"));
        assert_eq!(0, count_tpch_secondary_indexes(schema_name.as_str()));
    }

    #[pg_test]
    fn test_tpch_dbgen_fails_when_tables_are_missing() {
        let schema_name = "tpch_dbgen_missing";
        use_test_schema(schema_name);

        let panic = std::panic::catch_unwind(|| {
            let _: Vec<_> = crate::tpch_dbgen(0.01).collect();
        });

        assert!(
            panic.is_err(),
            "tpch_dbgen should fail when tables are missing"
        );
    }

    #[pg_test]
    fn test_create_tpch_tables_work_with_existing_loaders() {
        let schema_name = "tpch_loader_schema";
        use_test_schema(schema_name);
        let _ = crate::create_tpch_tables(false);

        let region_result: Vec<_> = crate::generate_region(1.0).collect();
        let nation_result: Vec<_> = crate::generate_nation(1.0).collect();
        let supplier_result: Vec<_> = crate::generate_supplier(0.01).collect();

        assert_eq!(5, region_result[0].0);
        assert_eq!(25, nation_result[0].0);
        assert_eq!(100, supplier_result[0].0);

        let supplier_count = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM {}.supplier",
            spi::quote_identifier(schema_name)
        ))
        .unwrap()
        .unwrap();
        assert_eq!(100, supplier_count);
    }

    #[pg_test]
    fn test_tpch_queries_returns_all_queries() {
        let result: Vec<_> = crate::tpch_queries(None).collect();

        assert_eq!(22, result.len());
        assert_eq!(1, result.first().unwrap().0);
        assert_eq!(22, result.last().unwrap().0);
        assert!(result.iter().all(|(_, query)| !query.trim().is_empty()));
    }

    #[pg_test]
    fn test_tpch_queries_returns_specific_query() {
        let result: Vec<_> = crate::tpch_queries(Some(15)).collect();

        assert_eq!(1, result.len());
        assert_eq!(15, result[0].0);
        assert!(result[0].1.contains("WITH revenue AS"));
        assert!(result[0].1.contains("1996-01-01"));
    }

    #[pg_test]
    fn test_generate_nation_loads_tpch_seed_data() {
        Spi::run(
            "CREATE TABLE nation (
                n_nationkey BIGINT NOT NULL,
                n_name TEXT NOT NULL,
                n_regionkey BIGINT NOT NULL,
                n_comment TEXT NOT NULL
            )",
        )
        .unwrap();

        let result: Vec<_> = crate::generate_nation(1.0).collect();
        assert_eq!(25, result[0].0);

        let count = Spi::get_one::<i64>("SELECT count(*) FROM nation")
            .unwrap()
            .unwrap();
        assert_eq!(25, count);

        let first = Spi::get_three::<i64, String, i64>(
            "SELECT n_nationkey, n_name, n_regionkey
             FROM nation
             ORDER BY n_nationkey
             LIMIT 1",
        )
        .unwrap();
        assert_eq!((Some(0), Some("ALGERIA".to_string()), Some(0)), first);

        let last = Spi::get_three::<i64, String, i64>(
            "SELECT n_nationkey, n_name, n_regionkey
             FROM nation
             ORDER BY n_nationkey DESC
             LIMIT 1",
        )
        .unwrap();
        assert_eq!((Some(24), Some("UNITED STATES".to_string()), Some(1)), last);
    }

    #[pg_test]
    fn test_generate_nation_loads_into_indexed_tables() {
        Spi::run(
            "CREATE TABLE nation (
                n_nationkey BIGINT PRIMARY KEY,
                n_name TEXT NOT NULL,
                n_regionkey BIGINT NOT NULL,
                n_comment TEXT NOT NULL
            )",
        )
        .unwrap();

        let result: Vec<_> = crate::generate_nation(1.0).collect();
        assert_eq!(25, result[0].0);

        let count = Spi::get_one::<i64>("SELECT count(*) FROM nation")
            .unwrap()
            .unwrap();
        assert_eq!(25, count);

        let nation_name = Spi::get_one::<String>(
            "SELECT n_name
             FROM nation
             WHERE n_nationkey = 24",
        )
        .unwrap()
        .unwrap();
        assert_eq!("UNITED STATES", nation_name);
    }

    #[pg_test]
    fn test_generate_nation_sets_xmin_and_xmax() {
        Spi::run(
            "CREATE TABLE nation (
                n_nationkey BIGINT NOT NULL,
                n_name TEXT NOT NULL,
                n_regionkey BIGINT NOT NULL,
                n_comment TEXT NOT NULL
            )",
        )
        .unwrap();

        let result: Vec<_> = crate::generate_nation(1.0).collect();
        assert_eq!(25, result[0].0);

        let current_xid = Spi::get_one::<i64>("SELECT (txid_current() % 4294967296)::bigint")
            .unwrap()
            .unwrap();

        let distinct_xmin =
            Spi::get_one::<i64>("SELECT count(DISTINCT xmin::text::bigint) FROM nation")
                .unwrap()
                .unwrap();
        let min_xmin = Spi::get_one::<i64>("SELECT min(xmin::text::bigint) FROM nation")
            .unwrap()
            .unwrap();
        let max_xmin = Spi::get_one::<i64>("SELECT max(xmin::text::bigint) FROM nation")
            .unwrap()
            .unwrap();
        let all_xmax_invalid = Spi::get_one::<bool>("SELECT bool_and(xmax = '0'::xid) FROM nation")
            .unwrap()
            .unwrap();

        assert_eq!(1, distinct_xmin);
        assert_eq!(current_xid, min_xmin);
        assert_eq!(current_xid, max_xmin);
        assert!(all_xmax_invalid);
    }

    #[pg_test]
    fn test_generate_region_loads_tpch_seed_data() {
        Spi::run(
            "CREATE TABLE region (
                r_regionkey BIGINT PRIMARY KEY,
                r_name TEXT NOT NULL,
                r_comment TEXT NOT NULL
            )",
        )
        .unwrap();

        let result: Vec<_> = crate::generate_region(1.0).collect();
        assert_eq!(5, result[0].0);

        let count = Spi::get_one::<i64>("SELECT count(*) FROM region")
            .unwrap()
            .unwrap();
        assert_eq!(5, count);

        let first = Spi::get_two::<i64, String>(
            "SELECT r_regionkey, r_name
             FROM region
             ORDER BY r_regionkey
             LIMIT 1",
        )
        .unwrap();
        assert_eq!((Some(0), Some("AFRICA".to_string())), first);

        let last = Spi::get_two::<i64, String>(
            "SELECT r_regionkey, r_name
             FROM region
             ORDER BY r_regionkey DESC
             LIMIT 1",
        )
        .unwrap();
        assert_eq!((Some(4), Some("MIDDLE EAST".to_string())), last);
    }

    #[pg_test]
    fn test_generate_supplier_loads_indexed_table_with_numeric_columns() {
        Spi::run(
            "CREATE TABLE supplier (
                s_suppkey BIGINT PRIMARY KEY,
                s_name TEXT NOT NULL,
                s_address TEXT NOT NULL,
                s_nationkey BIGINT NOT NULL,
                s_phone TEXT NOT NULL,
                s_acctbal NUMERIC(15,2) NOT NULL,
                s_comment TEXT NOT NULL
            )",
        )
        .unwrap();

        let result: Vec<_> = crate::generate_supplier(0.01).collect();
        assert_eq!(100, result[0].0);

        let count = Spi::get_one::<i64>("SELECT count(*) FROM supplier")
            .unwrap()
            .unwrap();
        assert_eq!(100, count);

        let first = Spi::get_three::<String, i64, String>(
            "SELECT s_name, s_nationkey, s_acctbal::text
             FROM supplier
             WHERE s_suppkey = 1",
        )
        .unwrap();
        assert_eq!(
            (
                Some("Supplier#000000001".to_string()),
                Some(17),
                Some("5755.94".to_string())
            ),
            first
        );
    }

    #[pg_test]
    fn test_generate_orders_loads_dates_and_prices() {
        let scale_factor = 0.01;
        let expected_rows = OrderGenerator::new(scale_factor, 1, 1).iter().count() as i64;
        let expected_first_order = OrderGenerator::new(scale_factor, 1, 1)
            .iter()
            .next()
            .unwrap();

        Spi::run(
            "CREATE TABLE orders (
                o_orderkey BIGINT PRIMARY KEY,
                o_custkey BIGINT NOT NULL,
                o_orderstatus TEXT NOT NULL,
                o_totalprice NUMERIC(15,2) NOT NULL,
                o_orderdate DATE NOT NULL,
                o_orderpriority TEXT NOT NULL,
                o_clerk TEXT NOT NULL,
                o_shippriority INTEGER NOT NULL,
                o_comment TEXT NOT NULL
            )",
        )
        .unwrap();

        let result: Vec<_> = crate::generate_orders(scale_factor).collect();
        assert_eq!(expected_rows, result[0].0);

        let count = Spi::get_one::<i64>("SELECT count(*) FROM orders")
            .unwrap()
            .unwrap();
        assert_eq!(expected_rows, count);

        let first = Spi::get_three::<String, String, String>(
            "SELECT o_orderstatus, o_totalprice::text, o_orderdate::text
             FROM orders
             WHERE o_orderkey = 1",
        )
        .unwrap();
        assert_eq!(
            (
                Some(expected_first_order.o_orderstatus.to_string()),
                Some(expected_first_order.o_totalprice.to_string()),
                Some(expected_first_order.o_orderdate.to_string())
            ),
            first
        );
    }

    #[pg_test]
    fn test_generate_part_loads_numeric_columns() {
        let scale_factor = 0.01;
        let expected_rows = PartGenerator::new(scale_factor, 1, 1).iter().count() as i64;
        let expected_first_part = PartGenerator::new(scale_factor, 1, 1)
            .iter()
            .next()
            .unwrap();

        Spi::run(
            "CREATE TABLE part (
                p_partkey BIGINT PRIMARY KEY,
                p_name TEXT NOT NULL,
                p_mfgr TEXT NOT NULL,
                p_brand TEXT NOT NULL,
                p_type TEXT NOT NULL,
                p_size INTEGER NOT NULL,
                p_container TEXT NOT NULL,
                p_retailprice NUMERIC(15,2) NOT NULL,
                p_comment TEXT NOT NULL
            )",
        )
        .unwrap();

        let result: Vec<_> = crate::generate_part(scale_factor).collect();
        assert_eq!(expected_rows, result[0].0);

        let count = Spi::get_one::<i64>("SELECT count(*) FROM part")
            .unwrap()
            .unwrap();
        assert_eq!(expected_rows, count);

        let first = Spi::get_three::<String, String, String>(
            "SELECT p_name, p_brand, p_retailprice::text
             FROM part
             WHERE p_partkey = 1",
        )
        .unwrap();
        assert_eq!(
            (
                Some(expected_first_part.p_name.to_string()),
                Some(expected_first_part.p_brand.to_string()),
                Some(expected_first_part.p_retailprice.to_string())
            ),
            first
        );
    }

    #[pg_test]
    fn test_generate_customer_loads_numeric_columns() {
        let scale_factor = 0.01;
        let expected_rows = CustomerGenerator::new(scale_factor, 1, 1).iter().count() as i64;
        let expected_first_customer = CustomerGenerator::new(scale_factor, 1, 1)
            .iter()
            .next()
            .unwrap();

        Spi::run(
            "CREATE TABLE customer (
                c_custkey BIGINT PRIMARY KEY,
                c_name TEXT NOT NULL,
                c_address TEXT NOT NULL,
                c_nationkey BIGINT NOT NULL,
                c_phone TEXT NOT NULL,
                c_acctbal NUMERIC(15,2) NOT NULL,
                c_mktsegment TEXT NOT NULL,
                c_comment TEXT NOT NULL
            )",
        )
        .unwrap();

        let result: Vec<_> = crate::generate_customer(scale_factor).collect();
        assert_eq!(expected_rows, result[0].0);

        let count = Spi::get_one::<i64>("SELECT count(*) FROM customer")
            .unwrap()
            .unwrap();
        assert_eq!(expected_rows, count);

        let first = Spi::get_three::<String, String, String>(
            "SELECT c_name, c_mktsegment, c_acctbal::text
             FROM customer
             WHERE c_custkey = 1",
        )
        .unwrap();
        assert_eq!(
            (
                Some(expected_first_customer.c_name.to_string()),
                Some(expected_first_customer.c_mktsegment.to_string()),
                Some(expected_first_customer.c_acctbal.to_string())
            ),
            first
        );
    }

    #[pg_test]
    fn test_generate_partsupp_loads_supplycost() {
        let scale_factor = 0.01;
        let expected_rows = PartSuppGenerator::new(scale_factor, 1, 1).iter().count() as i64;
        let expected_first_partsupp = PartSuppGenerator::new(scale_factor, 1, 1)
            .iter()
            .next()
            .unwrap();

        Spi::run(
            "CREATE TABLE partsupp (
                ps_partkey BIGINT NOT NULL,
                ps_suppkey BIGINT NOT NULL,
                ps_availqty INTEGER NOT NULL,
                ps_supplycost NUMERIC(15,2) NOT NULL,
                ps_comment TEXT NOT NULL
            )",
        )
        .unwrap();

        let result: Vec<_> = crate::generate_partsupp(scale_factor).collect();
        assert_eq!(expected_rows, result[0].0);

        let count = Spi::get_one::<i64>("SELECT count(*) FROM partsupp")
            .unwrap()
            .unwrap();
        assert_eq!(expected_rows, count);

        let first = Spi::get_three::<i64, i32, String>(
            "SELECT ps_suppkey, ps_availqty, ps_supplycost::text
             FROM partsupp
             WHERE ps_partkey = 1
             ORDER BY ps_suppkey
             LIMIT 1",
        )
        .unwrap();
        assert_eq!(
            (
                Some(expected_first_partsupp.ps_suppkey),
                Some(expected_first_partsupp.ps_availqty),
                Some(expected_first_partsupp.ps_supplycost.to_string())
            ),
            first
        );
    }

    #[pg_test]
    fn test_generate_lineitem_loads_numeric_and_date_columns() {
        let scale_factor = 0.01;
        let expected_rows = LineItemGenerator::new(scale_factor, 1, 1).iter().count() as i64;
        let expected_first_lineitem = LineItemGenerator::new(scale_factor, 1, 1)
            .iter()
            .next()
            .unwrap();

        Spi::run(
            "CREATE TABLE lineitem (
                l_orderkey BIGINT NOT NULL,
                l_partkey BIGINT NOT NULL,
                l_suppkey BIGINT NOT NULL,
                l_linenumber INTEGER NOT NULL,
                l_quantity NUMERIC(15,2) NOT NULL,
                l_extendedprice NUMERIC(15,2) NOT NULL,
                l_discount NUMERIC(15,2) NOT NULL,
                l_tax NUMERIC(15,2) NOT NULL,
                l_returnflag TEXT NOT NULL,
                l_linestatus TEXT NOT NULL,
                l_shipdate DATE NOT NULL,
                l_commitdate DATE NOT NULL,
                l_receiptdate DATE NOT NULL,
                l_shipinstruct TEXT NOT NULL,
                l_shipmode TEXT NOT NULL,
                l_comment TEXT NOT NULL
            )",
        )
        .unwrap();

        let result: Vec<_> = crate::generate_lineitem(scale_factor).collect();
        assert_eq!(expected_rows, result[0].0);

        let count = Spi::get_one::<i64>("SELECT count(*) FROM lineitem")
            .unwrap()
            .unwrap();
        assert_eq!(expected_rows, count);

        let first = Spi::get_three::<String, String, String>(
            "SELECT l_quantity::text, l_extendedprice::text, l_shipdate::text
             FROM lineitem
             WHERE l_orderkey = 1 AND l_linenumber = 1",
        )
        .unwrap();
        assert_eq!(
            (
                Some(expected_first_lineitem.l_quantity.to_string()),
                Some(expected_first_lineitem.l_extendedprice.to_string()),
                Some(expected_first_lineitem.l_shipdate.to_string())
            ),
            first
        );
    }

    unsafe fn copy_varlena_bytes_from_datum(datum: pg_sys::Datum) -> Vec<u8> {
        let ptr = datum.cast_mut_ptr::<u8>();
        #[cfg(target_endian = "little")]
        let len = ((ptr.cast::<u32>().read_unaligned() >> 2) & 0x3fff_ffff) as usize;
        #[cfg(target_endian = "big")]
        let len = (ptr.cast::<u32>().read_unaligned() & 0x3fff_ffff) as usize;

        std::slice::from_raw_parts(ptr as *const u8, len).to_vec()
    }

    fn scaled_i64_to_numeric_input(value: i64, dscale: u16) -> String {
        if dscale == 0 {
            return value.to_string();
        }

        let negative = value < 0;
        let digits = value.unsigned_abs().to_string();
        let split_at = digits.len().saturating_sub(dscale as usize);

        let mut out = String::new();
        if negative {
            out.push('-');
        }

        if split_at == 0 {
            out.push('0');
        } else {
            out.push_str(&digits[..split_at]);
        }

        out.push('.');

        if split_at == 0 {
            out.push_str(&"0".repeat(dscale as usize - digits.len()));
            out.push_str(&digits);
        } else {
            out.push_str(&digits[split_at..]);
        }

        out
    }

    #[pg_test]
    fn test_text_encoding_matches_textin() {
        let cases = ["", "R", "MAIL", "ship comment 123"];

        unsafe {
            for case in cases {
                let cstr = CString::new(case).unwrap();
                let reference = pgrx::direct_function_call::<pg_sys::Datum>(
                    pg_sys::textin,
                    &[Some(pg_sys::Datum::from(cstr.as_ptr() as usize))],
                )
                .unwrap();

                let expected = copy_varlena_bytes_from_datum(reference);
                let actual = crate::encoding::encode_text_bytes(case);

                assert_eq!(expected, actual, "text encoding mismatch for {case:?}");
                pg_sys::pfree(reference.cast_mut_ptr());
            }
        }
    }

    #[pg_test]
    fn test_formatted_text_encoding_matches_textin() {
        let cases = [
            PartManufacturerName::new(3).to_string(),
            PartBrandName::new(13).to_string(),
            SupplierName::new(1).to_string(),
            CustomerName::new(42).to_string(),
            ClerkName::new(951).to_string(),
        ];

        unsafe {
            for case in cases {
                let cstr = CString::new(case.clone()).unwrap();
                let reference = pgrx::direct_function_call::<pg_sys::Datum>(
                    pg_sys::textin,
                    &[Some(pg_sys::Datum::from(cstr.as_ptr() as usize))],
                )
                .unwrap();

                let expected = copy_varlena_bytes_from_datum(reference);
                let actual = crate::encoding::encode_text_display_bytes(&case);

                assert_eq!(expected, actual, "text encoding mismatch for {case:?}");
                pg_sys::pfree(reference.cast_mut_ptr());
            }
        }
    }

    #[pg_test]
    fn test_integer_numeric_encoding_matches_numeric_in() {
        let cases = [0_i64, 1, 50, 10_000, -1, -10_000, 9_999_999_999];

        unsafe {
            for case in cases {
                let cstr = CString::new(case.to_string()).unwrap();
                let reference = pgrx::direct_function_call::<pg_sys::Datum>(
                    pg_sys::numeric_in,
                    &[
                        Some(pg_sys::Datum::from(cstr.as_ptr() as usize)),
                        Some(pg_sys::Datum::from(pg_sys::InvalidOid)),
                        Some((-1_i32).into_datum().unwrap()),
                    ],
                )
                .unwrap();

                let expected = copy_varlena_bytes_from_datum(reference);
                let actual = crate::encoding::encode_numeric_i64_bytes(case);

                assert_eq!(expected, actual, "numeric encoding mismatch for {case}");
                pg_sys::pfree(reference.cast_mut_ptr());
            }
        }
    }

    #[pg_test]
    fn test_scaled_numeric_encoding_matches_numeric_in() {
        let cases = [
            TPCHDecimal::new(0),
            TPCHDecimal::new(1),
            TPCHDecimal::new(10),
            TPCHDecimal::new(1234),
            TPCHDecimal::new(-1234),
            TPCHDecimal::new(999_999_999_999),
        ];

        unsafe {
            for case in cases {
                let text = case.to_string();
                let cstr = CString::new(text.clone()).unwrap();
                let reference = pgrx::direct_function_call::<pg_sys::Datum>(
                    pg_sys::numeric_in,
                    &[
                        Some(pg_sys::Datum::from(cstr.as_ptr() as usize)),
                        Some(pg_sys::Datum::from(pg_sys::InvalidOid)),
                        Some((-1_i32).into_datum().unwrap()),
                    ],
                )
                .unwrap();

                let expected = copy_varlena_bytes_from_datum(reference);
                let actual =
                    crate::encoding::encode_numeric_i64_with_dscale_2_bytes(case.into_inner());

                assert_eq!(
                    expected, actual,
                    "scaled numeric encoding mismatch for {text}"
                );
                pg_sys::pfree(reference.cast_mut_ptr());
            }
        }
    }

    #[pg_test]
    fn test_generic_scaled_numeric_encoding_matches_numeric_in() {
        let cases = [
            (0_i64, 0_u16),
            (1, 0),
            (1234, 2),
            (-1234, 2),
            (1, 5),
            (12345, 5),
            (1, 6),
            (-987_654, 3),
            (123_450_000, 6),
            (1, 10),
        ];

        unsafe {
            for (value, dscale) in cases {
                let text = scaled_i64_to_numeric_input(value, dscale);
                let cstr = CString::new(text.clone()).unwrap();
                let reference = pgrx::direct_function_call::<pg_sys::Datum>(
                    pg_sys::numeric_in,
                    &[
                        Some(pg_sys::Datum::from(cstr.as_ptr() as usize)),
                        Some(pg_sys::Datum::from(pg_sys::InvalidOid)),
                        Some((-1_i32).into_datum().unwrap()),
                    ],
                )
                .unwrap();

                let expected = copy_varlena_bytes_from_datum(reference);
                let actual = crate::encoding::encode_numeric_i64_with_scale_bytes(value, dscale);

                assert_eq!(
                    expected, actual,
                    "scaled numeric encoding mismatch for {text}"
                );
                pg_sys::pfree(reference.cast_mut_ptr());
            }
        }
    }

    #[pg_test]
    fn test_tpch_date_encoding_matches_make_date() {
        let cases = [
            TPCHDate::new(tpchgen::dates::MIN_GENERATE_DATE),
            TPCHDate::new(tpchgen::dates::MIN_GENERATE_DATE + 41),
            TPCHDate::new(tpchgen::dates::MIN_GENERATE_DATE + 2556),
        ];

        for case in cases {
            let (year, month, day) = case.to_ymd();
            let reference = pgrx::datum::Date::new(1900 + year, month as u8, day as u8)
                .unwrap()
                .into_datum()
                .unwrap();
            let actual = crate::encoding::date_datum(case);

            assert_eq!(
                reference.value(),
                actual.value(),
                "date encoding mismatch for {case}"
            );
        }
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
