use pgrx::{prelude::*, spi};

const METADATA_SCHEMA_NAME: &str = "tpch";
const METADATA_TABLE_NAME: &str = "tpch_table_metadata";
const QUERY_TABLE_NAME: &str = "tpch_query_metadata";

pub(crate) fn create_tpch_tables(create_indexes: bool) -> String {
    let schema_name = current_target_schema_name();
    set_local_search_path(&schema_name);

    execute_statements(&table_definitions(), &schema_name, "tables");

    if create_indexes {
        for (_, statements) in index_statements_by_table() {
            execute_statements(&statements, &schema_name, "indexes");
        }
        format!(
            "Created TPC-H tables and indexes in schema {:?}",
            schema_name
        )
    } else {
        format!("Created TPC-H tables in schema {:?}", schema_name)
    }
}

pub(crate) fn ensure_tpch_tables_exist() {
    let schema_name = current_target_schema_name();
    let missing_tables = missing_table_names(&schema_name);
    assert!(
        missing_tables.is_empty(),
        "TPC-H tables are missing in schema {:?}: {}. Please create the tables first",
        schema_name,
        missing_tables.join(", ")
    );
}

pub(crate) fn create_tpch_indexes() -> String {
    let schema_name = current_target_schema_name();
    set_local_search_path(&schema_name);
    for (_, statements) in index_statements_by_table() {
        execute_statements(&statements, &schema_name, "indexes");
    }
    format!("Created TPC-H indexes in schema {:?}", schema_name)
}

pub(crate) fn drop_tpch_tables() -> String {
    let schema_name = current_target_schema_name();

    for table_name in table_names() {
        let qualified_name = spi::quote_qualified_identifier(schema_name.as_str(), &table_name);
        Spi::run(&format!("DROP TABLE IF EXISTS {qualified_name}")).unwrap();
    }

    format!("Dropped TPC-H tables from schema {:?}", schema_name)
}

pub(crate) fn cleanup_tpch_data() -> String {
    let schema_name = current_target_schema_name();
    ensure_tpch_tables_exist();

    let qualified_tables = table_names()
        .into_iter()
        .map(|table_name| spi::quote_qualified_identifier(schema_name.as_str(), &table_name))
        .collect::<Vec<_>>();

    Spi::run(&format!("TRUNCATE TABLE {}", qualified_tables.join(", "))).unwrap();

    format!("Truncated TPC-H data in schema {:?}", schema_name)
}

pub(crate) fn tpch_queries(qid: Option<i32>) -> Vec<(i32, String)> {
    ensure_tpch_query_metadata_seeded();

    let query_table = query_table_qname();
    let sql = match qid {
        Some(qid) => format!(
            "SELECT qid, query::text
             FROM {query_table}
             WHERE qid = {}
             ORDER BY qid",
            qid
        ),
        None => format!(
            "SELECT qid, query::text
             FROM {query_table}
             ORDER BY qid"
        ),
    };

    Spi::connect(|client| {
        client
            .select(&sql, None, &[])
            .unwrap()
            .map(|row| {
                (
                    row["qid"].value::<i32>().unwrap().unwrap(),
                    row["query"].value::<String>().unwrap().unwrap(),
                )
            })
            .collect()
    })
}

#[cfg(any(test, feature = "pg_test"))]
pub(crate) fn tpch_tables_batch_sql(schema_name: &str) -> String {
    batch_sql(schema_name, &table_definitions())
}

pub(crate) fn index_statements_by_table() -> Vec<(String, Vec<String>)> {
    let metadata_table = metadata_table_qname();
    Spi::connect(|client| {
        let table = client
            .select(
                &format!(
                    "SELECT table_name::text, table_indexes::text[]
                     FROM {metadata_table}
                     ORDER BY table_name"
                ),
                None,
                &[],
            )
            .unwrap();

        table
            .map(|row| {
                let table_name = row["table_name"].value::<String>().unwrap().unwrap();
                let statements = row["table_indexes"]
                    .value::<Vec<String>>()
                    .unwrap()
                    .unwrap_or_default();
                (table_name, statements)
            })
            .collect()
    })
}

pub(crate) fn current_target_schema_name() -> String {
    Spi::get_one::<String>("SELECT current_schema()::text")
        .unwrap()
        .unwrap_or_else(|| panic!("current schema is not set"))
}

fn set_local_search_path(schema_name: &str) {
    let quoted_schema = spi::quote_identifier(schema_name);
    Spi::run(&format!(
        "SET LOCAL search_path = {quoted_schema}, pg_catalog"
    ))
    .unwrap();
}

#[cfg(any(test, feature = "pg_test"))]
fn batch_sql(_schema_name: &str, statements: &[String]) -> String {
    let mut sql = String::new();
    for statement in statements {
        sql.push_str(statement);
        sql.push(';');
        sql.push(' ');
    }
    sql.trim().to_string()
}

fn execute_statements(statements: &[String], schema_name: &str, object_kind: &str) {
    for statement in statements {
        if let Err(error) = Spi::run(statement) {
            panic!(
                "failed to create TPC-H {object_kind} in schema {:?}: {}. If tables already exist, please delete the existing tables first and retry",
                schema_name,
                error
            );
        }
    }
}

fn table_names() -> Vec<String> {
    metadata_array_query("table_name", "table_name DESC")
}

fn missing_table_names(schema_name: &str) -> Vec<String> {
    let metadata_table = metadata_table_qname();
    Spi::get_one::<Vec<String>>(&format!(
        "SELECT COALESCE(
             array_agg(m.table_name::text ORDER BY m.table_name),
             ARRAY[]::text[]
         )
         FROM {metadata_table} AS m
         WHERE NOT EXISTS (
             SELECT 1
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = {}
               AND c.relname = m.table_name
               AND c.relkind = 'r'
         )",
        spi::quote_literal(schema_name)
    ))
    .unwrap()
    .unwrap_or_default()
}

fn table_definitions() -> Vec<String> {
    metadata_array_query("table_def", "table_name")
}

fn metadata_array_query(column_name: &str, order_by: &str) -> Vec<String> {
    let metadata_table = metadata_table_qname();
    Spi::get_one::<Vec<String>>(&format!(
        "SELECT COALESCE(
             array_agg(({column_name})::text ORDER BY {order_by}),
             ARRAY[]::text[]
         )
         FROM {metadata_table}"
    ))
    .unwrap()
    .unwrap_or_default()
}

fn metadata_table_qname() -> String {
    spi::quote_qualified_identifier(METADATA_SCHEMA_NAME, METADATA_TABLE_NAME)
}

fn query_table_qname() -> String {
    spi::quote_qualified_identifier(METADATA_SCHEMA_NAME, QUERY_TABLE_NAME)
}

fn ensure_tpch_query_metadata_seeded() {
    let query_table = query_table_qname();
    let seeded = Spi::get_one::<bool>(&format!(
        "SELECT EXISTS (SELECT 1 FROM {query_table} LIMIT 1)"
    ))
    .unwrap()
    .unwrap_or(false);

    if seeded {
        return;
    }

    for (qid, query) in crate::tpch_queries::TPCH_QUERY_TEXTS {
        Spi::run(&format!(
            "INSERT INTO {query_table} (qid, query)
             VALUES ({}, {})
             ON CONFLICT (qid) DO UPDATE SET query = EXCLUDED.query",
            qid,
            spi::quote_literal(query)
        ))
        .unwrap();
    }
}
