use pgrx::prelude::*;
::pgrx::pg_module_magic!();

mod loader;
mod utils;

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

#[pg_extern]
fn hello_pg_tpchrs() -> &'static str {
    "Hello, pg_tpchrs"
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;
    use tpchgen::generators::{
        CustomerGenerator, LineItemGenerator, OrderGenerator, PartGenerator, PartSuppGenerator,
    };

    #[pg_test]
    fn test_hello_pg_tpchrs() {
        assert_eq!("Hello, pg_tpchrs", crate::hello_pg_tpchrs());
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
        let expected_first_order = OrderGenerator::new(0.0001, 1, 1).iter().next().unwrap();

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

        let result: Vec<_> = crate::generate_orders(0.0001).collect();
        assert_eq!(150, result[0].0);

        let count = Spi::get_one::<i64>("SELECT count(*) FROM orders")
            .unwrap()
            .unwrap();
        assert_eq!(150, count);

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
        let scale_factor = 0.001;
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
        assert_eq!(200, result[0].0);

        let count = Spi::get_one::<i64>("SELECT count(*) FROM part")
            .unwrap()
            .unwrap();
        assert_eq!(200, count);

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
        let scale_factor = 0.001;
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
        assert_eq!(150, result[0].0);

        let count = Spi::get_one::<i64>("SELECT count(*) FROM customer")
            .unwrap()
            .unwrap();
        assert_eq!(150, count);

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
        let scale_factor = 0.001;
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
        assert_eq!(800, result[0].0);

        let count = Spi::get_one::<i64>("SELECT count(*) FROM partsupp")
            .unwrap()
            .unwrap();
        assert_eq!(800, count);

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
        let scale_factor = 0.0001;
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
