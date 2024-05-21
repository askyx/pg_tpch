drop schema if exists tpch cascade;
create schema tpch;

create table tpch.tpch_query_stats(
  ec_qid int ,
  ec_duration double precision,
  ec_recoed_time timestamp
);

create table tpch.tpch_tables(
  table_name varchar(100),
  status int, 
  child varchar(100));

INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('call_center', 0);
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('catalog_page', 0);
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('catalog_returns', 1);
INSERT INTO tpch.tpch_tables(table_name, status, child) VALUES ('catalog_sales', 2, 'catalog_returns');
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('customer', 0);
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('customer_address', 0);
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('customer_demographics', 0);
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('date_dim', 0);
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('household_demographics', 0);
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('income_band', 0);
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('inventory', 0);
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('item', 0);
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('promotion', 0);
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('reason', 0);
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('ship_mode', 0);
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('store', 0);
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('store_returns', 1);
INSERT INTO tpch.tpch_tables(table_name, status, child) VALUES ('store_sales', 2, 'store_returns');
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('time_dim', 0);
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('warehouse', 0);
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('web_page', 0);
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('web_returns', 1);
INSERT INTO tpch.tpch_tables(table_name, status, child) VALUES ('web_sales', 2, 'web_returns');
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('web_site', 0);