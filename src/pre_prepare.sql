DROP SCHEMA IF EXISTS tpch CASCADE;

CREATE SCHEMA tpch;

CREATE TABLE tpch.tpch_query_stats(
  ec_qid INT,
  ec_duration DOUBLE PRECISION,
  ec_recoed_time TIMESTAMP
);

create table tpch.tpch_tables(
  table_name varchar(100),
  status int, 
  child varchar(100));

INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('customer', 0);
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('lineitem', 1);
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('nation', 0);
INSERT INTO tpch.tpch_tables(table_name, status, child) VALUES ('orders', 2, 'lineitem');
INSERT INTO tpch.tpch_tables(table_name, status, child) VALUES ('part', 2, 'partsupp');
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('partsupp', 1);
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('region', 0);
INSERT INTO tpch.tpch_tables(table_name, status) VALUES ('supplier', 0);