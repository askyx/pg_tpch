DROP SCHEMA IF EXISTS tpch CASCADE;

CREATE SCHEMA tpch;

CREATE TABLE tpch.tpch_query_stats(
  ec_qid INT,
  ec_duration DOUBLE PRECISION,
  ec_recoed_time TIMESTAMP
);

CREATE TABLE tpch.tpch_tables(table_name VARCHAR(100));

INSERT INTO tpch.tpch_tables VALUES ('customer');
INSERT INTO tpch.tpch_tables VALUES ('lineitem');
INSERT INTO tpch.tpch_tables VALUES ('nation');
INSERT INTO tpch.tpch_tables VALUES ('orders');
INSERT INTO tpch.tpch_tables VALUES ('part');
INSERT INTO tpch.tpch_tables VALUES ('partsupp');
INSERT INTO tpch.tpch_tables VALUES ('region');
INSERT INTO tpch.tpch_tables VALUES ('supplier');