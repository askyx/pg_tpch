DROP SCHEMA IF EXISTS tpch CASCADE;

CREATE SCHEMA tpch;

CREATE TABLE tpch.tpch_query_stats(
  ec_qid INT,
  ec_duration DOUBLE PRECISION,
  ec_recoed_time TIMESTAMP
);

create table tpch.tpch_tables(table_name varchar(100));

insert into tpch.tpch_tables(table_name) values 
  ('customer'),
  ('nation'),
  ('region'),
  ('supplier'),
  ('lineitem'),
  ('partsupp'),
  ('orders'),
  ('part');