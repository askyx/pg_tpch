select * from tpch_cleanup();

select * from dbgen(0.1);


-- select * from tpch;
-- select * from tpch;
-- select * from tpch(1,1,1);
-- select * from tpch(2,2,2);

select count(*) from  customer;
select count(*) from  lineitem;
select count(*) from  nation;
select count(*) from  orders;
select count(*) from  part;
select count(*) from  partsupp;
select count(*) from  region;
select count(*) from  supplier;