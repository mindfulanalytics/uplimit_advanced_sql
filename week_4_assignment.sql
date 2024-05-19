-- Advanced SQL Week 4 Assignment
-- Author: Diego Galan
-- Date: 05/19/2024 

-- PART 1:

-- Step 1: Create auto_customers_urgent_orders cte with relevant fields, qualify for the top 3 orders
-- Step 2: Group by custkey, and report on last_order_date, order_numbers using list_agg function and total_spent
-- Step 3: Left join the lineitem table to the base auto_customers_urgent_orders table on orderkey, and qualify for the top 3 parts
-- Step 4: Use aggregate case when statements to get the part 1, 2, and 3 rows we are looking for
-- Step 5: Report on final results, order by last_order_date desc and limit 100

-- select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

-- select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM;

-- select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS;

-- select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.PART;

with auto_customers_urgent_orders as (
-- Rows 29,752

select 
    c.c_custkey,
    o.o_orderkey,
    o.o_totalprice,
    o.o_orderdate,
    row_number() over(partition by c.c_custkey order by o.o_totalprice desc) as total_order_price_rank
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER as c 
inner join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS as o 
    on c.c_custkey = o.o_custkey
where 1=1 
    and c.c_mktsegment like 'AUTOMOBILE'
    and o.o_orderpriority like '1-URGENT'
qualify total_order_price_rank <= 3
order by c.c_custkey, o.o_totalprice desc 

),

order_agg as (
-- Number of rows 18,367
select 
    c_custkey,
    max(o_orderdate) as last_order_date,
    listagg(o_orderkey, ', ') within group (order by o_orderkey) as order_numbers,
    sum(o_totalprice) as total_spent
from auto_customers_urgent_orders
group by 1 

),

part_details as (

select 
    co.c_custkey,
    co.o_orderkey,
    li.l_partkey,
    li.l_quantity,
    li.l_extendedprice,
    row_number() over(partition by co.c_custkey order by li.l_extendedprice desc) as part_spent_rank
from auto_customers_urgent_orders as co
inner join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM as li 
    on co.o_orderkey = li.l_orderkey
qualify part_spent_rank <= 3

),

customer_top_3_parts as (

select 
    c_custkey,
    max(case when part_spent_rank = 1 then o_orderkey end) as part_1_key,
    max(case when part_spent_rank = 1 then l_quantity end) as part_1_quantity,
    max(case when part_spent_rank = 1 then l_extendedprice end) as part_1_price,
    max(case when part_spent_rank = 2 then o_orderkey end) as part_2_key,
    max(case when part_spent_rank = 2 then l_quantity end) as part_2_quantity,
    max(case when part_spent_rank = 2 then l_extendedprice end) as part_2_price,
    max(case when part_spent_rank = 3 then o_orderkey end) as part_3_key,
    max(case when part_spent_rank = 3 then l_quantity end) as part_3_quantity,
    max(case when part_spent_rank = 3 then l_extendedprice end) as part_3_price
from part_details
group by 1

)
-- Total rows 18,367


select
    oa.c_custkey,
    last_order_date,
    order_numbers,
    total_spent,
    part_1_key,
    part_1_quantity,
    part_1_price,
    part_2_key,
    part_2_quantity,
    part_2_price,
    part_3_key,
    part_3_quantity,
    part_3_price
from order_agg as oa
inner join customer_top_3_parts ctp 
    on oa.c_custkey = ctp.c_custkey
order by last_order_date desc, c_custkey
limit 100

;


/* 

PART 2:
Review the candidate's tech exercise below, and provide a one-paragraph assessment of the SQL quality. Provide examples/suggestions for improvement if you think the candidate could have chosen a better approach.

- Q:Do you agree with the results returned by the query?
-A: I do not agree. The query returns 17,304 rows while my query returns 18,367 rows. Looking into the why, it looks like the query does a bunch of inner joins to the urgent_orders cte and filters each by price_rank. 
This filters out any automobile customers with urgent order who ordered less than 3 parts. Also, they only use one window row_number function. They should have used two, one to pull the top 3 orders based on order spent, 
and one to pull the top 3 parts based on part spend. The top_orders cte currently gives the total spent on the top 3 parts, not the total spent on the top 3 orders.

- Q: Is it easy to understand?
- A: The query is easy to understand, but the use of aliases when referring to fields from specific tables, and better cte naming would have made it more readable.

- Q: Could the code be more efficient?
- A: They use order by clauses in both ctes leading up to the final output. This costly clause should only be added to the final cte to provide the report on the requested format. Also, the multiple self joins 
to the urgent_orders cte is a more costly way of achieving the result than pivoting or an aggregate case when statements could achieve.

*/
