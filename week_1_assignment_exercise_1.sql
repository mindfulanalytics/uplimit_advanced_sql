-- Advanced SQL Week 1 Assignment
-- Author: Diego Galan
-- Date: -4/27/2024 
-- Step 1: Eliminate duplicate cities 
-- Step 2: Get customer info and geo location. How many customers can place an order with us?
-- Step 3: Get supplier info and geo location
-- Step 4: Identify the shortest customer-to-supplier shipping distance using cross join and qualify on row_number
-- Step 5: Final report order by last name and first name asc


-- Step 1: Eliminate duplicate cities
-- Total_rows in us_cities = 30,409
-- Total_rows after removing duplicates = 30,351

with us_cities as (

select 
    city_id,
    upper(trim(city_name)) as city_name,
    upper(trim(state_name)) as city_state_name,
    upper(trim(state_abbr)) as city_state_abbr,
    geo_location as city_geo_location
from vk_data.resources.us_cities
where 1=1 
    -- and city_name like 'GEORGETOWN'
    -- and state_abbr like 'PA'
qualify row_number() over (partition by upper(trim(city_name)), upper(trim(state_abbr)) order by upper(trim(city_name)) asc) = 1 

),

-- Step 2: Get customer info and geo location 
-- There are 10K customers
-- There are 2,401 customer for whom we have city geo location for

customer_data as (

select 
    customer_id,
    first_name as customer_first_name,
    last_name as customer_last_name,
    email as customer_email
from vk_data.customers.customer_data

),

customer_address as (

select 
    address_id,
    customer_id,
    upper(trim(customer_city)) as customer_city,
    upper(trim(customer_state)) as customer_state_abbr
from vk_data.customers.customer_address

),

customer_city_geo_location as (
 
select 
    cd.customer_id,
    customer_first_name,
    customer_last_name,
    customer_email,
    ca.customer_city,
    ca.customer_state_abbr,
    city_geo_location
from customer_data cd 
left join customer_address ca 
    on cd.customer_id = ca.customer_id
inner join us_cities uc 
    on ca.customer_city = uc.city_name
    and ca.customer_state_abbr = uc.city_state_abbr

),

-- select count(*) from customer_city_geo_location

-- Step 3: Get supplier info and geo location

supplier_info as (

select 
    supplier_id,
    supplier_name,
    upper(trim(supplier_city)) as supplier_city_name,
    upper(trim(supplier_state)) as supplier_state_abbr
from vk_data.suppliers.supplier_info

),

supplier_geo_location as (

select 
    supplier_id,
    supplier_name,
    supplier_city_name,
    supplier_state_abbr,
    city_geo_location as supplier_city_geo_location
from supplier_info si 
left join us_cities uc 
    on si.supplier_city_name = uc.city_name
    and si.supplier_state_abbr = uc.city_state_abbr

),

-- Step 4: Identify the shortest customer-to-supplier shipping distance

city_to_supplier_cross_join as (

select 
    city_id,
    supplier_id,
    city_name,
    city_state_abbr,
    supplier_name,
    supplier_city_name,
    supplier_state_abbr,
    round(st_distance(uc.city_geo_location, sgl.supplier_city_geo_location) / 1609,1) as distance_to_supplier_miles
from us_cities uc 
cross join supplier_geo_location sgl 

),

city_to_supplier as (

select 
    city_id,
    supplier_id,
    city_name,
    city_state_abbr,
    supplier_name,
    supplier_city_name,
    supplier_state_abbr,
    distance_to_supplier_miles
from city_to_supplier_cross_join
qualify row_number() over (partition by city_id order by distance_to_supplier_miles asc) = 1

)

-- Step 5: Final report

select
    customer_id,
    customer_first_name,
    customer_last_name,
    customer_email,
    supplier_id,
    supplier_name,
    distance_to_supplier_miles
from customer_city_geo_location cgl
left join city_to_supplier cts 
    on cgl.customer_city = cts.city_name
    and cgl.customer_state_abbr = cts.city_state_abbr
order by customer_last_name asc, customer_first_name asc
