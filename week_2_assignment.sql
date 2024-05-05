-- Advanced SQL Week 2 Assignment
-- Author: Diego Galan
-- Date: 05/05/2024 
-- Step 1: Confirm current output and logic, 19 rows total. Shouldn't it be 25+ according to the prompt?
-- Step 2: Convert subqueries into CTEs
-- Step 3: Fix style errors like spacing, indentation, aliases, CTE naming, other?
-- Step 4: Make inner join to active_customers into a left join, even if customers have not submited their food preference with a survey, 
    -- we still want to identify and send them some fresh parsley. Output row count changes from 19 to 31, over the 25 we had identified so far as impacted.
-- Step 5: 

with customers_with_preferences as (

    select 
            customer_id,
            count(*) as food_pref_count
    from vk_data.customers.customer_survey
    where is_active = true
    group by 1

),

chic as (

    select 
        geo_location
    from vk_data.resources.us_cities 
    where city_name = 'CHICAGO' 
        and state_abbr = 'IL'


),

gary as (

    select 
        geo_location
    from vk_data.resources.us_cities 
    where city_name = 'GARY' 
        and state_abbr = 'IN'


)

select 
    c.first_name || ' ' || c.last_name as customer_name,
    ca.customer_city,
    ca.customer_state,
    s.food_pref_count,
    (st_distance(us.geo_location, chic.geo_location) / 1609)::int as chicago_distance_miles,
    (st_distance(us.geo_location, gary.geo_location) / 1609)::int as gary_distance_miles
from vk_data.customers.customer_address as ca
inner join vk_data.customers.customer_data as c 
    on ca.customer_id = c.customer_id
left join vk_data.resources.us_cities as us 
    on lower(trim(ca.customer_state)) = lower(trim(us.state_abbr))
    and lower(trim(ca.customer_city)) = lower(trim(us.city_name))
left join customers_with_preferences as s 
    on c.customer_id = s.customer_id
cross join chic
cross join gary
where 1=1 
    and ( ca.customer_state = 'KY'
        and (trim(us.city_name) ilike '%concord%' 
            or trim(us.city_name) ilike '%georgetown%' 
            or trim(us.city_name) ilike '%ashland%'))
    or( ca.customer_state = 'CA' 
        and (trim(us.city_name) ilike '%oakland%' 
            or trim(us.city_name) ilike '%pleasant hill%'))
    or (ca.customer_state = 'TX' 
        and (trim(us.city_name) ilike '%arlington%' 
            or trim(us.city_name) ilike '%brownsville%'))
