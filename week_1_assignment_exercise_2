-- Advanced SQL Week 1 Assignment
-- Author: Diego Galan
-- Date: 4/27/2024 
-- Step 1: Pull eligible customers with at least one food preference in survey table 
-- Step 2: Use the pivot function to pull the first three pivot preferences as columns
-- Step 3: Flatten tags on recipes and match one to food preference #1
-- Step 4: Final Results ordered by email asc

-- Step 1: Pull eligible customers with at least one food preference

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

customer_surveys as (

select 
    customer_id,
    tag_id
from vk_data.customers.customer_survey
where is_active = TRUE

),

recipe_tags as (

select 
    tag_id,
    lower(trim(tag_property)) as tag_property
from vk_data.resources.recipe_tags

),

customer_tags as (

select 
    customer_id,
    rt.tag_property,
    row_number() over (partition by customer_id order by rt.tag_property asc ) as customer_tag_property_asc_row_id
from customer_surveys cs 
left join recipe_tags rt 
    on cs.tag_id = rt.tag_id

),
-- Step 2: Use the pivot function to pull the first three pivot preferences as columns

pivoted_customer_tags as (

select 
    *
from customer_tags 
pivot(min(tag_property) for customer_tag_property_asc_row_id in(1, 2, 3))
    as p(customer_id, tag_1, tag_2, tag_3)

),

-- Step 3: Flatten tags on recipes and match one to food preference #1

recipe_tags_flat as (

select 
    recipe_id,
    recipe_name,
    lower(trim(replace(flat_tag.value, '"', ''))) as recipe_tag
from vk_data.chefs.recipe
, table(flatten(tag_list)) as flat_tag

),

tag_to_recipe as (

select 
    recipe_tag,
    min(recipe_name) as suggested_recipe
from recipe_tags_flat 
group by 1

)

-- Step 4: Final Results ordered by email asc

select 
    c.customer_id,
    customer_email,
    customer_first_name,
    tag_1 as customer_food_preference_1,
    tag_2 as customer_food_preference_2,
    tag_3 as customer_food_preference_3,
    suggested_recipe
from customer_city_geo_location c 
inner join pivoted_customer_tags p 
    on c.customer_id = p.customer_id
left join tag_to_recipe t 
    on p.tag_1 = t.recipe_tag
order by customer_email asc 
