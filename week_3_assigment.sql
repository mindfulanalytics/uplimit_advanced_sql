-- Advanced SQL Week 3 Assignment
-- Author: Diego Galan
-- Date: 05/11/2024 
-- Ask: Create a daily report to track:
    -- Total unique sessions
    -- The average length of sessions in seconds
    -- The average number of searches completed before displaying a recipe 
    -- The ID of the recipe that was most viewed 

-- Step 1: Select the fields necessary from the vk_data.events.website_activity table, a select * is expensive, use json_parse
-- Step 2: Calculate the average session length in seconds group by date using avg() on the first and last timestamp in a session using max() and min() datediff
-- Step 4: Calculate the average number of searches completed before displaying a recipe, group by session
-- Step 5: Calculate the ID of the recipe that was most viewed using max() on count(recipe_id), group by date
-- Step 6: Get the final report on the following format, group by date

-- | session_date | total_unique_sessions | avg_session_length_in_sec | avg_searches_per_recipe_view | most_viewed_recipe_id |
-- |--------------|-----------------------|---------------------------|------------------------------|-----------------------|
-- | 2024-05-11   | 10                    | 60                        | 5                            | abcd                  |
-- | 2024-05-09   | 20                    | 120                       | 3                            | efgh                  |
-- | 2024-05-08   | 5                     | 180                       | 2                            | abcd                  |
-- | 2024-05-07   | 15                    | 60                        | 1                            | ijkl                  |
-- | 2024-05-06   | 10                    | 240                       | 3                            | mnop                  |


-- Q: Looking at the query optimizer, how would we improve the query? 
-- A: The query optimizer shows the inner join of our final CTE as the most expensive node in the query profile. If we converted the min_event_timestamp field into a date_field in the first cte, this might improve the query performance. After giving that a try, there is not longer a most expensive node called out.


-- Be sure to use the below command so the cached results are not used and you can see the full query profile  cache 
-- alter session set use_cached_result = false

with web_activity as (
-- There are 238 rows, but the event_details field is a an array field we will need flatten
-- There are nine distinct types of event_details, each with an event_name, page_name, and if it's a recipe, the applicable recipe_id

-- select distinct event_details from vk_data.events.website_activity


select 
    event_id,
    session_id,
    user_id,
    event_timestamp,
    date(event_timestamp) as event_date,
    event_details,
    trim(parse_json(event_details):"event", '*') as event_type,
    trim(parse_json(event_details):"page", '*') as page_type,
    trim(parse_json(event_details):"recipe_id", '*') as recipe_id
from vk_data.events.website_activity
-- , table(flatten(try_parse_json(event_details))) as flat_event_details

),

session_agg as (

select 
    session_id,
    min(event_timestamp) as min_event_timestamp,
    max(event_timestamp) as max_event_timestamp,
    min(event_date) as min_event_date,
    case 
        when count_if(event_type = 'view_recipe') = 0
            then null
        else round(count_if(event_type = 'search') / count_if( event_type = 'view_recipe'))
    end as searches_per_recipe_view
from web_activity
group by 1

),

most_viewed_recipe_per_day as (

select 
    event_date,
    recipe_id,
    count(*) as total_views
from web_activity
where 1=1 
    and recipe_id is not null
group by 1, 2
qualify row_number() over(partition by event_date order by total_views desc ) = 1

)

select 
    min_event_date as session_date,
    count(session_id) as total_unique_sessions,
    round(avg(datediff('sec',min_event_timestamp, max_event_timestamp))) as avg_session_length_in_sec,
    round(avg(searches_per_recipe_view)) as avg_searches_per_recipe_view,
    max(mvr.recipe_id) as most_viewed_recipe_id
from session_agg sa 
inner join most_viewed_recipe_per_day mvr 
    on sa.min_event_date = mvr.event_date
group by 1
order by 1 asc
