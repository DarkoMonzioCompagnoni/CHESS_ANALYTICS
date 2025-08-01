{{ config(materialized='table') }}

with bounds as (
  select
    min(date_played) as start_date,
    max(date_played) as end_date
  from {{ ref('int_world_chess_championship_matches') }}
),

-- generate a lot of integers at compile time
numbers as (
  select
    seq4() as day_offset
  from table(generator(rowcount => 100000))  -- covers ~273 years; adjust if needed
),

-- only keep offsets up to the real span
date_spine as (
  select
    dateadd(day, day_offset, start_date) as date
  from bounds
  join numbers
    on day_offset <= datediff(day, start_date, end_date)
)

select
  date                              as full_date,
  extract(year  from date)          as year,
  extract(month from date)          as month,
  extract(day   from date)          as day,
  dayofweek(date)                   as day_of_week,   -- 1=Sunday…7=Saturday
  extract(quarter from date)        as quarter
from date_spine
order by date
