{{ config(
    materialized = 'view'
) }}

with stg as (
  select *
  from {{ ref('stg_world_chess_grandmasters') }}
)

select
  -- all staging columns
  stg.*,

  -- split full_name of the form "Last, First" or "Last I."
  case
    when stg.full_name like '%,%' then
      trim(split_part(stg.full_name, ',', 2))
    else
      rtrim(trim(split_part(stg.full_name, ' ', 2)), '.')
  end as first_name,

  case
    when stg.full_name like '%,%' then
      trim(split_part(stg.full_name, ',', 1))
    else
      split_part(stg.full_name, ' ', 1)
  end as last_name

from stg