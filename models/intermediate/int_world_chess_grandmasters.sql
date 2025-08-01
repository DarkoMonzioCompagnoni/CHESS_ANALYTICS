{{ config(
    materialized = 'view'
) }}

with stg as (
  select *
  from {{ ref('stg_world_chess_grandmasters') }}
)

select 
  -- all staging columns
  stg.fide_id,
  stg.full_name,
  -- extract first_name
  case
    when stg.full_name like '%,%' then
      trim(split_part(stg.full_name, ',', 2))
    else
      rtrim(trim(split_part(stg.full_name, ' ', 2)), '.')
  end as first_name,
  -- extract first_name_initial
  case
    when stg.full_name like '%,%' then
      left(trim(split_part(stg.full_name, ',', 2)), 1)
    else
      left(rtrim(trim(split_part(stg.full_name, ' ', 2)), '.'), 1)
  end as first_name_initial,
  -- extract last_name
  case
    when stg.full_name like '%,%' then
      trim(split_part(stg.full_name, ',', 1))
    else
      split_part(stg.full_name, ' ', 1)
  end as last_name,
  stg.gender,
  stg.birth_date,
  stg.death_date,
  stg.birthplace,
  stg.federation,
  codes.country_code,
  stg.title_year,
  stg.notes
from stg
left join {{ ref('country_codes') }} codes
  on lower(stg.federation) = lower(codes.country_name)