{{ config(
    materialized = 'table'
) }}

with top_players as (
  select 
    player_name,
    first_name,
    left(first_name, 1) as first_name_initial,
    last_name,
    country_code,
    birth_year,
    cast(null as date) as birth_date,
    null as death_date,
    null as gender,
    null as fide_id,
    null as title_year,
    null as federation,
    null as birthplace,
    'top_100' as source
  from {{ ref('int_top_100_chess_players_historical') }}
),

grandmasters as (
  select
    full_name as player_name,
    first_name,
    left(first_name, 1) as first_name_initial,
    last_name,
    codes.country_code,
    extract(year from birth_date) as birth_year,
    birth_date,
    death_date,
    gender,
    fide_id,
    title_year,
    federation,
    birthplace,
    'grandmasters' as source
  from {{ ref('int_world_chess_grandmasters') }} stg
  left join {{ ref('country_codes') }} codes
    on lower(stg.federation) = lower(codes.country_name)
),

combined as (
  select * from top_players
  union all
  select * from grandmasters
),

deduplicated as (
  select
    *,
    md5(coalesce(lower(last_name), '') || '-' || coalesce(lower(first_name_initial), '') || '-' || coalesce(cast(birth_year as string), '')) as player_id,
    row_number() over (
      partition by 
        md5(coalesce(lower(last_name), '') || '-' || coalesce(lower(first_name_initial), '') || '-' || coalesce(cast(birth_year as string), '')) 
      order by 
        source desc, -- prioritize 'grandmasters' over 'top_100'
        birth_date desc nulls last
    ) as row_num
  from combined
)

select
  player_id,
  player_name,
  first_name,
  first_name_initial,
  last_name,
  country_code,
  birth_year,
  birth_date,
  death_date,
  gender,
  fide_id,
  title_year,
  federation,
  birthplace,
  source
from deduplicated
where row_num = 1
