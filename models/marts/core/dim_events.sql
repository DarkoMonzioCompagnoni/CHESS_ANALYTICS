{{ config(
    materialized = 'table'
) }}

with candidate_events as (
  select
    stg.event,
    stg.site,
    stg.country_code,
    stg.year,
    'candidates' as source
  from {{ ref('int_candidate_chess_tournament_data_1950_2022') }} stg
),

championship_events as (
  select
    stg.event,
    stg.site,
    stg.country_code,
    extract(year from stg.date_played) as year,
    'championships' as source
  from {{ ref('int_world_chess_championship_matches_1866_2021') }} stg
),

combined as (
  select * from candidate_events
  union all
  select * from championship_events
),

deduplicated as (
  select
    *,
    md5(lower(event) || '-' || coalesce(cast(year as string), '') || '-' || lower(site)) as event_id,
    row_number() over (
      partition by md5(lower(event) || '-' || coalesce(cast(year as string), '') || '-' || lower(site))
      order by source desc
    ) as row_num
  from combined
)

select
  event_id,
  event,
  year,
  site,
  country_code,
  source
from deduplicated
where row_num = 1
