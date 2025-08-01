{{ config(materialized='table') }}

with champs as (
  select distinct
    event                         as event_name,
    event                         as event_key,
    site,
    extract(year from date_played) as year
  from {{ ref('int_world_chess_championship_matches') }}
),

cand as (
  select distinct
    event                   as event_name,
    event_with_year         as event_key,
    site,
    year
  from {{ ref('int_candidate_chess_tournament_data_1950_2022') }}
),

combined as (
  select
    coalesce(champs.event_name, cand.event_name) as event_name,
    coalesce(champs.event_key,  cand.event_key)  as event_key,
    coalesce(champs.site,       cand.site)       as site,
    coalesce(champs.year,       cand.year)       as year
  from champs
  full outer join cand using (event_name)
),

exploded as (
  select
    *,
    -- extract the last three characters as country_code
    right(site, 3) as country_code,

    -- extract the city by removing trailing ' (XXX)' or last word
    case
      when site rlike '\\s*\\([^)]*\\)$' then
        regexp_replace(site, '\\s*\\([^)]*\\)$', '')
      else
        regexp_replace(site, '\\s+\\S+$', '')
    end as city

  from combined
)

select
  event_name,
  event_key,
  site,
  city,
  country_code,
  year
from exploded
order by event_name