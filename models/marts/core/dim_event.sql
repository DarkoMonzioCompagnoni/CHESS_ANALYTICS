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
    -- extract the raw country text
    case
      when site rlike '\\([^)]*\\)$' then
        regexp_substr(site, '\\(([^)]*)\\)', 1, 1, 'e', 1)
      else
        regexp_substr(site, '\\S+$')
    end as raw_country_name,

    -- extract the city as before
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
  coalesce(cc.country_code, 'UNK') as country_code,
  year

from exploded e

left join {{ ref('country_codes') }} cc
  on lower(e.raw_country_name) = lower(cc.country_name)

order by event_name
