{{ config(materialized='table') }}

with gm as (
  select
    cast(fide_id as varchar)                as player_key,
    first_name,
    last_name,
    gender,
    birth_date,
    death_date,
    birthplace,
    federation,
    title_year,
    notes
  from {{ ref('int_world_chess_grandmasters') }}
),

top100 as (
  select
    md5(lower(concat(first_name, ' ', last_name))) as player_key,
    first_name,
    last_name,
    null           as gender,
    date_from_parts(birth_year, 1, 1)  as birth_date,
    null           as death_date,
    null           as birthplace,
    null           as federation,
    null           as title_year,
    null           as notes
  from {{ ref('int_top_100_chess_players_historical') }}
)

select distinct
  coalesce(gm.player_key, top100.player_key) as player_key,
  coalesce(gm.first_name, top100.first_name)  as first_name,
  coalesce(gm.last_name,  top100.last_name)   as last_name,
  coalesce(gm.gender,     top100.gender)      as gender,
  coalesce(gm.birth_date, top100.birth_date)  as birth_date,
  coalesce(gm.death_date, top100.death_date)  as death_date,
  coalesce(gm.birthplace, top100.birthplace)  as birthplace,
  coalesce(gm.federation, top100.federation)  as federation,
  coalesce(gm.title_year, top100.title_year)  as title_year,
  coalesce(gm.notes,      top100.notes)       as notes
from gm
full outer join top100
  on gm.first_name = top100.first_name
 and gm.last_name  = top100.last_name