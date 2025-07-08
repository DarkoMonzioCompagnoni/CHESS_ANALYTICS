{{ config(
    materialized = 'view'
) }}

with stg as (
  select *
  from {{ ref('stg_top_100_chess_players_historical') }}
)

select
  -- all staging columns
  stg.*,

  -- split player_name
  case
    when stg.player_name like '%,%' then
      trim(split_part(stg.player_name, ',', 2))      -- After comma
    when stg.player_name like '% %' then
      split_part(stg.player_name, ' ', 1)            -- First token
    else
      stg.player_name                               -- fallback
  end as first_name,

  case
    when stg.player_name like '%,%' then
      trim(split_part(stg.player_name, ',', 1))      -- Before comma
    when stg.player_name like '% %' then
      split_part(stg.player_name, ' ', -1)           -- Last token
    else
      null                                           -- fallback
  end as last_name

from stg