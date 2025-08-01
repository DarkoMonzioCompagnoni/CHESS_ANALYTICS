{{ config(
    materialized = 'view'
) }}

with stg as (
  select *
  from {{ ref('stg_world_chess_championship_matches_1866_2021') }}
)

select
  stg.game_id,
  stg.event,
  stg.round,
  stg.game_order,
  stg.site,
  regexp_substr(stg.site, '\\S+$') as country_code,
  stg.date_played,
  stg.white_player,
  stg.white_elo,
  stg.black_player,
  stg.black_elo,
  stg.result,
  CASE 
    WHEN stg.result = '1-0' THEN 'White'
    WHEN stg.result = '0-1' THEN 'Black'
    ELSE 'Draw' END as winner_color,
    CASE 
    WHEN stg.result = '0-1' THEN 'White'
    WHEN stg.result = '1-0' THEN 'Black'
    ELSE 'Draw' END as loser_color,
  stg.winner as winner_player,
  stg.loser as loser_player,
  stg.winner_elo,
  stg.loser_elo,
  stg.elo_diff,
  stg.eco_code,
-- white player name split
trim(split_part(stg.white_player, ',', 1))             as white_last_name,
rtrim(trim(split_part(stg.white_player, ',', 2)), '.') as white_first_name,
left(rtrim(trim(split_part(stg.white_player, ',', 2)), '.'), 1) as white_first_name_initial,
-- black player name split
trim(split_part(stg.black_player, ',', 1))             as black_last_name,
rtrim(trim(split_part(stg.black_player, ',', 2)), '.') as black_first_name,
left(rtrim(trim(split_part(stg.black_player, ',', 2)), '.'), 1) as black_first_name_initial
from stg