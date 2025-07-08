{{ config(
    materialized = 'view'
) }}

with stg as (
  select *
  from {{ ref('stg_world_chess_championship_matches_1866_2021') }}
)

select
  stg.*,

  -- white player name split
  trim(split_part(stg.white_player, ',', 1))             as white_last_name,
  rtrim(trim(split_part(stg.white_player, ',', 2)), '.') as white_first_name,

  -- black player name split
  trim(split_part(stg.black_player, ',', 1))             as black_last_name,
  rtrim(trim(split_part(stg.black_player, ',', 2)), '.') as black_first_name

from stg
