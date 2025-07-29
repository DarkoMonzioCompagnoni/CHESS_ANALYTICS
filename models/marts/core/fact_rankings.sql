{{ config(materialized='table') }}

select
  md5(lower(concat(first_name, ' ', last_name))) as player_key,
  ranking_date          as date_key,
  world_rank,
  elo_rating,
  fide_title
from {{ ref('int_top_100_chess_players_historical') }}
