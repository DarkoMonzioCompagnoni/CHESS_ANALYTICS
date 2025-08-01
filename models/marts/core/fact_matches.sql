{{ config(materialized='table') }}

select
  m.game_id               as match_id,
  m.date_played           as date_key,
  wp.player_id           as white_player_id,
  bp.player_id           as black_player_id,
  m.result,
  m.eco_code              as opening_key,
  m.elo_diff
from {{ ref('int_world_chess_championship_matches') }} as m

left join {{ ref('dim_player') }} as wp
  on m.white_first_name = wp.first_name
 and m.white_last_name  = wp.last_name

left join {{ ref('dim_player') }} as bp
  on m.black_first_name = bp.first_name
 and m.black_last_name  = bp.last_name