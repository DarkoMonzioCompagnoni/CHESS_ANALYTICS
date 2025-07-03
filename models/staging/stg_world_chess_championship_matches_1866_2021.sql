with raw as (
  select
    data:game_id::string as game_id,
    data:event::string as event,
    data:round::number as round,
    data:game_order::number as game_order,
    data:site::string as site,
    to_date(data:date_played::string, 'YYYY.MM.DD') as date_played,
    to_timestamp_tz(data:date_created::string) as date_created_utc,
    
    data:white::string as white_player,
    data:white_elo::number as white_elo,
    data:white_title::string as white_title,

    data:black::string as black_player,
    data:black_elo::number as black_elo,
    data:black_title::string as black_title,

    data:result::string as result,
    data:winner::string as winner,
    data:loser::string as loser,
    data:winner_elo::string as winner_elo,  -- empty when draw
    data:loser_elo::string as loser_elo,    -- empty when draw
    data:winner_loser_elo_diff::number as elo_diff,

    data:eco::string as eco_code,
    data:file_name::string as pgn_file_name
  from {{ source('raw', 'WORLD_CHESS_CHAMPIONSHIP_MATCHES_1866_2021') }}
)

select *
from raw
where game_id is not null
  and date_played is not null
  and white_player is not null
  and black_player is not null
  and result is not null