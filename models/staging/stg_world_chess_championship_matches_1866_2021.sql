with raw as (
  select
    VARIANT_COL:game_id::string as game_id,
    VARIANT_COL:event::string as event,
    VARIANT_COL:round::number as round,
    VARIANT_COL:game_order::number as game_order,
    VARIANT_COL:site::string as site,
    to_date(VARIANT_COL:date_played::string, 'YYYY.MM.DD') as date_played,
    to_timestamp_ntz(VARIANT_COL:date_created::string, 'YYYY-MM-DD"T"HH24:MI:SS+0000') as date_created,
    
    VARIANT_COL:white::string as white_player,
    try_cast(nullif(trim(VARIANT_COL:white_elo::string), '') as number) as white_elo,
    VARIANT_COL:white_title::string as white_title,

    VARIANT_COL:black::string as black_player,
    try_cast(nullif(trim(VARIANT_COL:black_elo::string), '') as number) as black_elo,
    VARIANT_COL:black_title::string as black_title,

    VARIANT_COL:result::string as result,
    VARIANT_COL:winner::string as winner,
    VARIANT_COL:loser::string as loser,
    try_cast(nullif(trim(VARIANT_COL:winner_elo::string), '') as number) as winner_elo,
    try_cast(nullif(trim(VARIANT_COL:loser_elo::string), '') as number) as loser_elo,
    try_cast(nullif(trim(VARIANT_COL:winner_loser_elo_diff::string), '') as number) as elo_diff,

    VARIANT_COL:eco::string as eco_code,
    VARIANT_COL:file_name::string as pgn_file_name
  from {{ source('raw', 'WORLD_CHESS_CHAMPIONSHIP_MATCHES_1866_2021') }}
)

select *
from raw
where game_id is not null
  and date_played is not null
  and white_player is not null
  and black_player is not null
  and result is not null