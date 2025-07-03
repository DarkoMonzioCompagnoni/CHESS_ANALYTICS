with raw as (
  select
    VARIANT_COL:name::string as player_name,
    VARIANT_COL:country::string as country_code,
    VARIANT_COL:birth_year::number as birth_year,
    VARIANT_COL:games::number as games_played,
    VARIANT_COL:rank::number as world_rank,
    to_date(VARIANT_COL:ranking_date::string, 'DD-MM-YY') as ranking_date,
    VARIANT_COL:rating::number as elo_rating,
    VARIANT_COL:title::string as fide_title
  from {{ source('raw', 'TOP_100_CHESS_PLAYERS_HISTORICAL') }}
)

select *
from raw
