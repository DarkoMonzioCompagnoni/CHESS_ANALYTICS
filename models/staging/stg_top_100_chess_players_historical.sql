with raw as (
  select
    data:name::string as player_name,
    data:country::string as country_code,
    data:birth_year::number as birth_year,
    data:games::number as games_played,
    data:rank::number as world_rank,
    to_date(data:ranking_date::string, 'DD-MM-YY') as ranking_date,
    data:rating::number as elo_rating,
    data:title::string as fide_title
  from {{ source('raw', 'TOP_100_CHESS_PLAYERS_HISTORICAL') }}
)

select *
from raw
