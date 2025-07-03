with raw as (
  select
    data:Black::string as black_player,
    data:White::string as white_player,
    data:Date::date as game_date,
    data:ECO::string as eco_code,
    data:Event::string as event,
    data:"Event w/Year"::string as event_with_year,
    data:Full_Opening_Name::string as full_opening_name,
    data:Short_Opening_Name::string as short_opening_name,
    data:Opening_Family::string as opening_family,
    data:Moves::string as moves_json_string,
    data:Result::string as result,
    data:Round::number as round,
    data:Site::string as site,
    data:Year::number as year
  from {{ source('raw', 'CANDIDATE_CHESS_TOURNAMENT_DATA_1950_2022') }}
)

select *
from raw
