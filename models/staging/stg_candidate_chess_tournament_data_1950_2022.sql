with raw as (
  select
    VARIANT_COL:Black::string as black_player,
    VARIANT_COL:White::string as white_player,
    to_date(VARIANT_COL:Date::string, 'YYYY.MM.DD') as game_date,
    VARIANT_COL:ECO::string as eco_code,
    VARIANT_COL:Event::string as event,
    VARIANT_COL:"Event w/Year"::string as event_with_year,
    VARIANT_COL:Full_Opening_Name::string as full_opening_name,
    VARIANT_COL:Short_Opening_Name::string as short_opening_name,
    VARIANT_COL:Opening_Family::string as opening_family,
    VARIANT_COL:Moves::string as moves_json_string,
    VARIANT_COL:Result::string as result,
    VARIANT_COL:Round::number as round,
    VARIANT_COL:Site::string as site,
    VARIANT_COL:Year::number as year
  from {{ source('raw', 'CANDIDATE_CHESS_TOURNAMENT_DATA_1950_2022') }}
)

select *
from raw
