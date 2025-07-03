with raw as (
  select
    data:"FIDE ID"::number as fide_id,
    data:Name::string as full_name,
    data:Sex::string as gender,
    to_date(data:Born::string) as birth_date,
    nullif(data:Died::string, '')::date as death_date,
    data:Birthplace::string as birthplace,
    data:Federation::string as federation,
    data:"Title Year"::number as title_year,
    data:Notes::string as notes
  from {{ source('raw', 'WORLD_CHESS_GRANDMASTERS_DATA') }}
)

select *
from raw
where fide_id is not null
  and full_name is not null


