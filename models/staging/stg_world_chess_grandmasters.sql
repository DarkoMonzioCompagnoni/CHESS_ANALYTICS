with raw as (
  select
    try_cast(nullif(trim(VARIANT_COL:"FIDE ID"::string), '') as number) as fide_id,
    VARIANT_COL:Name::string as full_name,
    VARIANT_COL:Sex::string as gender,
    to_date(VARIANT_COL:Born::string) as birth_date,
    nullif(VARIANT_COL:Died::string, '')::date as death_date,
    VARIANT_COL:Birthplace::string as birthplace,
    VARIANT_COL:Federation::string as federation,
    VARIANT_COL:"Title Year"::number as title_year,
    VARIANT_COL:Notes::string as notes
  from {{ source('raw', 'WORLD_CHESS_GRANDMASTERS_DATA') }}
)

select *
from raw
where fide_id is not null
  and full_name is not null


