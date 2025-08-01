{{ config(
    materialized = 'view'
) }}

with stg as (
  select *
  from {{ ref('stg_candidate_chess_tournament_data_1950_2022') }}
)

select
  -- all your cleaned staging columns
  stg.black_player,
  stg.white_player,
  stg.game_date,
  stg.eco_code,
  stg.event,
  stg.full_opening_name,
  stg.short_opening_name,
  stg.opening_family,
  stg.moves_json_string,
  stg.result,
  stg.round,
  stg.site,
  regexp_substr(stg.site, '\\S+$') as country_code,
  stg.year,
  -- normalize whitespace, then pull first & last tokens
  regexp_substr(regexp_replace(stg.white_player, '\\s+', ' '),
                '^[^ ]+')                           as white_first_name,
  regexp_substr(regexp_replace(stg.white_player, '\\s+', ' '),
                '[^ ]+$')                           as white_last_name,

  regexp_substr(regexp_replace(stg.black_player, '\\s+', ' '),
                '^[^ ]+')                           as black_first_name,
  regexp_substr(regexp_replace(stg.black_player, '\\s+', ' '),
                '[^ ]+$')                           as black_last_name

from stg