{{ config(
    materialized = 'view'
) }}

with stg as (
  select *
  from {{ ref('stg_candidate_chess_tournament_data_1950_2022') }}
)

select
  -- all your cleaned staging columns
  stg.*,

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