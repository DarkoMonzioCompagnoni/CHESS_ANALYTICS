{{ config(
    materialized = 'view'
) }}

with stg as (
  select *
  from {{ ref('stg_candidate_chess_tournament_data_1950_2022') }}
),

country_codes_lkp as (
    select
        country_name,
        country_code
    from {{ ref('country_codes') }}
),

-- CTE to extract potential country identifiers
parsed_site_data as (
  select
    stg.*,
    -- Attempt to extract a 3-letter code at the end (e.g., 'NED')
    regexp_substr(stg.site, '\\s([A-Z]{3})$') as last_three_letter_code,
    -- Attempt to extract text within parentheses at the end (e.g., '(Netherlands)')
    regexp_substr(stg.site, '\\(([^)]+)\\)$', 1, 1, 'e', 1) as country_name_in_parentheses
  from stg
)

select
  -- all your cleaned staging columns
  psd.black_player,
  psd.white_player,
  psd.game_date,
  psd.eco_code,
  psd.event,
  psd.full_opening_name,
  psd.short_opening_name,
  psd.opening_family,
  psd.moves_json_string,
  psd.result,
  psd.round,
  psd.site,
  CASE
    -- Case 1: Site ends with a 3-letter code (e.g., "Amsterdam NED")
    WHEN psd.last_three_letter_code IS NOT NULL THEN TRIM(psd.last_three_letter_code)
    -- Case 2: Site ends with a country name in parentheses (e.g., "Amsterdam (Netherlands)")
    WHEN psd.country_name_in_parentheses IS NOT NULL THEN country_lkp.country_code
    -- Default case
    ELSE 'UNK'
  END AS country_code,
  psd.year,
  -- normalize whitespace, then pull first & last tokens
  regexp_substr(regexp_replace(psd.white_player, '\\s+', ' '),
                '^[^ ]+')                           as white_first_name,
  regexp_substr(regexp_replace(psd.white_player, '\\s+', ' '),
                '[^ ]+$')                           as white_last_name,

  regexp_substr(regexp_replace(psd.black_player, '\\s+', ' '),
                '^[^ ]+')                           as black_first_name,
  regexp_substr(regexp_replace(psd.black_player, '\\s+', ' '),
                '[^ ]+$')                           as black_last_name

from parsed_site_data as psd
LEFT JOIN country_codes_lkp as country_lkp
  ON country_lkp.country_name = psd.country_name_in_parentheses