WITH dem AS 
(
    SELECT
        UPPER(TRIM(name)) AS player_name,
        UPPER(REGEXP_EXTRACT(footed, r'▪\s*(.*)')) as footed,
        UPPER(REGEXP_EXTRACT(position, r'^(.*?)\s*▪')) as position,
        CAST(REGEXP_EXTRACT(height_cm, r'(\d+)') AS FLOAT64) AS height_cm,
        CAST(REGEXP_EXTRACT(weight_kg, r'(\d+)') AS FLOAT64) AS weight_kg,
        CAST(REGEXP_EXTRACT(age, r'^(.*?)-') AS INT) as age,
        UPPER(national_team) AS nationality,
        UPPER(club) AS CLUB,
        birthplace,
        html_file AS source_file
    FROM {{ source('banking_bronze', 'fbref_player_demographics') }}
),
stats AS 
(
    SELECT DISTINCT
        UPPER(TRIM(player)) AS player_name,
        UPPER(pos) AS position,
        UPPER(squad) AS club
    FROM {{ source('banking_bronze', 'fbref_player_stats') }}
)
SELECT
    GENERATE_UUID() AS player_id,
    COALESCE(dem.player_name, stats.player_name) AS player_name,
    COALESCE(dem.position, stats.position) AS position,
    dem.footed,
    dem.height_cm,
    dem.weight_kg,
    dem.age,
    dem.nationality,
    COALESCE(dem.club, stats.club) AS club,
    dem.source_file
FROM stats
LEFT JOIN dem
    ON stats.player_name = dem.player_name