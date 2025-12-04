SELECT DISTINCT 
    UPPER(team_name) as team_name,
    ROW_NUMBER() OVER (ORDER BY team_name) AS team_id
FROM (
    SELECT Home AS team_name FROM {{ source('banking_bronze', 'fbref_scores_fixtures') }}
    UNION DISTINCT
    SELECT Away FROM {{ source('banking_bronze', 'fbref_scores_fixtures') }}
    UNION DISTINCT
    SELECT squad FROM {{ source('banking_bronze', 'fbref_player_stats') }}
    
)
WHERE team_name IS NOT NULL