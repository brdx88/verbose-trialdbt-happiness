SELECT
    m.match_id,
    t_home.team_id AS home_team_id,
    -- t_home.team_name AS home_team_name,
    t_away.team_id AS away_team_id,
    -- t_away.team_name as away_team_name,
    a.xG AS home_xg,
    a.xG_1 AS away_xg,
    CAST(SPLIT(a.Score, '-')[OFFSET(0)] AS INT64) AS home_score,
    CAST(SPLIT(a.Score, '-')[OFFSET(1)] AS INT64) AS away_score,
    a.Attendance,
    UPPER(a.Venue) AS Venue,
    UPPER(a.Referee) AS Referee
FROM {{ source('banking_bronze', 'fbref_scores_fixtures') }} a
JOIN {{ ref('fbref_dim_match') }} m
    ON UPPER(a.filename) = UPPER(m.match_file)
LEFT JOIN {{ ref('fbref_dim_team') }} t_home
    ON UPPER(a.Home) = UPPER(t_home.team_name)
LEFT JOIN {{ ref('fbref_dim_team') }} t_away
    ON UPPER(a.Away) = UPPER(t_away.team_name)
