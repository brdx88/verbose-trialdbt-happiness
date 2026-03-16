SELECT
    ROW_NUMBER() OVER (ORDER BY filename) AS match_id,
    filename AS match_file,
    Wk AS week,
    UPPER(Day) AS day,
    `date` as match_date,
    Time AS match_time,
    UPPER(Home) AS home_team_name,
    UPPER(Away) AS away_team_name,
    UPPER(Venue) AS venue,
    UPPER(Referee) AS referee,
    Attendance
FROM {{ source('banking_bronze', 'fbref_scores_fixtures') }}