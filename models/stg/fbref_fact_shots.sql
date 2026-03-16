SELECT
    GENERATE_UUID() AS shot_id,

    m.match_id,
    p.player_id,
    t.team_id,

    minute,
    xg,
    psxg,
    UPPER(outcome) as outcome,
    distance,
    UPPER(body_part) as body_part,
    UPPER(notes) as notes,

    -- SCA players
    p1.player_id AS sca1_player_id,
    sca_1_event AS sca1_event,
    p2.player_id AS sca2_player_id,
    sca_2_event AS sca2_event

FROM {{ source('banking_bronze', 'fbref_matches_all_shots') }} s
LEFT JOIN {{ ref('fbref_dim_match') }} m
    ON s.match_file = m.match_file
LEFT JOIN {{ ref('fbref_dim_player') }} p
    ON UPPER(s.player) = p.player_name
LEFT JOIN {{ ref('fbref_dim_team') }} t
    ON UPPER(s.squad) = t.team_name
LEFT JOIN {{ ref('fbref_dim_player') }} p1
    ON UPPER(s.sca_1_player) = p1.player_name
LEFT JOIN {{ ref('fbref_dim_player') }} p2
    ON UPPER(s.sca_2_player) = p2.player_name