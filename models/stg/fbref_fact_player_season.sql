SELECT
    p.player_id,
    t.team_id,

    playing_time_mp AS playing_mp,
    playing_time_starts AS playing_starts,
    playing_time_min AS playing_minutes,
    playing_time_90s AS playing_90s,

    performance_gls AS gls,
    performance_ast AS ast,
    performance_ga AS ga,
    performance_g_pk AS g_pk,
    performance_pk AS pk,
    performance_pkatt AS pk_att,
    performance_crdy AS crdy,
    performance_crdr AS crdr,

    expected_xg AS xg,
    expected_npxg AS npxg,
    expected_xag AS xag,
    expected_npxgxag AS npxgxag,

    progression_prgc AS prog_c,
    progression_prgp AS prog_p,
    progression_prgr AS prog_r,

    per_90_minutes_gls AS per90_gls,
    per_90_minutes_ast AS per90_ast,
    per_90_minutes_ga AS per90_ga,
    per_90_minutes_npxg AS per90_npxg,
    per_90_minutes_xag AS per90_xag

FROM {{ source('banking_bronze','fbref_player_stats') }} s
LEFT JOIN {{ref('fbref_dim_player')}} p
    ON UPPER(s.player) = p.player_name
LEFT JOIN {{ref('fbref_dim_team')}} t
    ON UPPER(s.squad) = t.team_name