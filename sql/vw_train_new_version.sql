SELECT 
	p.display_first_last player
	,ps.game_date
	,ps.season
	,ps.player_previous_game_date 
	,ps.days_rest
	,ps.team_abbreviation AS team
	,ps.team_loc AS loc
	,ps.opp_team_abbreviation AS opp_team
	,ps.opp_team_loc AS opp_loc
	,ps.age
	,p.nba_pos
    ,to_char(ps.game_date, 'MON') AS mnth
    ,to_char(ps.game_date, 'dy') AS dow
    ,gp.seconds AS seconds_played
    ,gp.ast * 1.5 + gp.reb * 1.2 + gp.pts + gp.stl * 2 + gp.blk * 2 - gp.tov AS fan_pts
    ,avg(gp.ast * 1.5 + gp.reb * 1.2 + gp.pts + gp.stl * 2 + gp.blk * 2 - gp.tov) OVER (PARTITION BY ps.player_id, ps.season) AS avg_fan_pts
    ,ft_opp.opp_efg_pct
    ,ft_opp.opp_fta_rate
    ,ft_opp.opp_oreb_pct
    ,ft_opp.opp_pts_2nd_chance
    ,ft_opp.opp_pts_fb
    ,ft_opp.opp_pts_off_tov
    ,ft_opp.opp_pts_paint
    ,ft_opp.opp_tov_pct
    ,ft_opp.plus_minus
    ,ft_opp.pie
    ,ft_opp.def_rating
    ,ft_opp.pts
    ,ft_opp.fga
    ,ft_opp.pace
    ,ft_opp.efg_pct
    ,ft_opp.blka
    ,ft_opp.pts_fb
    ,ft_opp.tm_tov_pct
    ,ft_opp.off_rating
    ,ft_opp.pf
    ,ft_opp.oreb_pct
    ,ft_opp.pts_2nd_chance
    ,ft_opp.fta_rate
    ,ft_opp.reb_pct
    ,ft_opp.ft_pct
    ,ft_opp.dreb
    ,ft_opp.pts_off_tov
    ,ft_opp.fg3_pct
    ,ft_opp.blk
    ,ft_opp.pct_pts_2pt_mr
    ,ft_opp.pts_paint
    ,ft_opp.dreb_pct
    ,ft_opp.pct_pts_off_tov
    ,ft_opp.pfd
    ,ft_opp.fg3a
    ,ft_opp.pct_pts_3pt
    ,ft_opp.pct_pts_fb
    ,ft_opp.ts_pct
    ,ft_opp.pct_pts_paint
    ,ft_opp.stl
    ,ft_opp.ast
    ,ft_opp.pct_pts_2pt
    ,ft_opp.fg_pct
    ,ft_opp.pct_pts_ft
    ,ft_opp.oreb
FROM rpt.dim_player_schedule ps
  LEFT JOIN rpt.dim_player p ON ps.player_id = p.id AND p.is_rec_active = TRUE
  LEFT JOIN rpt.fct_game_player gp ON ps.game_id = gp.game_id AND ps.player_id = gp.player_id
  LEFT JOIN rpt.fct_game_team ft_opp ON ps.game_id = ft_opp.game_id AND ps.opp_team_id = ft_opp.team_id
  

 

SELECT ps.player,
    ps.loc,
    ps.team,
    ps.opponent,
    ps.opp_loc,
    ps.game_date::text AS game_date,
    ps.game_date - p.birthdate AS age_in_days,
    p.nba_pos,
    pp.fd_pos,
    to_char(ps.game_date::timestamp with time zone, 'MON'::text) AS mnth,
    to_char(ps.game_date::timestamp with time zone, 'dy'::text) AS dow,
    ps.season_str,
        CASE
            WHEN gp.seconds = 0 THEN NULL::integer
            ELSE gp.seconds
        END AS seconds,
    gp.ast * 1.5::double precision + gp.reb * 1.2::double precision + gp.pts + gp.stl * 2::double precision + gp.blk * 2::double precision - gp.tov AS fdpts,
    avg(gp.ast * 1.5::double precision + gp.reb * 1.2::double precision + gp.pts + gp.stl * 2::double precision + gp.blk * 2::double precision - gp.tov) OVER (PARTITION BY ps.player_id, ps.season) AS avg_fdpts,
    lnd.get_days_rest(ps.game_date, lag(ps.game_date, 1) OVER (ORDER BY ps.season, ps.team_id, ps.player_id, ps.game_date)) AS player_days_rest,
    pd.avg_norm_plusminus AS opp_pos_plusminus,
    t.opp_efg_pct,
    t.opp_fta_rate,
    t.opp_oreb_pct,
    t.opp_pts_2nd_chance,
    t.opp_pts_fb,
    t.opp_pts_off_tov,
    t.opp_pts_paint,
    t.opp_tov_pct,
    t.plus_minus,
    t.pie,
    t.def_rating,
    t.pts,
    t.fga,
    t.pace,
    t.efg_pct,
    t.blka,
    t.pts_fb,
    t.tm_tov_pct,
    t.off_rating,
    t.pf,
    t.oreb_pct,
    t.pts_2nd_chance,
    t.fta_rate,
    t.reb_pct,
    t.ft_pct,
    t.dreb,
    t.pts_off_tov,
    t.fg3_pct,
    t.blk,
    t.pct_pts_2pt_mr,
    t.pts_paint,
    t.dreb_pct,
    t.pct_pts_off_tov,
    t.pfd,
    t.fg3a,
    t.pct_pts_3pt,
    t.pct_pts_fb,
    t.ts_pct,
    t.pct_pts_paint,
    t.stl,
    t.ast,
    t.pct_pts_2pt,
    t.fg_pct,
    t.pct_pts_ft,
    t.oreb
   FROM rpt.mvw_player_schedule ps
     LEFT JOIN rpt.dim_player p ON ps.player_id = p.id AND p.is_active = true
     LEFT JOIN rpt.dim_season s ON ps.season = s.id
     LEFT JOIN rpt.fct_game_player gp ON ps.game_id::text = gp.game_id::text AND ps.player_id = gp.player_id
     LEFT JOIN rpt.fct_game_team t ON ps.game_id::text = t.game_id::text AND ps.opponent_team_id = t.team_id
     LEFT JOIN rpt.mvw_position_defense pd ON pd.season::text = ps.season_str::text AND pd.team::text = ps.opponent::text AND pd.fd_pos::text = p.fd_pos::text
     LEFT JOIN rpt.mvw_player_fd_pos pp ON p.id = pp.player_id
  WHERE 1 = 1 AND ps.game_date <= 'now'::text::date AND gp.seconds > 0;