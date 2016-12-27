CREATE MATERIALIZED VIEW rpt.mvw_player_schedule
AS
SELECT --- this is finished games 
	gp.player_id
	,p.first_name
	,p.last_name
	,g.id game_id
	,g.game_code
	,g.game_date
	,g.game_status
	,t.id team_id
	,t.team_name
	,t.team_abbreviation
	,gp.team_loc
	,t_op.id opp_team_id
	,t_op.team_name opp_team_name
	,t_op.team_abbreviation opp_team_abbreviation
	,gp.opp_team_loc
	,gp.season 
	,g.game_date - p.birthdate age
FROM rpt.dim_game g
	LEFT JOIN rpt.fct_game_player gp ON g.id = gp.game_id
	LEFT JOIN rpt.dim_team t ON gp.team_id = t.id
	LEFT JOIN rpt.dim_team t_op ON gp.opp_team_id = t_op.id
	LEFT JOIN rpt.dim_player p ON gp.player_id = p.id AND p.is_rec_active = TRUE
WHERE g.game_status = 'Final'
UNION 
SELECT -- this is un-played games when player is at "HOME"
	pth.player_id
	,p.first_name
	,p.last_name
	,g.id game_id
	,g.game_code
	,g.game_date
	,g.game_status
	,t.id team_id
	,t.team_name
	,t.team_abbreviation
	,'H' team_loc
	,t_opp.id opp_team_id
	,t_opp.team_name opp_team_name
	,t_opp.team_abbreviation opp_team_abbreviation
	,'V' opp_team_loc
	,g.season
	,g.game_date - p.birthdate age
FROM rpt.dim_game g
	LEFT JOIN job.season s ON g.season = s.season AND g.season_type = s.season_type
    LEFT JOIN ( SELECT 
    				player_id, display_first_last, team_id, team_abbrv, season, rec_start_date, rec_end_date,
    				ROW_NUMBER() OVER (PARTITION BY player_id, season ORDER BY rec_start_date DESC) rn
    			  FROM rpt.dim_player_team_history ) pth
    										  ON g.home_team_id = pth.team_id AND
    			  							 	 g.season = pth.season AND
    			  							 	 pth.rn = 1
	LEFT JOIN rpt.dim_team t ON g.home_team_id = t.id
	LEFT JOIN rpt.dim_team t_opp ON visitor_team_id = t_opp.id
	LEFT JOIN rpt.dim_player p ON pth.player_id = p.id AND p.is_rec_active = TRUE     			  							 	 
WHERE g.game_status <> 'Final'
UNION 
SELECT -- this is un-played games when player is at "VISITOR"
	pth.player_id
	,p.first_name
	,p.last_name
	,g.id game_id
	,g.game_code
	,g.game_date
	,g.game_status
	,t.id team_id
	,t.team_name
	,t.team_abbreviation
	,'V' team_loc
	,t_opp.id opp_team_id
	,t_opp.team_name opp_team_name
	,t_opp.team_abbreviation opp_team_abbreviation
	,'H' opp_team_loc
	,g.season
	,g.game_date - p.birthdate age
FROM rpt.dim_game g
	LEFT JOIN job.season s ON g.season = s.season AND g.season_type = s.season_type
    LEFT JOIN ( SELECT 
    				player_id, display_first_last, team_id, team_abbrv, season, rec_start_date, rec_end_date,
    				ROW_NUMBER() OVER (PARTITION BY player_id, season ORDER BY rec_start_date DESC) rn
    			  FROM rpt.dim_player_team_history ) pth
    										  ON g.visitor_team_id = pth.team_id AND
    			  							 	 g.season = pth.season AND
    			  							 	 pth.rn = 1
	LEFT JOIN rpt.dim_team t ON g.visitor_team_id = t.id
	LEFT JOIN rpt.dim_team t_opp ON home_team_id = t_opp.id
	LEFT JOIN rpt.dim_player p ON pth.player_id = p.id AND p.is_rec_active = TRUE     			  							 	 
WHERE g.game_status <> 'Final'
ORDER BY game_date desc