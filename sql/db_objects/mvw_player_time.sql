DROP MATERIALIZED VIEW rpt.mvw_player_time;
CREATE MATERIALIZED VIEW rpt.mvw_player_time
AS
SELECT *
  FROM 
(  
WITH team_pos_time 
AS
(SELECT 
	 a.season
	,a.team_id
	,a.team_abbreviation
	,a.pos
	,a.game_date
	,a.pos_time_per_game
	,sum(a.pos_time_per_game) OVER (PARTITION BY a.season, a.team_id, a.pos ORDER BY a.game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) pos_time_last_5_games
	,sum(a.pos_time_per_game) OVER (PARTITION BY a.season, a.team_id, a.pos ORDER BY a.game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) pos_time_last_10_games
FROM (SELECT 
		gp.season
		,gp.team_id
		,gp.team_abbreviation
		,p.pos
		,gp.game_date
		,COALESCE(sum(gp.seconds),0) pos_time_per_game
	  FROM rpt.fct_game_player gp
	    JOIN rpt.dim_player p ON gp.dim_player_guid = p.dim_player_guid
	WHERE trim(p.pos) <> '' 
	GROUP BY 	
		gp.season
		,gp.team_id
		,gp.team_abbreviation
		,p.pos
		,gp.game_date
	ORDER BY 
		gp.season
		,gp.team_abbreviation
		,p.pos
		,gp.game_date
	) a
)	
,
player_news
AS 
(SELECT
	 CASE WHEN trim(n.playerid) = '' THEN -2 ELSE n.playerid::integer END player_id
	,n.firstname || ' ' || n.lastname display_first_last
	,to_timestamp(n.date::numeric) tm 
	,to_timestamp(n.date::numeric)::date dt
	,row_number() OVER (PARTITION BY playerid, to_timestamp(n.date::numeric)::date ORDER BY n.priority) rn
	,n.priority
	,n.headline
	,n.injured
	,n.injured_status
	,n.injury_detail
	,n.injury_location
	,n.injury_side
	,n.injury_type
	,n.listitemcaption
  FROM lnd.mvw_player_news n 
)
SELECT 
	ps.player_id
	,row_number() OVER (PARTITION BY ps.player_id, ps.game_date ORDER BY ps.created_at DESC) rn
	,p.display_first_last
	,p.pos
	,ps.game_date
	,ps.team_abbreviation
	,ps.opp_team_abbreviation
	,ps.opp_team_id
	,ps.season
	,gp.start_position
	,gp.seconds player_time
	,sum(gp.seconds) OVER (PARTITION BY ps.season, ps.team_id, p.pos, ps.player_id ORDER BY ps.game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) player_time_last_5_games
	,sum(gp.seconds) OVER (PARTITION BY ps.season, ps.team_id, p.pos, ps.player_id ORDER BY ps.game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) player_time_last_10_games
	,tpt.pos_time_per_game
	,tpt.pos_time_last_5_games
	,tpt.pos_time_last_10_games
	,CASE 
		WHEN coalesce(tpt.pos_time_last_5_games,0) = 0 THEN 0
		ELSE sum(gp.seconds) OVER (PARTITION BY ps.season, ps.team_id, p.pos, ps.player_id ORDER BY ps.game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) / tpt.pos_time_last_5_games 
	 END pct_player_time_5_games
	,CASE 
		WHEN COALESCE(pos_time_last_10_games,0) = 0 THEN 0 
		ELSE sum(gp.seconds) OVER (PARTITION BY ps.season, ps.team_id, p.pos, ps.player_id ORDER BY ps.game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) / tpt.pos_time_last_10_games 
	 END pct_player_time_10_games
	,gp.comment
	,CASE 
		WHEN pn.injured IS NOT NULL THEN pn.injured
		WHEN upper(gp.COMMENT) LIKE '%DECISION%' OR 
			 upper(gp.COMMENT) LIKE '%DRESS%' OR
			 upper(gp.COMMENT) LIKE '%SUSPEN%' OR
			 upper(gp.COMMENT) LIKE '%PERSONAL%' OR
			 upper(gp.COMMENT) LIKE '%SERVING%' THEN 'NO'
		WHEN gp.COMMENT LIKE '%DND%' THEN 'YES'
		ELSE 'NO'
	 END injured
	,pn.injured_status
	,pn.injury_detail
	,pn.injury_location
	,pn.injury_side
	,pn.injury_type
	,pn.listitemcaption caption
  FROM rpt.dim_player_schedule ps
    JOIN rpt.dim_player p ON ps.player_id = p.id AND p.is_rec_active = TRUE
    LEFT JOIN rpt.fct_game_player gp ON ps.player_id = gp.player_id AND ps.game_id = gp.game_id
    LEFT JOIN player_news pn ON ps.player_id = pn.player_id AND ps.game_date = pn.dt AND pn.rn = 1
    LEFT JOIN team_pos_time tpt ON ps.season = tpt.season AND ps.team_id = tpt.team_id AND ps.game_date = tpt.game_date AND p.pos = tpt.pos
WHERE 1=1
--  AND ps.season = '2016-17'
--  AND ps.game_date between '2017-01-01' AND current_date
--   AND ps.first_name = 'Kawhi'
) a
WHERE rn = 1
ORDER BY game_date, team_abbreviation 
  