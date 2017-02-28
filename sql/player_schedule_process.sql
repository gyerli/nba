DROP TABLE rpt.tmp_sch;
CREATE TABLE rpt.tmp_sch
AS


SELECT *
FROM (
WITH team_pos_defense
AS
(
SELECT 
	  id team_id
	, pos
	, season
	, round(avg(less_than_6ft)::numeric, 4) less_than_6ft 
	, round(avg(less_than_10ft)::numeric, 4) less_than_10ft
	, round(avg(greater_than_15ft)::numeric, 4) greater_than_15ft
	, round(avg(three_pointers)::numeric, 4) three_pointers
	, round(avg(two_pointers)::numeric, 4) two_pointers
	, round(avg(overall)::numeric, 4) overall 
FROM
(
SELECT 
	  t.id
	, t.team_name
	, p.pos
	, CASE WHEN pd.defense_category = 'Less Than 6 Ft' THEN pd.pct_plusminus ELSE 0 END less_than_6ft
	, CASE WHEN pd.defense_category = 'Less Than 10 Ft' THEN pd.pct_plusminus ELSE 0 END less_than_10ft
	, CASE WHEN pd.defense_category = 'Greater Than 15 Ft' THEN pd.pct_plusminus ELSE 0 END greater_than_15ft
	, CASE WHEN pd.defense_category = '3 Pointers' THEN pd.pct_plusminus ELSE 0 END three_pointers
	, CASE WHEN pd.defense_category = '2 Pointers' THEN pd.pct_plusminus ELSE 0 END two_pointers
	, CASE WHEN pd.defense_category = 'Overall' THEN pd.pct_plusminus ELSE 0 END overall
	, pd._season season
  FROM lnd.mvw_player_defending_shots pd
    JOIN rpt.dim_player p ON pd._player_id = p.id AND p.is_rec_active = TRUE AND p.rosterstatus = 'Active'
    JOIN rpt.dim_team t ON pd._team_id = t.id     
) a
WHERE trim(pos) <> ''
GROUP BY 
	  id
	, pos
	, season
),
team_pos_time
AS
(SELECT 
	 season
	,team_id
	,team_abbreviation
	,pos
	,game_date
	,pos_time_per_game
	,sum(pos_time_per_game) OVER (PARTITION BY season, team_id, pos ORDER BY game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) pos_time_last_5_games
	,sum(pos_time_per_game) OVER (PARTITION BY season, team_id, pos ORDER BY game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) pos_time_last_10_games
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
),
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
	,sum(gp.seconds) OVER (PARTITION BY ps.season, ps.team_id, p.pos, ps.player_id ORDER BY ps.game_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) / tpt.pos_time_last_5_games pct_player_time_5_games
	,sum(gp.seconds) OVER (PARTITION BY ps.season, ps.team_id, p.pos, ps.player_id ORDER BY ps.game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) / tpt.pos_time_last_10_games pct_player_time_10_games
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
	,pd.less_than_6ft       opp_def_less_than_6ft 
	,pd.less_than_10ft      opp_def_less_than_10ft
	,pd.greater_than_15ft   opp_def_greater_than_15ft
	,pd.three_pointers      opp_def_three_pointers
	,pd.two_pointers        opp_def_two_pointers
	,pd.overall             opp_def_overall 
	,ths.charges_drawn          opp_hust_charges_drawn
	,ths.contested_shots        opp_hust_contested_shots
	,ths.contested_shots_2pt    opp_hust_contested_shots_2pt
	,ths.contested_shots_3pt    opp_hust_contested_shots_3pt
	,ths.deflections            opp_hust_deflections
	,ths.loose_balls_recovered  opp_hust_loose_balls_recovered
	,ths.screen_assists	        opp_hust_screen_assists
  FROM rpt.dim_player_schedule ps
    JOIN rpt.dim_player p ON ps.player_id = p.id AND p.is_rec_active = TRUE
    JOIN lnd.mvw_team_hustle_stats ths ON ps.opp_team_id = ths.team_id AND ps.season = ths._season
    LEFT JOIN rpt.fct_game_player gp ON ps.player_id = gp.player_id AND ps.game_id = gp.game_id
    LEFT JOIN team_pos_defense pd ON ps.opp_team_id = pd.team_id AND p.pos = pd.pos AND ps.season = pd.season
    LEFT JOIN player_news pn ON ps.player_id = pn.player_id AND ps.game_date = pn.dt AND pn.rn = 1
    LEFT JOIN team_pos_time tpt ON ps.season = tpt.season AND ps.team_id = tpt.team_id AND ps.game_date = tpt.game_date AND p.pos = tpt.pos
WHERE 1=1
  AND ps.season = '2016-17'
--  AND ps.game_date between '2017-01-01' AND current_date
--   AND ps.first_name = 'Kawhi'
) a
WHERE rn = 1
ORDER BY game_date, team_abbreviation 

  
  
