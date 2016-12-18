SELECT 
   a.player_id 
  ,a.player_name 
  ,a.team_id
  ,a.team_abbreviation
  ,a.season
  ,a.start_date
  ,CASE
     WHEN s.is_current THEN 'Y'
     ELSE 'N'
   END is_active
  ,CASE
     WHEN s.is_current THEN NULL
     ELSE a.end_date 
   END end_date
FROM (
SELECT 
  gps.player_id 
  ,gps.player_name 
  ,gps.team_id
  ,gps.team_abbreviation
  ,gh._season season
  ,MIN(to_date(split_part(gh.game_date_est,'T',1),'YYYY-MM-DD')) start_date
  ,MAX(to_date(split_part(gh.game_date_est,'T',1),'YYYY-MM-DD')) end_date
  FROM lnd.mvw_game_player_stats gps
    INNER JOIN lnd.mvw_schedule_game_header gh ON gps.game_id = gh.game_id
 WHERE gps.team_abbreviation NOT IN ('EST','WST')
GROUP BY 
  gps.player_id 
  ,gps.player_name 
  ,gps.team_id
  ,gps.team_abbreviation
  ,gh._season
) a
INNER JOIN ( SELECT season, is_current 
			   FROM job.season 
			  WHERE season_type = 'Regular Season') s ON a.season = s.season 
ORDER BY a.player_name, a.season, start_date 
