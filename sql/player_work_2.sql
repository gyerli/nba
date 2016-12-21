select
  public.hash8(a.player_id|| '|' || now()) dim_player_guid
  ,a.player_id|| '|' || now() crc_str
  ,a.player_id id
  ,pc.first_name 
  ,pc.last_name
  ,pc.display_first_last
  ,pc.display_last_comma_first
  ,to_date(pc.birthdate,'YYYY-MM-DD') birthdate
  ,pc.school
  ,pc.country
  ,pc.last_affiliation
  ,nullif(split_part(pc.height,'-',1),'')::integer * 12 + nullif(split_part(pc.height,'-',2),'')::integer height
  ,nullif(pc.weight,'')::INTEGER weight
  ,nullif(pc.jersey,'')::INTEGER jersey
  ,pc.position nba_pos
  ,pc.rosterstatus
  ,t.team_id
  ,t.team_name
  ,t.team_abbrv
  ,a.season
  ,a.start_date
  ,a.end_date
  ,case when pc.dleague_flag= 'Y' then true else false end is_dleague
  ,case when pc.games_played_flag = 'Y' then true else false end is_games_played
from
  (SELECT 
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
left join lnd.mvw_player_common pc on a.player_id = pc.person_id
left join lnd.team t on a.team_id = t.team_id
order by a.player_id, start_date
