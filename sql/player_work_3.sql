select 
player_id
,first_name
,last_name
,display_first_last
,display_last_comma_first
,display_fi_last
,birthdate
,school
,country
,last_affiliation
,height
,weight
,season_exp
,jersey
,nba_pos
,rosterstatus
,playercode
,from_year
,to_year
,is_dleague
,is_games_played
,draft_year
,draft_round
,draft_number
,season
,team_id
,team_name
,end_date last_played_date
,start_date rec_start_date
,case 
   when end_date = max_dt then null
   else end_date 
 end rec_end_date
,case 
   when end_date = max_dt then true
   else false
 end is_rec_active
--,max_dt
--,cnt
--,rn
from (select 
	pc.person_id player_id
	,pc.first_name
	,pc.last_name
	,pc.display_first_last
	,pc.display_last_comma_first
	,pc.display_fi_last
    	,to_date(pc.birthdate,'YYYY-MM-DD') birthdate
    	,pc.school
    	,pc.country
    	,pc.last_affiliation
	,nullif(split_part(pc.height,'-',1),'')::integer * 12 + nullif(split_part(pc.height,'-',2),'')::integer height
	,nullif(pc.weight,'')::INTEGER weight
	,pc.season_exp
	,nullif(pc.jersey,'')::INTEGER jersey
	,pc.position nba_pos
	,pc.rosterstatus
	,pc.playercode
	,pc.from_year
	,pc.to_year
  	,case when pc.dleague_flag= 'Y' then true else false end is_dleague
  	,case when pc.games_played_flag = 'Y' then true else false end is_games_played
    	,pc.draft_year
    	,pc.draft_round
	,pc.draft_number
	,pc._season season
	,t.team_id
	,t.team_name
	,a.start_date
	,a.end_date
	,row_number() over (partition by pc.person_id, pc._season, pc._team_id order by a.start_date desc) rn
	,max(a.end_date) over(partition by pc.person_id) max_dt
	,cnt
  from lnd.player_common pc
    left join lnd.team t on pc._team_id = t.team_id
    left join ( SELECT 
		   gps.player_id 
		  ,gps.team_id
		  ,gps._season
		  ,MIN(to_date(split_part(gh.game_date_est,'T',1),'YYYY-MM-DD')) start_date
		  ,MAX(to_date(split_part(gh.game_date_est,'T',1),'YYYY-MM-DD')) end_date
		  ,count(player_id) over(partition by player_id) cnt
		  FROM lnd.mvw_game_player_stats gps
		    join lnd.mvw_schedule_game_header gh on gps.game_id = gh.game_id
		WHERE gps.team_abbreviation NOT IN ('EST','WST')
		GROUP BY 
		   gps.player_id 
		  ,gps.team_id
		  ,gps._season ) a on pc.person_id = a.player_id and
						      pc._team_id = a.team_id and
						      pc._season = a._season
) b
where 1=1
  and rn = 1
--  and cnt > 0
--  and player_id = 201196
order by player_id, season, start_date
