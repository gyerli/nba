CREATE OR REPLACE FUNCTION rpt.refresh_dim_player()
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
DECLARE
  cnt INTEGER;
BEGIN
with new_values as
(
select 
public.hash8(player_id|| '|' || team_id::varchar || '|' || season || '|' || now()) dim_player_guid
,player_id|| '|' || team_id::varchar || '|' || season || '|' || now() crc_str
,player_id id
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
,team_abbrv
,max_dt last_played_date
,start_date rec_start_date
,case 
   when end_date = max_dt then null
   else end_date 
 end rec_end_date
,case 
   when end_date = max_dt then true
   else false
 end is_rec_active
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
		,t.team_abbrv
		,a.start_date
		,a.end_date
		,row_number() over (partition by pc.person_id, pc._season, pc._team_id order by a.start_date desc) rn
		,max(a.end_date) over(partition by pc.person_id) max_dt
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
),
update_deactv as
(
update rpt.dim_player da
set 
  updated_at = now(),
  rec_end_date	 = now(),
  is_rec_active	 = false
from new_values nv
where da.id = nv.id
and da.is_rec_active = true
and (da.first_name                   <>      nv.first_name                           OR
	da.last_name                     <>      nv.last_name                            OR
	da.display_first_last            <>      nv.display_first_last                   OR
	da.display_last_comma_first      <>      nv.display_last_comma_first             OR
	da.display_fi_last               <>      nv.display_fi_last                      OR
	da.birthdate                     <>      nv.birthdate                            OR
	da.school                        <>      nv.school                               OR
	da.country                       <>      nv.country                              OR
	da.last_affiliation              <>      nv.last_affiliation                     OR
	da.height                        <>      nv.height                               OR
	da.weight                        <>      nv.weight                               OR
	da.season_exp                    <>      nv.season_exp                           OR
	da.jersey                        <>      nv.jersey                               OR
	da.nba_pos                       <>      nv.nba_pos                              OR
	da.rosterstatus                  <>      nv.rosterstatus                         OR
	da.playercode                    <>      nv.playercode                           OR
	da.from_year                     <>      nv.from_year                            OR
	da.to_year                       <>      nv.to_year                              OR
	da.is_dleague                    <>      nv.is_dleague                           OR
	da.is_games_played               <>      nv.is_games_played                      OR
	da.draft_year                    <>      nv.draft_year                           OR
	da.draft_round                   <>      nv.draft_round                          OR
	da.draft_number                  <>      nv.draft_number                         OR
	da.season                        <>      nv.season                               OR
	da.team_id                       <>      nv.team_id                              OR
	da.team_name                     <>      nv.team_name                            OR
	da.team_abbrv                    <>      nv.team_abbrv                           OR
	da.last_played_date              <>      nv.last_played_date        )
returning da.*
)
INSERT INTO rpt.dim_player(
            dim_player_guid, crc_str, id, first_name, last_name, display_first_last, 
            display_last_comma_first, display_fi_last, birthdate, school, country, 
            last_affiliation, height, weight, season_exp, jersey, nba_pos, rosterstatus, 
            playercode, from_year, to_year, is_dleague, is_games_played, draft_year, 
            draft_round, draft_number, season, team_id, team_name, team_abbrv, last_played_date, 
            rec_start_date, rec_end_date, is_rec_active, created_at, updated_at)
SELECT 
	dim_player_guid, crc_str, id, first_name, last_name, display_first_last, 
	display_last_comma_first, display_fi_last, birthdate, school, country, 
	last_affiliation, height, weight, season_exp, jersey, nba_pos, rosterstatus, 
	playercode, from_year, to_year, is_dleague, is_games_played, draft_year, 
	draft_round, draft_number, season, team_id, team_name, team_abbrv, last_played_date, 
	rec_start_date, rec_end_date, true, now(), null::timestamp
FROM new_values
where not exists ( select 1
		             from rpt.dim_player dp
		            where dp.id = new_values.id )
union
SELECT 	dim_player_guid, crc_str, id, first_name, last_name, display_first_last, 
	display_last_comma_first, display_fi_last, birthdate, school, country, 
	last_affiliation, height, weight, season_exp, jersey, nba_pos, rosterstatus, 
	playercode, from_year, to_year, is_dleague, is_games_played, draft_year, 
	draft_round, draft_number, season, team_id, team_name, team_abbrv, last_played_date,   
   now(), now(), true, now(), null::timestamp
 FROM new_values
 where exists ( select 1
		          from update_deactv up
		         where up.id = new_values.id )
;

GET DIAGNOSTICS cnt = ROW_COUNT;
RETURN cnt;
END
$function$
