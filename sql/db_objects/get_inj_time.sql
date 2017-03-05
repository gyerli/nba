CREATE OR REPLACE FUNCTION rpt.get_injury_time(p_season character varying, p_team_abbrv character varying, p_player_id integer, p_date date, p_debug boolean DEFAULT false)
 RETURNS double precision
 LANGUAGE plpgsql
AS $function$
DECLARE
-- o_secs double precision;
-- r_secs double precision;
-- r_pct_dist double precision;
 seconds double precision;
 benefit_seconds double precision;
 pos_rec record;
 inj_rec record;
BEGIN
	
	SELECT 
		pt.display_first_last
		,pt.player_id
		,pt.pos
		,pt.player_time_last_5_games
		,pt.player_time_last_10_games
		,pt.avg_player_time_last_5_games
		,pt.avg_player_time_last_10_games
		,pt.pct_player_time_5_games
		,pt.pct_player_time_10_games
		,pt.pos_time_last_5_games
		,pt.pos_time_last_10_games
	 INTO
	 	pos_rec
	  FROM rpt.mvw_player_time pt
	 WHERE pt.season = p_season
	   AND pt.team_abbreviation = p_team_abbrv
	   AND pt.player_id = p_player_id
	   AND pt.game_date = p_date
	   AND pt.injured = 'NO';  

--	select 
--	     pwm.player,
--	     pwm.fd_pos,
--	     pwm.wkly_num_games,
--	     pwm.tot_wkly_player_time,
--	     pwm.tot_wkly_pos_time,
--	     pwm.pct_wkly_player_time
--	  INTO
--	     r_player,
--	     r_pos,
--	     r_num_games,
--	     r_player_time,
--	     r_pos_time,
--	     r_pct_player_time
--	  from rpt.mvw_player_weekly_minutes pwm
--	 where pwm.season = p_season
--	   and pwm.team = p_team_abbrv
--	   and pwm.player_id = p_player_id
--	   and pwm.wk = extract('week' from p_date) - 1
--	   and pwm.injury_indicator <> 'O';

	if p_debug then
	  RAISE NOTICE 'Calculating injury benefit of % (%)', pos_rec.display_first_last, pos_rec.pos;
	  RAISE NOTICE '================================================================';
	end if;

	benefit_seconds = 0;
	seconds = 0;
	
	for inj_rec in
	select 
	    pt.display_first_last inj_player
	    ,pt.pos inj_position
	    ,pt.player_time_last_5_games inj_player_time_5
		,pt.avg_player_time_last_5_games inj_player_avg_time_5
		,pt.pos_time_last_5_games inj_pos_time_5
		,pt.pct_player_time_5_games inj_pct_player_time_5
	    ,id.dist_factor benefit_factor
	  from rpt.mvw_player_time pt
	    inner join xlat.pos_injuriy_dist id on pt.pos = id.inj_pos and
						   					   id.pos = pos_rec.pos
	 where pt.season = p_season
	   and pt.team_abbreviation = p_team_abbrv
	   and pt.game_date = p_date
	   and pt.injured = 'YES'
	   and pt.player_id <> pos_rec.player_id
	
	loop
		raise notice 'inj_player => %',inj_rec.inj_player;
		raise notice 'inj_position => %',inj_rec.inj_position;
		raise notice 'inj_player_time_5 => %',inj_rec.inj_player_time_5;
		raise notice 'inj_player_avg_time_5 => %',inj_rec.inj_player_avg_time_5;
	    raise notice 'inj_pos_time_5 => %',inj_rec.inj_pos_time_5;
		raise notice 'benefit_player_pos => %',pos_rec.pos;
		raise notice 'benefit_factor => %',inj_rec.benefit_factor;
		raise notice 'pct_inj_player_pos_contrib => %',inj_rec.inj_pct_player_time_5;
		raise notice 'pct_benefit_player_pos_contrib => %',pos_rec.pct_player_time_5_games; 
		raise notice '===> END INJ <===';
	END loop;
	
--	loop
--	  if p_debug then
--	    raise notice 'inj_player => %',pos_rec.inj_player;
--	    raise notice 'inj_player_team => %',p_team_abbrv;
--	    raise notice 'inj_position => %',pos_rec.inj_position;
--	    raise notice 'inj_wkly_num_games => %',pos_rec.inj_wkly_num_games;
--	    raise notice 'inj_wkly_player_time => %',pos_rec.inj_wkly_player_time;
--	    raise notice 'inj_wkly_player_avg_time => %',pos_rec.inj_wkly_player_avg_time;
--	    raise notice 'inj_wkly_pos_time => %',pos_rec.inj_wkly_pos_time;
--	    raise notice 'benefit_player_pos => %',pos_rec.benefit_player_pos;
--	    raise notice 'benefit_factor => %',pos_rec.benefit_factor;
--	    raise notice 'pct_benefit_player_pos_contrib => %',pos_rec.pct_benefit_player_pos_contrib;
--	  end if;
--	--r_pct_dist = r_player_time::numeric / ( r_pos_time::numeric - r_inj_secs::numeric );
--	--RAISE NOTICE '%, %, %',r_player_time,r_pos_time,r_inj_secs;
--	--RAISE NOTICE '%',r_pct_dist;
--	  seconds = ( pos_rec.inj_wkly_player_avg_time * pos_rec.pct_benefit_player_pos_contrib * pos_rec.benefit_factor);
--	  if p_debug then
--	    raise notice 'benefit seconds: % => % + ( % * % * % )', seconds, benefit_seconds, pos_rec.inj_wkly_player_avg_time, pos_rec.pct_benefit_player_pos_contrib, pos_rec.benefit_factor;
--	    raise notice '===> END INJ <===';
--	  end if;
--	  
--	  benefit_seconds = benefit_seconds + seconds;
--	end loop;
--
--	if p_debug then
--	  raise notice 'Total benefit => %', benefit_seconds;
--	  RAISE NOTICE '======> END BENEFIT PLAYER <=======';
--	end if;
--	
--	return case 
--	  when benefit_seconds = 0 then null
--	  else benefit_seconds
--	end;
-- 	r_secs =
-- 	case
-- 	  when r_num_games > 0 then (r_inj_secs * r_pct_dist) / r_num_games
-- 	  else 0
-- 	end ;
-- 	RAISE NOTICE '%',r_secs;
-- 
-- 	select
-- 	case
-- 	  when coalesce(r_secs,0) = 0 then 0.0
-- 	  when r_inj_pos = 'C' and r_pos = 'C' then r_secs * 0.75
-- 	  when r_inj_pos = 'C' and r_pos = 'PF' then r_secs * 0.25
-- 	  
-- 	  when r_inj_pos = 'PF' and r_pos = 'PF' then r_secs * 0.75
-- 	  when r_inj_pos = 'PF' and r_pos = 'C' then r_secs * 0.15
-- 	  when r_inj_pos = 'PF' and r_pos = 'SF' then r_secs * 0.10
-- 
-- 	  when r_inj_pos = 'SF' and r_pos = 'SF' then r_secs * 0.75
-- 	  when r_inj_pos = 'SF' and r_pos = 'SG' then r_secs * 0.10
-- 	  when r_inj_pos = 'SF' and r_pos = 'PG' then r_secs * 0.10
-- 	  when r_inj_pos = 'SF' and r_pos = 'PF' then r_secs * 0.05
-- 
-- 	  when r_inj_pos = 'PG' and r_pos = 'PG' then r_secs * 0.65
-- 	  when r_inj_pos = 'PG' and r_pos = 'SG' then r_secs * 0.25
-- 	  when r_inj_pos = 'PG' and r_pos = 'SF' then r_secs * 0.10
-- 	  else 0.0
-- 	END
-- 	into o_secs;
-- 	
--	return round((o_secs/60)::numeric,2);
	RETURN 0;
end	
$function$