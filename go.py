import sys
import nba_py
import datetime
import argparse
import inspect
import sqlparse

from nba_py import player as _player
from nba_py import constants as _constants
import gamev2 as _game
import teamv2 as _team
import common as c


def update_schedule():
  start_dt = g_season_start_date
  end_dt = g_season_end_date

  cur = c.conn.cursor()

  #find if any game processed for this season
  sql = "SELECT " \
        "  MAX(to_date(split_part(game_date_est, 'T', 1), 'YYYY-MM-DD')) " \
        "FROM lnd.schedule_game_header " \
        "WHERE game_status_text = 'Final' " \
        "  AND _season = '{0}' " \
        "  AND _season_type = '{1}' ".format(g_season, g_season_type)
 
  try:
    cur.execute(sql)
    last_processed_game_dt = cur.fetchone()[0]
    if  last_processed_game_dt != None:
      c.log.info('found processed games for this season')
      c.log.info('last processed schedule date: {0}'.format(
                        last_processed_game_dt.strftime("%Y-%m-%d")))
      start_dt = last_processed_game_dt + datetime.timedelta(days=1)
      end_dt = datetime.date.today()
      #remove below after testing
      end_dt = datetime.date(2016,10,26)

    c.log.info('adjusted start date: {0}'.format(start_dt.strftime("%Y-%m-%d")))
    c.log.info('adjusted end date: {0}'.format(end_dt.strftime("%Y-%m-%d")))

    while start_dt <= end_dt:
      c.log.info('getting games for the date => {0}'.format(start_dt.strftime("%Y%m%d")))
      measures = c.get_measures('schedule')
      for measure in measures.fetchall():
        m = c.reg(measures,measure)
        c.log.debug('running schedule endpoint => {0}, measure => {1}'.format(
                        m.endpoint,m.measure))
        try:
          endpoint = getattr(nba_py,m.endpoint)(year=start_dt.year, month=start_dt.month, day=start_dt.day)
          df = getattr(endpoint,m.measure)()
          s_params = {'table_name':m.table_name}
          c.s_to_sql(df,s_params)
        except Exception, e:
          c.log.error('error processing measure in {0}'.format(inspect.stack()[0][3]))
          c.log.error(e)
          raise

      start_dt += datetime.timedelta(days=1)
    c.refresh_mviews()
  
  except Exception, e:
    c.log.info(e)
    raise


def refresh_team_roster_coaches():
  c.log.info('updating team roster and coaches')

  sql = "SELECT DISTINCT " \
        "    team_id, abbreviation " \
        "  FROM lnd.team " \
        " WHERE abbreviation IS NOT NULL"
  cur = c.conn.cursor()
  cur.execute(sql)
  teams = cur.fetchall()
  season_type = 'Regular Season'

  for season in c.valid_seasons:
    for t in teams:
      c.log.info('Updating Team => {0} ({1}) seasonal stats for season => {2}'.format(t[1],t[0],season))
      # ts_params = teamid|season|season_type

      ts = _team.TeamSummary(team_id=t[0],season=season,season_type=season_type)
      tr = _team.TeamCommonRoster(team_id=t[0],season=season)

      c.m_to_sql(ts.info(),{'team_id':t[0],'season':season,'table_name':'team_summary'})
      c.m_to_sql(ts.season_ranks(),{'team_id':t[0],'season':season,'table_name':'team_season_ranks'})
      c.m_to_sql(tr.roster(),{'team_id':t[0],'season':season,'table_name':'team_common_roster'})
      c.m_to_sql(tr.coaches(),{'team_id':t[0],'season':season,'table_name':'team_coaches'})


def get_games():
  c.log.info('getting games to be processed')
  
  sql =  "SELECT " \
        " g.game_date_est " \
        ",g.game_id " \
        ",g.gamecode " \
        ",g.home_team_id " \
        ",g.visitor_team_id " \
        ",g._season " \
        ",g._season_type " \
        "  FROM lnd.mvw_schedule_game_header g " \
        " WHERE g._season = '{0}' AND g._season_type = '{1}' " \
        "   AND g.game_status_text = 'Final' " \
        "   AND NOT EXISTS ( SELECT 1 FROM job.run_log rl " \
        "                     WHERE rl.node = 'game' " \
        "                       AND rl.node_key = g.game_id  " \
        "                       AND rl.node_status = 'COMPLETED' ) ".format(g_season, g_season_type)

  c.log.debug(sql)
  cur = c.conn.cursor()
  cur.execute(sql)
  return cur

def process_game(game_id):
  #check if this game stats are already in
  sql = 

####################################################################################
# M A I N  M O D U L E
####################################################################################

def main():

  global g_season
  global g_season_type
  global g_force
  global g_history
  global g_season_start_date
  global g_season_end_date
  global g_is_current_season
  global g_run_id


  parser = argparse.ArgumentParser(description='Executes job and job details')
  parser.add_argument('-s', '--season', help='NBA season (e.g. 2014-15)', required=False, default=c.current_season)
  parser.add_argument('-t', '--season_type', help='Season type (R=>Regular Season, P=>Playoffs)', default='R')
  parser.add_argument('-e', '--schedule', help='Update schedule', action='store_true')
  parser.add_argument('-r', '--roster', help='Refresh team roster and coaches', action='store_true')

  args = vars(parser.parse_args())

  g_season = args['season']

  if args['season_type'] == 'P': g_season_type = _constants.SeasonType.Playoffs
  else: g_season_type = _constants.SeasonType.Regular


  c.g_season = g_season 
  c.g_season_type = g_season_type 

  dt =  c.get_season_dates()

  g_season_start_date = dt[0]
  g_season_end_date = dt[1]
  g_is_current_season = dt[2]

  c.g_season_start_date = g_season_start_date 
  c.g_season_end_date = g_season_end_date

  c.log.info('processing season {0} for {1}'.format(g_season,g_season_type))
  c.log.info('season start date: {0} end date: {1}'.format(
                g_season_start_date.strftime("%Y-%m-%d"), 
                g_season_end_date.strftime("%Y-%m-%d")))

  update_schedule()
  if args['roster']: refresh_team_roster_coaches()

  g_run_id = c.start_run()

  games = get_games()
  for game in games.fetchall():
    g = c.reg(games,game)
    c.log.info('processing game:{0}'.format(g.gamecode))
    try:
      c.start_log(g_run_id,'game',g.game_id,'IN PROCESS')     
      process_game(g.game_id)
      raw_input("Press Enter to continue...")
      c.end_log(g_run_id,'game',g.game_id,'COMPLETED')
    except Exception, e:
      c.log.error('error processing game:{0}'.format(g.gamecode))
      c.log.error(e)
      c.end_run(g_run_id,'FAILED') 
      raise
  c.end_run(g_run_id,'COMPLETED') 

if __name__ == '__main__':
    sys.exit(main())
