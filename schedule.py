import logging
import nba_py
import datetime
import psycopg2 as pg
from nba_py import constants
from datetime import date
from sqlalchemy import create_engine
from pandas.io import sql

def get_last_processed_game_date():
    sql = "SELECT " \
          "  max(to_date(split_part(game_date_est, 'T', 1), 'YYYY-MM-DD')) + interval '1' day " \
          "FROM lnd.mvw_schedule_game_header " \
          "WHERE game_status_text = 'Final' " \
          "  AND _season = '{0}' " \
          "  AND _season_type = '{1}' ".format(g_season, g_season_type)

    rootLogger.debug(sql)
    cur = conn.cursor()
    cur.execute(sql)
    e = cur.fetchone()
    start_date = e[0]
    return start_date.date()

####################################################################################
# M A I N  M O D U L E
####################################################################################

rootLogger.info("Getting schedule for season => {0} and season_type => {1}".format(g_season, g_season_type))    
dates = get_season_start_end()

start_date = dates[0]

if args['update']:
  rootLogger.info('Updating schedule as of today ({0})'.format(datetime.date.today()))
  end_date = datetime.date.today()
  start_date = get_last_processed_game_date()
else:
  rootLogger.info('Retrieving full schedule')
  end_date = dates[1]

rootLogger.info("Start date => {0} End Date => {1}".format(start_date,end_date))

delta = datetime.timedelta(days=1)
while start_date <= end_date:
  rootLogger.info('Getting games for the date => {0}'.format(start_date.strftime("%Y%m%d")))
  sb = nba_py.Scoreboard(year=start_date.year, month=start_date.month, day=start_date.day)
  pg_to_sql(sb.game_header(),'schedule_game_header')
  pg_to_sql(sb.line_score(),'schedule_line_score')
  pg_to_sql(sb.series_standings(),'schedule_series_standings')
  pg_to_sql(sb.last_meeting(),'schedule_last_meeting')
  start_date += delta
