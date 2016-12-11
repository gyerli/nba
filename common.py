import datetime
import logging
import psycopg2 as pg
from sqlalchemy import create_engine



class reg(object):
  def __init__(self, cursor, row):
    for (attr, val) in zip((d[0] for d in cursor.description), row) :
      setattr(self, attr, val)


def start_run():

  param_str = "season:{0};season_type:{1}".format(g_season,g_season_type)

  try:
    cur = conn.cursor()
    sql = "INSERT INTO job.run(start_dtm,run_status,parameters,season,season_type) " \
          "VALUES ('{0}','{1}','{2}','{3}','{4}') " \
          "RETURNING id".format(datetime.datetime.now(),'IN PROGRESS',param_str,g_season,g_season_type)
    cur.execute(sql)
    run_id = cur.fetchone()[0]
    log.debug('started run => {0}'.format(run_id))
    conn.commit()
  except Exception, e:
    log.error('error creating session')
    raise

  return run_id

def end_run(run_id,status):
  try:
    cur = conn.cursor()
    sql = "UPDATE job.run " \
          "   SET run_status = '{0}', " \
          "       end_dtm = '{1}' " \
          " WHERE id = {2} ".format(status,datetime.datetime.now(),run_id)
    cur.execute(sql)
    log.debug('ended run => {0} with status => {1}'.format(run_id,status))
    conn.commit()

    if status == 'FAILED':
      log.error('updating all run logs as FAILED')
      sql = "UPDATE job.run_log " \
            "   SET node_status = 'FAILED', " \
            "       end_dtm = '{0}' " \
            " WHERE run_id = {1} ".format(datetime.datetime.now(),run_id)
      log.debug(sql)
      cur.execute(sql)
      conn.commit()

  except Exception, e:
    log.error('error ending run')
    raise



def start_log(run_id,node,node_key,node_status):
  sql = "INSERT INTO job.run_log " \
        "(run_id,node,node_key,node_status,season,season_type,started_dtm) " \
        "VALUES " \
        "({0},'{1}','{2}','{3}','{4}','{5}','{6}')".format(
         run_id
        ,node
        ,node_key
        ,node_status
        ,g_season
        ,g_season_type
        ,datetime.datetime.now())

  cur = conn.cursor()
  cur.execute(sql)
  conn.commit()

def end_log(run_id,node,key,status):
  sql = "UPDATE job.run_log " \
        "  SET node_status = '{0}' " \
        "      ,end_dtm = '{1}' " \
        " WHERE run_id = {2} " \
        "   AND node = '{3}' " \
        "   AND node_key = '{4}' " \
        "   AND season = '{5}' " \
        "   AND season_type = '{6}' ".format(status,datetime.datetime.now(),run_id,node,key,g_season,g_season_type)

  cur = conn.cursor()
  cur.execute(sql)
  conn.commit()
   

def get_season_dates():
  sql = "SELECT start_date, end_date, is_current " \
        "  FROM job.season " \
        " WHERE season = '{0}'" \
        "   AND season_type = '{1}'".format(g_season,g_season_type)

  cur = conn.cursor()
  cur.execute(sql)
  dt = cur.fetchone()
  return [dt[0],dt[1],dt[2]]

def get_measures(node):
  cur = conn.cursor()

  sql = "SELECT node, endpoint, measure, table_name, active, measure_type, " \
        "       measure_category, comments, available_year, pk, measure_scope " \
        "  FROM job.node " \
        " WHERE node = '{0}' ".format(node)

  cur.execute(sql)
  return cur

def s_to_sql(df,params):
  df.columns = map(unicode.lower, df.columns)
  df.rename(columns={'to':'tov'},inplace=True)
  df.rename(columns={'stats_value':'_stats_value'},inplace=True)
  df['_season'] = g_season
  df['_season_type'] = g_season_type
  df['_create_date'] = cmn.now()
  df.to_sql(name=params['table_name'],con=engine,schema='lnd',if_exists='append',index=False)

def p_to_sql(df,params):
  df.columns = map(unicode.lower, df.columns)
  df.rename(columns={'to':'tov'},inplace=True)
  df.rename(columns={'stats_value':'_stats_value'},inplace=True)
  df['_player_id'] = params['player_id']
  df['_team_id'] = params['team_id']
  df['_season'] = g_season
  df['_season_type'] = g_season_type
  df['_create_date'] = datetime.datetime.now()
  df.to_sql(name=params['table_name'],con=engine,schema='lnd',if_exists='append',index=False)


def g_to_sql(df,params):
  df.columns = map(unicode.lower, df.columns)
  df.rename(columns={'to':'tov'},inplace=True)
  df['_game_id'] = params['game_id']
  df['_season'] = g_season
  df['_season_type'] = g_season_type
  df['_create_date'] = datetime.datetime.now()
  df.to_sql(name=params['table_name'],con=engine,schema='lnd',if_exists='append',index=False)


def t_to_sql(df,params):
  df.columns = map(unicode.lower, df.columns)
  df.rename(columns={'to':'tov'},inplace=True)
  df['_team_id'] = params['team_id']
  df['_season'] = g_season
  df['_season_type'] = g_season_type
  df['_create_date'] = datetime.datetime.now()
  df.to_sql(name=params['table_name'],con=engine,schema='lnd',if_exists='append',index=False)

def m_to_sql(df,params):
  df.columns = map(unicode.lower, df.columns)
  df.rename(columns={'to':'tov'},inplace=True)
  df['_team_id'] = params['team_id']
  df['_season'] = params['season']
  df['_create_date'] = datetime.datetime.now()
  df.to_sql(name=params['table_name'],con=engine,schema='lnd',if_exists='append',index=False)


def s_to_sql(df,params):
  df.columns = map(unicode.lower, df.columns)
  df.rename(columns={'to':'tov'},inplace=True)
  df['_season'] = g_season
  df['_season_type'] = g_season_type
  df['_create_date'] = datetime.datetime.now()
  df.to_sql(name=params['table_name'],con=engine,schema='lnd',if_exists='append',index=False)

def refresh_mviews():
  sql = "SELECT job.refresh_mviews()"
  cur = conn.cursor()
  cur.execute(sql)
  conn.commit()

valid_seasons = ['2016-17', '2015-16', '2014-15', '2013-14', '2012-13', '2011-12', '2010-11', '2009-10', '2008-09',
                 '2007-08', '2006-07', '2005-06']

current_season = '2016-17'

<<<<<<< HEAD
nba_home = './'
data_folder = './data/'
log_folder = './log/'
>>>>>>> adc542fd0e0bb85f6e37e7f97c71371b69c7a4af

# Database
#==============================================================================
conn = pg.connect(database="nba", user="ictsh", password="gyerli", host="aws-srv-1", port="5432")
engine = create_engine('postgresql://ictsh:gyerli@aws-srv-1:5432/nba')

# Logger
#==============================================================================
log = logging.getLogger('go')
log.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler("{0}go.log".format(log_folder))
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
ch.setFormatter(formatter)
fh.setFormatter(formatter)
# add the handlers to logger
log.addHandler(ch)
log.addHandler(fh)

global g_season
global g_season_type
global g_season_start_date
global g_season_end_date