import datetime
import logging
from logging.handlers import TimedRotatingFileHandler
import psycopg2 as pg
from sqlalchemy import create_engine
import os


class reg(object):
    def __init__(self, cursor, row):
        for (attr, val) in zip((d[0] for d in cursor.description), row):
            setattr(self, attr, val)


def start_run():
    param_str = "season:{0};season_type:{1}".format(g_season, g_season_type)

    try:
        cur = conn.cursor()
        sql = "INSERT INTO job.run(start_dtm,run_status,parameters,season,season_type) " \
              "VALUES ('{0}','{1}','{2}','{3}','{4}') " \
              "RETURNING id".format(datetime.datetime.now(), 'IN PROGRESS', param_str, g_season, g_season_type)
        cur.execute(sql)
        run_id = cur.fetchone()[0]
        log.debug('started run => {0}'.format(run_id))
        conn.commit()
    except Exception, e:
        log.error('error creating session')
        raise

    return run_id


def end_run(run_id, status):
    try:
        cur = conn.cursor()
        sql = "UPDATE job.run " \
              "   SET run_status = '{0}', " \
              "       end_dtm = '{1}' " \
              " WHERE id = {2} ".format(status, datetime.datetime.now(), run_id)
        cur.execute(sql)
        log.debug('ended run => {0} with status => {1}'.format(run_id, status))
        conn.commit()

        # if status == 'FAILED':
        #     log.error('updating all run logs as FAILED')
        #     sql = "UPDATE job.run_log " \
        #           "   SET node_status = 'FAILED', " \
        #           "       end_dtm = '{0}' " \
        #           " WHERE run_id = {1} " \
        #           "   AND node <> 'game' ".format(datetime.datetime.now(), run_id)
        #     log.debug(sql)
        #     cur.execute(sql)
        #     conn.commit()

    except Exception, e:
        log.error('error ending run')
        raise


def start_log(run_id, node, node_name, node_key, parent_key, node_status):
    sql = "INSERT INTO job.run_log " \
          "(run_id,node,node_name,node_key,parent_key,node_status,group_status,season,season_type,started_dtm) " \
          "VALUES " \
          "({0},'{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}')".format(
        run_id
        , node
        , str(node_name).replace("'", "''")
        , node_key
        , parent_key
        , node_status
        , 'IN PROGRESS'
        , g_season
        , g_season_type
        , datetime.datetime.now())

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


def end_log(run_id, node, key, status, group_status, cnt):
    if status is not None:
        sql = "UPDATE job.run_log " \
              "  SET  node_status = '{0}' " \
              "      ,group_status = '{1}' " \
              "      ,end_dtm = '{2}' " \
              "      ,measure_count = {3} " \
              " WHERE run_id = {4} " \
              "   AND node = '{5}' " \
              "   AND node_key = '{6}' ".format(status, group_status, datetime.datetime.now(), cnt, run_id, node, key)
    else:
        sql = "UPDATE job.run_log " \
              "  SET  group_status = '{0}' " \
              "      ,end_dtm = '{1}' " \
              "      ,measure_count = {2} " \
              " WHERE run_id = {3} " \
              "   AND node = '{4}' " \
              "   AND node_key = '{5}' ".format(group_status, datetime.datetime.now(), cnt, run_id, node, key)

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


def get_season_dates():
    sql = "SELECT start_date, end_date, is_current " \
          "  FROM job.season " \
          " WHERE season = '{0}'" \
          "   AND season_type = '{1}'".format(g_season, g_season_type)

    cur = conn.cursor()
    cur.execute(sql)
    dt = cur.fetchone()
    return [dt[0], dt[1], dt[2]]


def get_measures(node):
    cur = conn.cursor()

    sql = "SELECT node, endpoint, measure, table_name, active, measure_type, " \
          "       measure_category, comments, available_year, pk, measure_scope " \
          "  FROM job.node " \
          " WHERE node = '{0}' " \
          "   AND active = True " \
          "   AND available_year <= split_part('{1}','-',1)::integer " \
          " ORDER BY endpoint, measure, table_name".format(node, g_season)

    cur.execute(sql)
    return cur


# def s_to_sql(df,params):
#   df.columns = map(unicode.lower, df.columns)
#   df.rename(columns={'to':'tov'},inplace=True)
#   df.rename(columns={'stats_value':'_stats_value'},inplace=True)
#   df['_season'] = g_season
#   df['_season_type'] = g_season_type
#   df['_create_date'] = cmn.now()
#   df.to_sql(name=params['table_name'],con=engine,schema='lnd',if_exists='append',index=False)

def check_db_column(df,table_name):
    cur = conn.cursor()
    sql = "SELECT column_name " \
          "  FROM information_schema.columns " \
          " WHERE table_schema = 'lnd' " \
          "   AND table_name='{0}' ".format(table_name)

    cur.execute(sql)
    db_cols = [row[0] for row in cur]
    for df_col in df.columns:
        if df_col not in db_cols:
            log.warning('new column {0} found in NBA stats that doesn''t exists in landing table {1}'.format(df_col,table_name))
            log.warning('attempting to add that column to database')
            df_data_type = str(df[df_col].dtype)
            if df_data_type == 'int64': 
                data_type = 'integer'
            elif df_data_dtpe == 'float64':
                data_type = 'float'

            alter_sql = " ALTER TABLE lnd.{0} ADD COLUMN {1} {2} ".format(table_name,df_col,data_type)
            log.warning(alter_sql)
            cur.execute(alter_sql)
            conn.commit()
            break
    
def p_to_sql(df, params):
    df.columns = map(unicode.lower, df.columns)
    df.rename(columns={'to': 'tov'}, inplace=True)
    df.rename(columns={'stats_value': '_stats_value'}, inplace=True)
    check_db_column(df, params['table_name'])
    df['_player_id'] = params['player_id']
    df['_team_id'] = params['team_id']
    df['_season'] = g_season
    df['_season_type'] = g_season_type
    df['_create_date'] = datetime.datetime.now()
    df['_run_id'] = g_run_id
    df.to_sql(name=params['table_name'], con=engine, schema='lnd', if_exists='append', index=False)


def g_to_sql(df, params):
    df.columns = map(unicode.lower, df.columns)
    df.rename(columns={'to': 'tov'}, inplace=True)
    check_db_column(df, params['table_name'])
    df['_game_id'] = params['game_id']
    df['_season'] = g_season
    df['_season_type'] = g_season_type
    df['_create_date'] = datetime.datetime.now()
    df['_run_id'] = g_run_id
    df.to_sql(name=params['table_name'], con=engine, schema='lnd', if_exists='append', index=False)


def t_to_sql(df, params):
    df.columns = map(unicode.lower, df.columns)
    df.rename(columns={'to': 'tov'}, inplace=True)
    check_db_column(df, params['table_name'])
    df['_team_id'] = params['team_id']
    df['_season'] = g_season
    df['_season_type'] = g_season_type
    df['_create_date'] = datetime.datetime.now()
    df['_run_id'] = g_run_id
    df.to_sql(name=params['table_name'], con=engine, schema='lnd', if_exists='append', index=False)


def m_to_sql(df, params):
    df.columns = map(unicode.lower, df.columns)
    df.rename(columns={'to': 'tov'}, inplace=True)
    check_db_column(df, params['table_name'])
    df['_team_id'] = params['team_id']
    df['_season'] = params['season']
    df['_create_date'] = datetime.datetime.now()
    df['_run_id'] = g_run_id
    df.to_sql(name=params['table_name'], con=engine, schema='lnd', if_exists='append', index=False)


def s_to_sql(df, params):
    df.columns = map(unicode.lower, df.columns)
    df.rename(columns={'to': 'tov'}, inplace=True)
    check_db_column(df, params['table_name'])
    df['_season'] = g_season
    df['_season_type'] = g_season_type
    df['_create_date'] = datetime.datetime.now()
    df['_run_id'] = g_run_id
    df.to_sql(name=params['table_name'], con=engine, schema='lnd', if_exists='append', index=False)


def refresh_mviews():
    sql = "SELECT job.refresh_mviews()"
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

def refresh_mviews():
    cur = conn.cursor()
    cur.execute('SELECT job.refresh_mviews()')
    conn.commit()


def refresh_rpt_mviews():
    cur = conn.cursor()
    cur.execute('SELECT job.refresh_dim_player()')
    cur.execute('SELECT job.refresh_dim_game()')
    cur.execute('SELECT ')

    conn.commit()


def get_team_abbrv(team_id):
    sql = "SELECT DISTINCT team_abbrv FROM lnd.team WHERE team_id = {0}".format(team_id)
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchone()[0]


valid_seasons = ['2016-17', '2015-16', '2014-15', '2013-14', '2012-13', '2011-12', '2010-11', '2009-10', '2008-09',
                 '2007-08', '2006-07', '2005-06']

current_season = '2016-17'

nba_home = os.path.dirname(__file__)
data_folder = os.path.join(nba_home,'data') 
log_folder = os.path.join(nba_home,'log') 
log_file = os.path.join(log_folder,'nba_daily.log')

# Database
# ==============================================================================
conn = pg.connect(database="nba", user="ictsh", password="gyerli", host="aws-srv-1", port="5432")
engine = create_engine('postgresql://ictsh:gyerli@aws-srv-1:5432/nba')

# Logger
# ==============================================================================
log = logging.getLogger('go')
log.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
# fh = logging.FileHandler(log_file)
# fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

th = TimedRotatingFileHandler(log_file,
                              when="midnight",
                              interval=1,
                              backupCount=5)

th.setLevel(logging.DEBUG)

# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
ch.setFormatter(formatter)
# fh.setFormatter(formatter)
th.setFormatter(formatter)

# add the handlers to logger
log.addHandler(ch)
# log.addHandler(fh)
log.addHandler(th)

global g_season
global g_season_type
global g_season_start_date
global g_season_end_date
global g_run_id
