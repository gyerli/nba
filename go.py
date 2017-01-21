#!/usr/bin/python

import sys
import nba_py
import datetime
import time
import argparse
import inspect
import traceback
import requests
import json
import pandas as pd

# from nba_py import player as _player
from nba_py import constants as _constants
from nba_py import shotchart as _shotchart

import gamev2 as _game
import teamv2 as _team
import playerv2 as _player
import common as c


def update_hustle_stats():
    c.log.info('updating player hustle stats'.center(80, '+'))
    endpoint = _player.PlayerHustleStats(season=g_season, season_type=g_season_type)
    df_hustle = endpoint.overall()

    df_hustle.columns = map(unicode.lower, df_hustle.columns)
    c.check_db_column(df_hustle, 'player_hustle_stats')
    df_hustle['_season'] = g_season
    df_hustle['_season_type'] = g_season_type
    df_hustle['_create_date'] = datetime.datetime.now()

    df_hustle.to_sql(name='player_hustle_stats', con=c.engine, schema='lnd', if_exists='append', index=False)

    sql = 'REFRESH MATERIALIZED VIEW lnd.mvw_player_hustle_stats'
    cur = c.conn.cursor()
    cur.execute(sql)
    c.conn.commit()

    c.log.info('updating team hustle stats'.center(80, '+'))
    endpoint = _team.HustleStatsTeam(season=g_season, season_type=g_season_type)
    df_hustle_team = endpoint.overall()

    df_hustle_team.columns = map(unicode.lower, df_hustle_team.columns)
    c.check_db_column(df_hustle_team, 'team_hustle_stats')
    df_hustle_team['_season'] = g_season
    df_hustle_team['_season_type'] = g_season_type
    df_hustle_team['_create_date'] = datetime.datetime.now()

    df_hustle_team.to_sql(name='team_hustle_stats', con=c.engine, schema='lnd', if_exists='append', index=False)

    sql = 'REFRESH MATERIALIZED VIEW lnd.mvw_team_hustle_stats'
    cur = c.conn.cursor()
    cur.execute(sql)
    c.conn.commit()


def get_player_news():
    url = 'http://stats-prod.nba.com/wp-json/statscms/v1/rotowire/player/'

    response = requests.get(url)
    data = json.loads(response.text)

    news = data['ListItems']
    df_news = pd.DataFrame(news)
    df_news.columns = map(unicode.lower, df_news.columns)
    c.check_db_column(df_news, 'player_news')
    df_news['_season'] = g_season
    df_news['_season_type'] = g_season_type
    df_news['_create_date'] = datetime.datetime.now()

    df_news.to_sql(name='player_news', con=c.engine, schema='lnd', if_exists='append', index=False)

    sql = 'REFRESH MATERIALIZED VIEW lnd.mvw_player_news'
    cur = c.conn.cursor()
    cur.execute(sql)
    c.conn.commit()


def get_players_from_game(game_id, team_id):
    sql = "SELECT DISTINCT player_id, player_name, team_id, team_abbreviation " \
          "  FROM lnd.game_player_stats " \
          " WHERE game_id = '{0}' " \
          "   AND team_id = '{1}' " \
          " ORDER BY player_name ".format(game_id, team_id)

    c.log.debug(sql)
    cur = c.conn.cursor()
    cur.execute(sql)
    return cur


def get_player_name(player_id):
    sql = "SELECT player_name " \
          "  FROM lnd.game_player_stats " \
          " WHERE player_id = '{id}' " \
          " LIMIT 1 ".format(id=player_id)

    # c.log.debug(sql)
    cur = c.conn.cursor()
    cur.execute(sql)
    if cur.rowcount > 0:
        return cur.fetchone()[0]
    else:
        return 'Unknown Player (for now)'


def update_schedule():
    c.log.info('updating schedule'.center(80, '#'))
    start_dt = g_season_start_date
    end_dt = g_season_end_date

    cur = c.conn.cursor()

    # find if any game processed and finished for this season
    sql = "SELECT " \
          "  MAX(to_date(split_part(game_date_est, 'T', 1), 'YYYY-MM-DD')) " \
          "FROM lnd.mvw_schedule_game_header " \
          "WHERE game_status_text = 'Final' " \
          "  AND _season = '{0}' " \
          "  AND _season_type = '{1}' ".format(g_season, g_season_type)

    c.log.debug(sql)
    cur.execute(sql)
    last_processed_game_dt = cur.fetchone()[0]

    # Is there any game in the past where it's status is not final
    # some late games could be still in progress at the time of scheduled pull
    sql = "SELECT " \
          "  MIN(to_date(split_part(game_date_est, 'T', 1), 'YYYY-MM-DD')) " \
          "FROM lnd.mvw_schedule_game_header " \
          "WHERE to_date(split_part(game_date_est, 'T', 1), 'YYYY-MM-DD') < date(now()) " \
          "  AND game_status_text <> 'Final' " \
          "  AND _season = '{0}' " \
          "  AND _season_type = '{1}' ".format(g_season, g_season_type)

    c.log.debug(sql)
    cur.execute(sql)
    not_processed_game_dt = cur.fetchone()[0]

    # find if any game processed for this season even if it is not finished
    sql = "SELECT " \
          "  MAX(to_date(split_part(game_date_est, 'T', 1), 'YYYY-MM-DD')) " \
          "FROM lnd.mvw_schedule_game_header " \
          "WHERE _season = '{0}' " \
          "  AND _season_type = '{1}' ".format(g_season, g_season_type)

    c.log.debug(sql)
    cur.execute(sql)
    last_game_dt = cur.fetchone()[0]
    if last_game_dt is None:
        last_game_dt = g_season_start_date - datetime.timedelta(days=1)

    if last_processed_game_dt is not None:
        c.log.info('found dates has already been processed for this season')
        c.log.info('last processed schedule date: {0}'.format(
            last_processed_game_dt.strftime("%Y-%m-%d")))
        if not_processed_game_dt is not None:
            c.log.warning('found game(s) in the past where it''s status is not final')
            start_dt = not_processed_game_dt
        else:
            start_dt = last_processed_game_dt + datetime.timedelta(days=1)

        if g_is_current_season:
            end_dt = datetime.date.today()
        else:
            end_dt = g_season_end_date
            # remove below after testing
            # end_dt = datetime.date(2016, 10, 26)

    if g_schedule:
        # this is possibly requested when future games need to be processed
        # but not sure
        c.log.info('full schedule requested')
        c.log.info('getting all dates until the end of season')
        start_dt = last_game_dt + datetime.timedelta(days=1)
        if start_dt > g_season_end_date:
            c.log.info('seems like we already pulled all the game dates')
            start_dt = g_season_end_date
        end_dt = g_season_end_date

    if start_dt >= end_dt:
        c.log.info('no need to process schedule pull')
        c.log.info('completed schedule'.center(80, '-'))
        return

    c.log.info('adjusted start date: {0}'.format(start_dt.strftime("%Y-%m-%d")))
    c.log.info('adjusted end date: {0}'.format(end_dt.strftime("%Y-%m-%d")))

    while start_dt <= end_dt:
        c.log.info('getting games for the date => {0}'.format(start_dt.strftime("%Y%m%d")))
        c.start_log(run_id=g_run_id, node='schedule', node_name=start_dt.strftime("%Y%m%d"),
                    node_key=start_dt.strftime("%Y%m%d"), parent_key=None, node_status='IN PROGRESS')

        measures = c.get_measures('schedule')
        for measure in measures.fetchall():
            m = c.reg(measures, measure)
            c.log.debug('running schedule endpoint => {0}, measure => {1}'.format(
                m.endpoint, m.measure))
            try:
                endpoint = getattr(nba_py, m.endpoint)(year=start_dt.year, month=start_dt.month, day=start_dt.day)
                df = getattr(endpoint, m.measure)()
                s_params = {'table_name': m.table_name}
                c.s_to_sql(df, s_params)
            except (Exception, KeyboardInterrupt):
                c.log.error('error processing measure in {0}'.format(inspect.stack()[0][3]))
                c.log.error(traceback.format_exc())
                c.end_log(run_id=g_run_id, node='schedule', key=start_dt.strftime("%Y%m%d"), status='FAILED',
                          group_status='N/A', cnt=0)
                raise Exception('Error processing schedule: {0}'.format(start_dt.strftime("%Y%m%d")))

        c.end_log(run_id=g_run_id, node='schedule', key=start_dt.strftime("%Y%m%d"), status='COMPLETED',
                  group_status='N/A', cnt=measures.rowcount)

        start_dt += datetime.timedelta(days=1)
    c.log.info('updating schedule materialized view')
    cur.execute('REFRESH MATERIALIZED VIEW lnd.mvw_schedule_game_header')
    c.log.info('completed schedule'.center(80, '-'))


def refresh_players():
    c.log.info('Updating Player list'.center(80, '#'))
    for season in c.valid_seasons:
        pl = _player.PlayerList(season=season, only_current=0)
        df = pl.info()
        df.columns = map(unicode.lower, df.columns)
        df['_season'] = season
        df['_create_date'] = datetime.datetime.now()
        df.to_sql(name='player', con=c.engine, schema='lnd', if_exists='append', index=False)


#
#   c.log.info('Updating common player information')
#   sql = "SELECT DISTINCT person_id,display_first_last,display_last_comma_first " \
#         "  FROM lnd.player " \
#         " ORDER BY display_last_comma_first "
#   cur = c.conn.cursor()
#   cur.execute(sql)
#   players = cur.fetchall()
#
#   for _p in players:
#     c.log.info('Player => {0} ({1})'.format(_p[1], _p[0]))
#     p = _player.PlayerSummary(player_id=_p[0])
#     df = p.info()
#     df.columns = map(unicode.lower, df.columns)
#     df['_create_date'] = datetime.datetime.now()
#     df.to_sql(name='player_common',con=c.engine,schema='lnd',if_exists='append',index=False)


def refresh_team_roster_coaches():
    c.log.info('updating team roster and coaches'.center(80, '-'))

    sql = "SELECT DISTINCT " \
          "    team_id, team_abbrv " \
          "  FROM lnd.team " \
          " WHERE team_abbrv IS NOT NULL"

    cur = c.conn.cursor()
    cur.execute(sql)
    teams = cur.fetchall()
    season_type = 'Regular Season'

    for season in c.valid_seasons:
        for t in teams:
            c.log.info('Updating Team => {0} ({1}) seasonal stats for season => {2}'.format(t[1], t[0], season))

            ts = _team.TeamSummary(team_id=t[0], season=season, season_type=season_type)
            tr = _team.TeamCommonRoster(team_id=t[0], season=season)

            c.m_to_sql(ts.info(), {'team_id': t[0], 'season': season, 'table_name': 'team_summary'})
            c.m_to_sql(ts.season_ranks(), {'team_id': t[0], 'season': season, 'table_name': 'team_season_ranks'})
            c.m_to_sql(tr.roster(), {'team_id': t[0], 'season': season, 'table_name': 'team_common_roster'})
            c.m_to_sql(tr.coaches(), {'team_id': t[0], 'season': season, 'table_name': 'team_coaches'})

    c.log.info('updating landing materialized views')
    c.refresh_mviews()


def get_games():
    c.log.info('getting games to be processed which has never been processed')

    sql = "SELECT " \
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
          "   AND (g.gamecode NOT LIKE '%WSTEST%' AND g.gamecode NOT LIKE '%ESTWST%' ) " \
          "   AND NOT EXISTS ( SELECT 1 FROM job.run_log rl " \
          "                     WHERE rl.node = 'game' " \
          "                       AND rl.node_key = g.game_id  " \
          "                       AND rl.node_status = 'COMPLETED' " \
          "                       AND rl.group_status = 'COMPLETED' ) " \
          " ORDER BY to_date(split_part(game_date_est, 'T', 1), 'YYYY-MM-DD') ".format(g_season, g_season_type)

    c.log.debug(sql)
    cur = c.conn.cursor()
    cur.execute(sql)
    return cur


def process_game_measures(p_game_id, p_endpoint, p_measure, p_table_name, p_type):
    c.log.debug('game:{gid} endpoint:{ep}, measure:{m}'.format(gid=p_game_id, ep=p_endpoint, m=p_measure))
    g_params = {'game_id': p_game_id, 'table_name': p_table_name}
    try:
        endpoint = getattr(_game, p_endpoint)(game_id=p_game_id)
        df = getattr(endpoint, p_measure)()
        c.g_to_sql(df, g_params)
    except (Exception, KeyboardInterrupt):
        c.log.error('error processing measure in {0}'.format(inspect.stack()[0][3]))
        c.log.error(traceback.format_exc())
        raise Exception('Error processing game: {0}'.format(p_game_id))


def process_game(game_id):
    # check if this game stats are already in
    # game stats need to be processed only once
    # if the game processed and completed do not re-pull the data
    # but if the game stats are done but teams and players might not be done
    if get_game_status(game_id=game_id) > 0:
        c.log.info('This game''s {0} stats completed before'.format(game_id))
        return 0

    _measures = c.get_measures('game')
    pool = c.ThreadPool(_measures.rowcount)
    for _measure in _measures.fetchall():
        m = c.reg(_measures, _measure)

        pool.add_task(process_game_measures, p_game_id=game_id, p_endpoint=m.endpoint, p_measure=m.measure,
                      p_table_name=m.table_name, p_type=m.measure_type)

    pool.wait_completion()

    return _measures.rowcount


def get_game_status(game_id):
    sql = "SELECT 1 " \
          "  FROM job.run_log " \
          " WHERE node = 'game' " \
          "   AND node_key = '{0}' " \
          "   AND node_status = 'COMPLETED' ".format(game_id)

    c.log.debug(sql)
    cur = c.conn.cursor()
    cur.execute(sql)
    return cur.rowcount > 0


def get_node_status(node, node_key):
    sql = "SELECT 1 " \
          "  FROM job.run_log " \
          " WHERE node = '{0}' " \
          "   AND node_key = '{1}' " \
          "   AND ( run_id = '{2}' OR date(end_dtm) = date(current_date) )" \
          "   AND season = '{3}' " \
          "   AND season_type = '{4}' " \
          "   AND node_status = 'COMPLETED' ".format(node, node_key, g_run_id, g_season, g_season_type)

    c.log.debug(sql)
    cur = c.conn.cursor()
    cur.execute(sql)
    return cur.rowcount > 0


def get_player_status(player_id, team_id):
    sql = "SELECT 1 " \
          "  FROM job.run_log " \
          " WHERE node = 'player' " \
          "   AND node_key = '{0}' " \
          "   AND parent_key = '{1}' " \
          "   AND ( run_id = '{2}' OR date(end_dtm) = date(current_date) ) " \
          "   AND season = '{3}' " \
          "   AND season_type = '{4}' " \
          "   AND node_status = 'COMPLETED' ".format(player_id, team_id, g_run_id, g_season, g_season_type)

    c.log.debug(sql)
    cur = c.conn.cursor()
    cur.execute(sql)
    return cur.rowcount > 0


def process_team_measures(p_team_id, p_endpoint, p_measure, p_table_name, p_type):
    c.log.debug('team:{tid}, endpoint:{ep}, measure:{m}, type:{mt}'.format(tid=p_team_id, ep=p_endpoint, m=p_measure,
                                                                           mt=p_type))

    try:
        if p_type == 'self':
            endpoint = getattr(_team, p_endpoint)(team_id=p_team_id, season=g_season, season_type=g_season_type)
        else:
            endpoint = getattr(_team, p_endpoint)(team_id=p_team_id, season=g_season, season_type=g_season_type,
                                                  measure_type=p_type)
        df = getattr(endpoint, p_measure)()
        t_params = {'team_id': p_team_id, 'table_name': p_table_name}
        c.t_to_sql(df, t_params)
    except (Exception, KeyboardInterrupt):
        c.log.error('error processing measure in {0}'.format(inspect.stack()[0][3]))
        c.log.error(traceback.format_exc())
        raise Exception('Error processing team: {0}'.format(p_team_id))


def process_team(team_id):
    if get_node_status('team', team_id):
        c.log.debug('This team {0} is refreshed in this session or today'.format(team_id))
        return 0

    _measures = c.get_measures('team')
    pool = c.ThreadPool(_measures.rowcount)
    for _measure in _measures.fetchall():
        m = c.reg(_measures, _measure)
        # if rotowire entry or other one a day entry then do not process
        # rotowire news retrieved every 15 minutes after 12pm through 22pm
        # hustle stats are processed at the beginning of the process
        if m.measure_type == 'pass':
            continue

        pool.add_task(process_team_measures, p_team_id=team_id, p_endpoint=m.endpoint, p_measure=m.measure,
                      p_table_name=m.table_name, p_type=m.measure_type)

    pool.wait_completion()

    return _measures.rowcount


def process_player_measures(p_player_id, p_team_id, p_endpoint, p_measure, p_table_name, p_type, p_category):
    player_name = get_player_name(p_player_id)
    c.log.debug(
        'player:{pid} endpoint:{ep}, measure:{m}, type:{mt}'.format(ep=p_endpoint, m=p_measure, pid=player_name,
                                                                    mt=p_type))

    try:
        if p_type == 'self' and p_category == '1':
            endpoint = getattr(_player, p_endpoint)(player_id=p_player_id, season=g_season, season_type=g_season_type)
        elif p_type == 'self':
            endpoint = getattr(_player, p_endpoint)(player_id=p_player_id)
        else:
            endpoint = getattr(_player, p_endpoint)(player_id=p_player_id, season=g_season, season_type=g_season_type,
                                                    measure_type=p_type)
        df = getattr(endpoint, p_measure)()
        p_params = {'player_id': p_player_id, 'team_id': p_team_id, 'table_name': p_table_name}
        c.p_to_sql(df, p_params)

    except (Exception, KeyboardInterrupt):
        c.log.error('error processing measure in {0}'.format(inspect.stack()[0][3]))
        c.log.error(traceback.format_exc())
        raise Exception('Error processing player: {0}'.format(p_player_id))


def process_player(player_id, team_id):
    player_name = get_player_name(player_id)

    c.start_log(run_id=g_run_id, node='player', node_name=player_name, node_key=player_id,
                parent_key=team_id, node_status='IN PROGRESS')
    _measures = c.get_measures('player')
    for _measure in _measures.fetchall():
        m = c.reg(_measures, _measure)
        # if rotowire entry or other one a day entry then do not process
        # rotowire news retrieved every 15 minutes after 12pm through 22pm
        # hustle stats are processed at the beginning of the process
        if m.measure_type == 'pass':
            continue

        process_player_measures(p_player_id=player_id, p_team_id=team_id, p_endpoint=m.endpoint, p_measure=m.measure,
                                p_table_name=m.table_name, p_type=m.measure_type, p_category=m.measure_category)

    c.end_log(run_id=g_run_id, node='player', key=player_id, status='COMPLETED',
              group_status='N/A', cnt=_measures.rowcount)

    c.log.debug('total {0} measures processed'.format(_measures.rowcount))
    return _measures.rowcount


def find_player_team_season(player_id, season):
    p_sql = "SELECT " \
            " team_id " \
            "FROM (" \
            "SELECT " \
            "   player_id, season, team_id " \
            "  ,row_number() over(PARTITION BY player_id, season ORDER BY rec_start_date desc) rn " \
            "  FROM rpt.dim_player_team_history ) a " \
            "WHERE a.rn = 1 " \
            "  AND a.player_id = '{pid}' " \
            "  AND a.season = '{s}' ".format(pid=player_id, s=season)

    # c.log.debug(p_sql)
    p_cur = c.conn.cursor()
    p_cur.execute(p_sql)
    return p_cur.fetchone()[0]


def process_single_measure(p_measure):
    sql = "SELECT node, endpoint, measure, measure_type, measure_category, available_year, table_name " \
          "  FROM job.node " \
          " WHERE table_name = '{m}'" \
          "   AND active = True " \
          "   AND available_year <= split_part('{s}','-',1)::integer ".format(m=p_measure, s=g_season)

    cur = c.conn.cursor()
    cur.execute(sql)
    if cur.rowcount != 1:
        c.log.error('Invalid measure name (table_name): {tbl} or measure is not available for this season {s}'.format(
            tbl=p_measure, s=g_season))
        sys.exit(1)

    measure = cur.fetchone()
    c.log.info(measure)

    if measure[0] == 'player':
        process_player_single_measure(p_measure=measure)


def process_player_single_measure(p_measure):
    _node = p_measure[0]
    _endpoint = p_measure[1]
    _measure = p_measure[2]
    _measure_type = p_measure[3]
    _measure_category = p_measure[4]
    _available_year = p_measure[5]
    _table_name = p_measure[6]

    thread_count = 30
    pool = c.ThreadPool(thread_count)

    d_sql = "SELECT DISTINCT player_id " \
            "  FROM lnd.game_player_stats  " \
            " WHERE _season = '{s}'".format(s=g_season)

    # c.log.info(d_sql)
    d_cur = c.conn.cursor()
    d_cur.execute(d_sql)
    players = d_cur.fetchall()
    cnt = 1
    for player in players:
        team_id = find_player_team_season(player_id=player[0], season=g_season)
        pool.add_task(process_player_measures, p_player_id=player[0], p_team_id=team_id, p_endpoint=_endpoint,
                      p_measure=_measure,
                      p_table_name=_table_name, p_type=_measure_type, p_category=_measure_category)
        if cnt >= thread_count:
            pool.wait_completion()
            cnt = 0

        cnt += 1

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
    global g_debug
    global g_schedule
    global g_measure
    global g_news
    global g_roster
    global g_players

    parser = argparse.ArgumentParser(description='Executes job and job details')
    parser.add_argument('-s', '--season', help='NBA season (e.g. 2014-15)', required=False, default=c.current_season)
    parser.add_argument('-t', '--season_type', help='Season type (R=>Regular Season, P=>Playoffs)', default='R')
    parser.add_argument('-f', '--schedule', help='Update full schedule', action='store_true')
    parser.add_argument('-r', '--roster', help='Refresh team roster and coaches', action='store_true')
    parser.add_argument('-p', '--players', help='Refresh player list', action='store_true')
    parser.add_argument('-d', '--debug', help='Debugging flag', action='store_true')
    parser.add_argument('-n', '--news', help='Get NBA fantasy news only', action='store_true')
    parser.add_argument('-m', '--measure', help='Measure in the node table (table_name)', required=False)

    args = vars(parser.parse_args())

    g_season = args['season']
    g_debug = args['debug']
    g_schedule = args['schedule']
    g_measure = args['measure']
    g_news = args['news']
    g_roster = args['roster']
    g_players = args['players']

    if args['season_type'] == 'P':
        g_season_type = _constants.SeasonType.Playoffs
    else:
        g_season_type = _constants.SeasonType.Regular

    c.g_season = g_season
    c.g_season_type = g_season_type
    c.g_measure = g_measure
    c.g_news = g_news
    c.g_roster = g_roster
    c.g_players = g_players

    dt = c.get_season_dates()

    g_season_start_date = dt[0]
    g_season_end_date = dt[1]
    g_is_current_season = dt[2]

    c.g_season_start_date = g_season_start_date
    c.g_season_end_date = g_season_end_date

    c.log.info('processing season {0} for {1}'.format(g_season, g_season_type))
    c.log.info('season start date: {0} end date: {1}'.format(
        g_season_start_date.strftime("%Y-%m-%d"),
        g_season_end_date.strftime("%Y-%m-%d")))

    g_run_id = c.start_run()
    c.g_run_id = g_run_id

    if args['roster']:
        refresh_team_roster_coaches()
        c.end_run(g_run_id, 'COMPLETED')
        sys.exit(0)

    if args['players']:
        refresh_players()
        c.end_run(g_run_id, 'COMPLETED')
        sys.exit(0)

    if args['news']:
        c.log.info('getting player rotowire news and exiting')
        get_player_news()
        c.end_run(g_run_id, 'COMPLETED')
        sys.exit(0)

    if args['measure'] is not None:
        c.log.info('processing a single measure'.center(80, '*'))
        c.log.info('measure:{m}'.format(m=args['measure']))
        process_single_measure(args['measure'])
        c.end_run(g_run_id, 'COMPLETED')
        sys.exit(0)

    update_hustle_stats()
    update_schedule()

    c.log.info('starting games'.center(80, '#'))
    games = get_games()
    for game in games.fetchall():
        g = c.reg(games, game)
        schedule_key = g.gamecode[0:8]
        c.log.info('processing game:{0}'.format(g.gamecode))
        try:  # this is game try
            c.start_log(run_id=g_run_id, node='game', node_name=g.gamecode, node_key=g.game_id,
                        parent_key=schedule_key, node_status='IN PROGRESS')

            game_measure_count = process_game(g.game_id)
            c.log.debug('total {0} measures processed'.format(game_measure_count))

            c.end_log(run_id=g_run_id, node='game', key=g.game_id, status='COMPLETED',
                      group_status='IN PROGRESS', cnt=game_measure_count)

            try:  # this is team try for home team
                home_team_name = c.get_team_abbrv(g.home_team_id)
                c.log.info('home team:{tm} id:{id}'.format(tm=home_team_name, id=g.home_team_id))
                c.start_log(run_id=g_run_id, node='team', node_name=home_team_name, node_key=g.home_team_id,
                            parent_key=g.game_id, node_status='IN PROGRESS')

                home_team_measure_count = process_team(g.home_team_id)
                c.log.debug('total {0} measures processed'.format(home_team_measure_count))
                c.end_log(run_id=g_run_id, node='team', key=g.home_team_id, status='COMPLETED',
                          group_status='N/A', cnt=home_team_measure_count)

                c.log.info('processing home team players'.center(80, '='))
                players = get_players_from_game(game_id=g.game_id, team_id=g.home_team_id)
                h_pool = c.ThreadPool(players.rowcount)
                for player in players.fetchall():
                    p = c.reg(players, player)
                    if get_player_status(p.player_id, g.home_team_id):
                        continue

                    c.log.info('player:{nm}, id:{id}, team:{tm}'.format(nm=p.player_name, tm=p.team_abbreviation,
                                                                        id=p.player_id))
                    try:  # this is home team players' try
                        h_pool.add_task(process_player, player_id=p.player_id, team_id=g.home_team_id)
                    except (Exception, KeyboardInterrupt):  # this is home team players' exception
                        c.log.error('error processing home team players:{0}'.format(g.home_team_id))
                        c.log.error(traceback.format_exc())
                        c.log.error('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
                        # here we need to fail both team and game (only group status=FAILED)
                        c.end_log(run_id=g_run_id, node='player', key=p.player_id, status='FAILED', group_status='N/A',
                                  cnt=0)
                        c.end_log(run_id=g_run_id, node='game', key=g.game_id, status=None, group_status='FAILED',
                                  cnt=0)
                        raise Exception('Error processing player')

                h_pool.wait_completion()

            except (Exception, KeyboardInterrupt):  # this is home team exception
                c.log.error('error processing home team:{0}'.format(g.home_team_id))
                c.log.error(traceback.format_exc())
                c.log.error('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
                # here we need to fail both team and game (only group status=FAILED)
                c.end_log(run_id=g_run_id, node='team', key=g.home_team_id, status='FAILED', group_status='N/A',
                          cnt=0)
                c.end_log(run_id=g_run_id, node='game', key=g.game_id, status=None, group_status='FAILED',
                          cnt=0)
                raise Exception('Error processing home team')

            try:
                visitor_team_name = c.get_team_abbrv(g.visitor_team_id)
                c.log.info('visitor team:{tm} id:{id}'.format(tm=visitor_team_name, id=g.visitor_team_id))
                c.start_log(run_id=g_run_id, node='team', node_name=visitor_team_name, node_key=g.visitor_team_id,
                            parent_key=g.game_id, node_status='IN PROGRESS')

                visitor_team_measure_count = process_team(g.visitor_team_id)
                c.log.debug('total {0} measures processed'.format(visitor_team_measure_count))
                c.end_log(run_id=g_run_id, node='team', key=g.visitor_team_id, status='COMPLETED',
                          group_status='N/A', cnt=visitor_team_measure_count)

                c.log.info('processing visitor team players'.center(80, '='))
                players = get_players_from_game(game_id=g.game_id, team_id=g.visitor_team_id)
                v_pool = c.ThreadPool(players.rowcount)
                for player in players.fetchall():
                    p = c.reg(players, player)
                    if get_player_status(p.player_id, g.visitor_team_id):
                        continue
                    c.log.info('player:{nm}, id:{id}, team:{tm}'.format(nm=p.player_name, tm=p.team_abbreviation,
                                                                        id=p.player_id))
                    try:  # this is visitor team players' try
                        v_pool.add_task(process_player, player_id=p.player_id, team_id=g.visitor_team_id)
                    except (Exception, KeyboardInterrupt):  # this is visitor team players' exception
                        c.log.error('error processing visitor team players:{0}'.format(g.visitor_team_id))
                        c.log.error(traceback.format_exc())
                        c.log.error('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
                        # here we need to fail both team and game (only group status=FAILED)
                        c.end_log(run_id=g_run_id, node='player', key=p.player_id, status='FAILED', group_status='N/A',
                                  cnt=0)
                        c.end_log(run_id=g_run_id, node='game', key=g.game_id, status=None, group_status='FAILED',
                                  cnt=0)
                        raise Exception('Error processing player')

                v_pool.wait_completion()

            except (Exception, KeyboardInterrupt):  # this is visitor team exception
                c.log.error('error processing visitor team:{0}'.format(g.visitor_team_id))
                c.log.error(traceback.format_exc())
                c.log.error('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
                # here we need to fail both team and game (only group status=FAILED)
                c.end_log(run_id=g_run_id, node='team', key=g.visitor_team_id, status='FAILED', group_status='N/A',
                          cnt=0)
                c.end_log(run_id=g_run_id, node='game', key=g.game_id, status=None, group_status='FAILED',
                          cnt=0)
                raise Exception('Error processing visitor team')

            c.end_log(run_id=g_run_id, node='game', key=g.game_id, status='COMPLETED', group_status='COMPLETED',
                      cnt=games.rowcount)
            #  at this point all the teams and players processed so game group_status can be updated as completed
        except (Exception, KeyboardInterrupt):  # this is game exception
            c.log.error('error processing game:{0}'.format(g.gamecode))
            c.log.error(traceback.format_exc())
            c.log.error('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
            # we don't need to end_log for the game. it is already been handled in team and player exceptions
            # c.end_log(run_id=g_run_id, node='game', key=g.game_id, status='FAILED', group_status='FAILED')
            c.end_run(g_run_id, 'FAILED')
            sys.exit(1)
    c.end_run(g_run_id, 'COMPLETED')
    c.log.info('updating landing materialized views'.center(80, '#'))
    c.refresh_mviews()
    c.log.info('updating reporting materialized views'.center(80, '#'))
    c.refresh_rpt_mviews()


if __name__ == '__main__':
    sys.exit(main())
