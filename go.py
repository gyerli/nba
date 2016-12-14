#!/usr/bin/python

import sys
import nba_py
import datetime
import time
import argparse
import inspect
import traceback

from nba_py import player as _player
from nba_py import constants as _constants
from nba_py import shotchart as _shotchart

import gamev2 as _game
import teamv2 as _team
import common as c


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


def update_schedule():
    c.log.info('################# updating schedule ##########################')
    start_dt = g_season_start_date
    end_dt = g_season_end_date

    cur = c.conn.cursor()

    # find if any game processed and finished for this season
    sql = "SELECT " \
          "  MAX(to_date(split_part(game_date_est, 'T', 1), 'YYYY-MM-DD')) " \
          "FROM lnd.schedule_game_header " \
          "WHERE game_status_text = 'Final' " \
          "  AND _season = '{0}' " \
          "  AND _season_type = '{1}' ".format(g_season, g_season_type)

    cur.execute(sql)
    last_processed_game_dt = cur.fetchone()[0]

    # find if any game processed for this season even it is not finished
    sql = "SELECT " \
          "  MAX(to_date(split_part(game_date_est, 'T', 1), 'YYYY-MM-DD')) " \
          "FROM lnd.schedule_game_header " \
          "WHERE _season = '{0}' " \
          "  AND _season_type = '{1}' ".format(g_season, g_season_type)

    cur.execute(sql)
    last_game_dt = cur.fetchone()[0]
    if last_game_dt is None:
        last_game_dt = g_season_start_date - datetime.timedelta(days=1)

    if last_processed_game_dt is not None:
        c.log.info('found dates has already been processed for this season')
        c.log.info('last processed schedule date: {0}'.format(
            last_processed_game_dt.strftime("%Y-%m-%d")))
        start_dt = last_processed_game_dt + datetime.timedelta(days=1)
        if g_is_current_season:
            end_dt = datetime.date.today()
        else:
            end_dt = g_season_end_date
            # remove below after testing
            # end_dt = datetime.date(2016, 10, 26)
    if g_schedule:
        c.log.info('full schedule requested')
        c.log.info('getting all dates until the end of season')
        start_dt = last_game_dt + datetime.timedelta(days=1)
        if start_dt > g_season_end_date:
            c.log.info('seems like we already pulled all the game dates')
            start_dt = g_season_end_date
        if g_is_current_season:
            end_dt = datetime.date.today()
        else:
            end_dt = g_season_end_date

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
    c.log.info('updating materialized views')
    c.refresh_mviews()
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
          " WHERE abbreviation IS NOT NULL"

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
          "   AND g.gamecode NOT LIKE '%WSTEST%' " \
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


def process_game(game_id):
    # check if this game stats are already in
    # game stats need to be processed only once
    # if the game processed and completed do not re-pull the data
    # but if the game stats are done but teams and players might not be done
    if get_game_status(game_id=game_id) > 0:
        c.log.info('This game''s {0} stats completed before'.format(game_id))
        return 0

    _measures = c.get_measures('game')
    for _measure in _measures.fetchall():
        m = c.reg(_measures, _measure)
        c.log.debug('running game endpoint => {0}, measure => {1}'.format(m.endpoint, m.measure))
        g_params = {'game_id': game_id, 'table_name': m.table_name}
        try:
            endpoint = getattr(_game, m.endpoint)(game_id=game_id)
            # if m.measure == 'season_series':
            #     raise Exception('Debugging exceptions "season series"')
            df = getattr(endpoint, m.measure)()
            c.g_to_sql(df, g_params)
        except (Exception, KeyboardInterrupt):
            c.log.error('error processing measure in {0}'.format(inspect.stack()[0][3]))
            c.log.error(traceback.format_exc())
            raise Exception('Error processing game: {0}'.format(game_id))
    return _measures.rowcount


def get_game_status(game_id):
    sql = "SELECT 1 " \
          "  FROM job.run_log " \
          " WHERE node = 'game' " \
          "   AND node_key = '{0}' " \
          "   AND node_status = 'COMPLETED' ".format(game_id)

    cur = c.conn.cursor()
    cur.execute(sql)
    return cur.rowcount > 0


def get_node_status(node, node_key):
    sql = "SELECT 1 " \
          "  FROM job.run_log " \
          " WHERE node = '{0}' " \
          "   AND node_key = '{1}' " \
          "   AND run_id = '{2}' " \
          "   AND node_status = 'COMPLETED' ".format(node, node_key, g_run_id)

    cur = c.conn.cursor()
    cur.execute(sql)
    return cur.rowcount > 0


def get_player_status(player_id, team_id):
    sql = "SELECT 1 " \
          "  FROM job.run_log " \
          " WHERE node = 'player' " \
          "   AND node_key = '{0}' " \
          "   AND parent_key = '{1}' " \
          "   AND run_id = '{2}' " \
          "   AND node_status = 'COMPLETED' ".format(player_id, team_id, g_run_id)

    cur = c.conn.cursor()
    cur.execute(sql)
    return cur.rowcount > 0


def process_team(team_id):
    if get_node_status('team', team_id):
        c.log.debug('This team is refreshed in this session {0}'.format(team_id))
        return 0
    _measures = c.get_measures('team')
    for _measure in _measures.fetchall():
        m = c.reg(_measures, _measure)
        c.log.debug('running team endpoint => {0}, measure => {1}'.format(m.endpoint, m.measure))

        try:
            if m.measure_type == 'self':
                endpoint = getattr(_team, m.endpoint)(team_id=team_id, season=g_season, season_type=g_season_type)
            else:
                endpoint = getattr(_team, m.endpoint)(team_id=team_id, season=g_season, season_type=g_season_type,
                                                      measure_type=m.measure_type)
            df = getattr(endpoint, m.measure)()
            t_params = {'team_id': team_id, 'table_name': m.table_name}
            c.t_to_sql(df, t_params)
        except (Exception, KeyboardInterrupt):
            c.log.error('error processing measure in {0}'.format(inspect.stack()[0][3]))
            c.log.error(traceback.format_exc())
            raise Exception('Error processing team: {0}'.format(team_id))

    return _measures.rowcount


def process_player(player_id, team_id):
    _measures = c.get_measures('player')
    for _measure in _measures.fetchall():
        m = c.reg(_measures, _measure)
        c.log.debug('running player endpoint => {0}, measure => {1}'.format(m.endpoint, m.measure))

        try:
            if m.measure_type == 'self' and m.measure_category == '1':
                endpoint = getattr(_player, m.endpoint)(player_id=player_id, season=g_season, season_type=g_season_type)
            elif m.measure_type == 'self':
                endpoint = getattr(_player, m.endpoint)(player_id=player_id)
            else:
                endpoint = getattr(_player, m.endpoint)(player_id=player_id, season=g_season, season_type=g_season_type,
                                                        measure_type=m.measure_type)
            df = getattr(endpoint, m.measure)()
            p_params = {'player_id': player_id, 'team_id': team_id, 'table_name': m.table_name}
            c.p_to_sql(df, p_params)

        except (Exception, KeyboardInterrupt):
            c.log.error('error processing measure in {0}'.format(inspect.stack()[0][3]))
            c.log.error(traceback.format_exc())
            raise Exception('Error processing player: {0}'.format(player_id))

    return _measures.rowcount


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

    parser = argparse.ArgumentParser(description='Executes job and job details')
    parser.add_argument('-s', '--season', help='NBA season (e.g. 2014-15)', required=False, default=c.current_season)
    parser.add_argument('-t', '--season_type', help='Season type (R=>Regular Season, P=>Playoffs)', default='R')
    parser.add_argument('-f', '--schedule', help='Update full schedule', action='store_true')
    parser.add_argument('-r', '--roster', help='Refresh team roster and coaches', action='store_true')
    parser.add_argument('-p', '--players', help='Refresh player list', action='store_true')
    parser.add_argument('-d', '--debug', help='Debugging flag', action='store_true')

    args = vars(parser.parse_args())

    g_season = args['season']
    g_debug = args['debug']
    g_schedule = args['schedule']

    if args['season_type'] == 'P':
        g_season_type = _constants.SeasonType.Playoffs
    else:
        g_season_type = _constants.SeasonType.Regular

    c.g_season = g_season
    c.g_season_type = g_season_type

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

    update_schedule()

    if args['roster']:
        refresh_team_roster_coaches()
        sys.exit(0)

    if args['players']:
        refresh_players()
        sys.exit(0)

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
                c.log.info('processing home team:{0} ({1})'.format(home_team_name, g.home_team_id))
                c.start_log(run_id=g_run_id, node='team', node_name=home_team_name, node_key=g.home_team_id,
                            parent_key=g.game_id, node_status='IN PROGRESS')

                # if g.home_team_id  == 1610612757:
                #     raise Exception('Debugging team completion')

                home_team_measure_count = process_team(g.home_team_id)
                c.log.debug('total {0} measures processed'.format(home_team_measure_count))
                c.end_log(run_id=g_run_id, node='team', key=g.home_team_id, status='COMPLETED',
                          group_status='N/A', cnt=home_team_measure_count)

                c.log.info('processing home team players')
                players = get_players_from_game(game_id=g.game_id, team_id=g.home_team_id)
                for player in players.fetchall():
                    p = c.reg(players, player)
                    if get_player_status(p.player_id, g.home_team_id):
                        continue

                    c.log.info('Processing player {0} ({1}) id=>{2}'.format(p.player_name, p.team_abbreviation,
                                                                            p.player_id))
                    c.start_log(run_id=g_run_id, node='player', node_name=p.player_name, node_key=p.player_id,
                                parent_key=g.home_team_id, node_status='IN PROGRESS')
                    try:  # this is home team players' try
                        player_measure_count = process_player(player_id=p.player_id, team_id=g.home_team_id)
                        c.log.debug('total {0} measures processed'.format(player_measure_count))
                        # if p.player_id == 201142:
                        #     raise Exception('Debugging game completion')

                        c.end_log(run_id=g_run_id, node='player', key=p.player_id, status='COMPLETED',
                                  group_status='N/A', cnt=player_measure_count)
                        if g_debug:
                            time.sleep(0.25)

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
                c.log.info('processing visitor team:{0} ({1})'.format(visitor_team_name, g.visitor_team_id))
                c.start_log(run_id=g_run_id, node='team', node_name=visitor_team_name, node_key=g.visitor_team_id,
                            parent_key=g.game_id, node_status='IN PROGRESS')

                visitor_team_measure_count = process_team(g.visitor_team_id)
                c.log.debug('total {0} measures processed'.format(visitor_team_measure_count))
                c.end_log(run_id=g_run_id, node='team', key=g.visitor_team_id, status='COMPLETED',
                          group_status='N/A', cnt=visitor_team_measure_count)

                c.log.info('processing visitor team players')
                players = get_players_from_game(game_id=g.game_id, team_id=g.visitor_team_id)
                for player in players.fetchall():
                    p = c.reg(players, player)
                    if get_player_status(p.player_id, g.visitor_team_id):
                        continue

                    c.log.info('Processing player {0} ({1}) id=>{2}'.format(p.player_name, p.team_abbreviation,
                                                                            p.player_id))
                    c.start_log(run_id=g_run_id, node='player', node_name=p.player_name, node_key=p.player_id,
                                parent_key=g.visitor_team_id, node_status='IN PROGRESS')

                    try:  # this is visitor team players' try
                        player_measure_count = process_player(player_id=p.player_id, team_id=g.home_team_id)
                        c.log.debug('total {0} measures processed'.format(player_measure_count))
                        c.end_log(run_id=g_run_id, node='player', key=p.player_id, status='COMPLETED',
                                  group_status='N/A', cnt=player_measure_count)
                        if g_debug:
                            time.sleep(0.25)

                    except (Exception, KeyboardInterrupt):  # this is visitor team players' exception
                        c.log.error('error processing home team players:{0}'.format(g.home_team_id))
                        c.log.error(traceback.format_exc())
                        c.log.error('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
                        # here we need to fail both team and game (only group status=FAILED)
                        c.end_log(run_id=g_run_id, node='player', key=p.player_id, status='FAILED', group_status='N/A',
                                  cnt=0)
                        c.end_log(run_id=g_run_id, node='game', key=g.game_id, status=None, group_status='FAILED',
                                  cnt=0)
                        raise Exception('Error processing player')

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
            # we don't need to end_log for the game. it is already been handeled in team and player exceptions
            # c.end_log(run_id=g_run_id, node='game', key=g.game_id, status='FAILED', group_status='FAILED')
            c.end_run(g_run_id, 'FAILED')
            sys.exit(1)
    c.end_run(g_run_id, 'COMPLETED')


if __name__ == '__main__':
    sys.exit(main())
