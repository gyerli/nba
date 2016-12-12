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
    start_dt = g_season_start_date
    end_dt = g_season_end_date

    cur = c.conn.cursor()

    # find if any game processed for this season
    sql = "SELECT " \
          "  MAX(to_date(split_part(game_date_est, 'T', 1), 'YYYY-MM-DD')) " \
          "FROM lnd.schedule_game_header " \
          "WHERE game_status_text = 'Final' " \
          "  AND _season = '{0}' " \
          "  AND _season_type = '{1}' ".format(g_season, g_season_type)

    try:
        cur.execute(sql)
        last_processed_game_dt = cur.fetchone()[0]
        if last_processed_game_dt is not None:
            c.log.info('found processed games for this season')
            c.log.info('last processed schedule date: {0}'.format(
                last_processed_game_dt.strftime("%Y-%m-%d")))
            start_dt = last_processed_game_dt + datetime.timedelta(days=1)
            end_dt = datetime.date.today()
            # remove below after testing
            end_dt = datetime.date(2016, 10, 26)

        c.log.info('adjusted start date: {0}'.format(start_dt.strftime("%Y-%m-%d")))
        c.log.info('adjusted end date: {0}'.format(end_dt.strftime("%Y-%m-%d")))

        while start_dt <= end_dt:
            c.log.info('getting games for the date => {0}'.format(start_dt.strftime("%Y%m%d")))
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
            c.log.info('Updating Team => {0} ({1}) seasonal stats for season => {2}'.format(t[1], t[0], season))

            ts = _team.TeamSummary(team_id=t[0], season=season, season_type=season_type)
            tr = _team.TeamCommonRoster(team_id=t[0], season=season)

            c.m_to_sql(ts.info(), {'team_id': t[0], 'season': season, 'table_name': 'team_summary'})
            c.m_to_sql(ts.season_ranks(), {'team_id': t[0], 'season': season, 'table_name': 'team_season_ranks'})
            c.m_to_sql(tr.roster(), {'team_id': t[0], 'season': season, 'table_name': 'team_common_roster'})
            c.m_to_sql(tr.coaches(), {'team_id': t[0], 'season': season, 'table_name': 'team_coaches'})


def get_games():
    c.log.info('getting games to be processed')

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
          "   AND NOT EXISTS ( SELECT 1 FROM job.run_log rl " \
          "                     WHERE rl.node = 'game' " \
          "                       AND rl.node_key = g.game_id  " \
          "                       AND rl.node_status = 'COMPLETED' " \
          "                       AND rl.group_status = 'COMPLETED' ) ".format(g_season, g_season_type)

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
        c.log.info('This game''s stats completed before {0}'.format(game_id))
        return 0

    _measures = c.get_measures('game')
    for _measure in _measures.fetchall():
        m = c.reg(_measures, _measure)
        c.log.debug('running game endpoint => {0}, measure => {1}'.format(m.endpoint, m.measure))
        g_params = {'game_id': game_id, 'table_name': m.table_name}
        try:
            pass
            endpoint = getattr(_game, m.endpoint)(game_id=game_id)
            # if m.measure == 'season_series':
            #     raise Exception('Debugging exceptions "season series"')
            df = getattr(endpoint, m.measure)()
            c.g_to_sql(df, g_params)
        except Exception, e:
            c.log.error('error processing measure in {0}'.format(inspect.stack()[0][3]))
            c.log.error(e)
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


def process_team(team_id):
    if get_node_status('team', team_id):
        c.log.info('This team is refreshed in this session {0}'.format(team_id))
        return 0
    _measures = c.get_measures('team')
    for _measure in _measures.fetchall():
        m = c.reg(_measures, _measure)
        c.log.debug('running team endpoint => {0}, measure => {1}'.format(m.endpoint, m.measure))

        try:
            pass
            # if m.measure_type == 'self':
            #     endpoint = getattr(_team, m.endpoint)(team_id=team_id, season=g_season, season_type=g_season_type)
            # else:
            #     endpoint = getattr(_team, m.endpoint)(team_id=team_id, season=g_season, season_type=g_season_type,
            #                                           measure_type=m.measure_type)
            # df = getattr(endpoint, m.measure)()
            # t_params = {'team_id': team_id, 'table_name': m.table_name}
            # c.t_to_sql(df, t_params)
        except Exception, e:
            c.log.error('error processing measure in {0}'.format(inspect.stack()[0][3]))
            c.log.error(e)
            raise Exception('Error processing team: {0}'.format(team_id))

    return _measures.rowcount


def process_player(player_id):
    if get_node_status('player', player_id):
        c.log.info('This player is refreshed in this session {0}'.format(player_id))
        return 0
    _measures = c.get_measures('player')
    for _measure in _measures.fetchall():
        m = c.reg(_measures, _measure)
        c.log.debug('running player endpoint => {0}, measure => {1}'.format(m.endpoint, m.measure))

        try:
            pass
            # if m.measure_type == 'self' and m.measure_category == '1':
            #     endpoint = getattr(_player, m.endpoint)(player_id=player_id, season=g_season, season_type=g_season_type)
            # elif m.measure_type == 'self':
            #     endpoint = getattr(_player, m.endpoint)(player_id=player_id)
            # else:
            #     endpoint = getattr(_player, m.endpoint)(player_id=player_id, season=g_season, season_type=g_season_type,
            #                                             measure_type=m.measure_type)
            # df = getattr(endpoint, m.measure)()
            # p_params = {'player_id': player_id, 'team_id': m.task_team, 'table_name': m.table_name}
            # c.p_to_sql(df, p_params)

        except Exception, e:
            c.log.error('error processing measure in {0}'.format(inspect.stack()[0][3]))
            c.log.error(e)
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

    parser = argparse.ArgumentParser(description='Executes job and job details')
    parser.add_argument('-s', '--season', help='NBA season (e.g. 2014-15)', required=False, default=c.current_season)
    parser.add_argument('-t', '--season_type', help='Season type (R=>Regular Season, P=>Playoffs)', default='R')
    parser.add_argument('-e', '--schedule', help='Update schedule', action='store_true')
    parser.add_argument('-r', '--roster', help='Refresh team roster and coaches', action='store_true')

    args = vars(parser.parse_args())

    g_season = args['season']

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

    update_schedule()
    if args['roster']: refresh_team_roster_coaches()

    g_run_id = c.start_run()

    games = get_games()
    for game in games.fetchall():
        g = c.reg(games, game)
        schedule_key = g.gamecode[0:8]
        c.log.info('processing game:{0}'.format(g.gamecode))
        try:  # this is game try
            c.start_log(run_id=g_run_id, node='game', node_key=g.game_id, parent_key=schedule_key,
                        node_status='IN PROGRESS')

            game_measure_count = process_game(g.game_id)

            c.end_log(run_id=g_run_id, node='game', key=g.game_id, status='COMPLETED',
                      group_status='IN PROGRESS')

            try:  # this is team try for home team
                c.log.info('processing home team:{0}'.format(g.home_team_id))
                c.start_log(run_id=g_run_id, node='team', node_key=g.home_team_id, parent_key=g.game_id,
                            node_status='IN PROGRESS')

                home_team_measure_count = process_team(g.home_team_id)
                c.end_log(run_id=g_run_id, node='team', key=g.home_team_id, status='COMPLETED',
                          group_status='N/A')
                try:  # this is home team players' try
                    c.log.info('processing home team players')
                    players = get_players_from_game(game_id=g.game_id, team_id=g.home_team_id)
                    for player in players.fetchall():
                        p = c.reg(players, player)
                        c.log.info('Processing player {0} ({1}) id=>{2}'.format(p.player_name, p.team_abbreviation,
                                                                                p.player_id))
                        c.start_log(run_id=g_run_id, node='player', node_key=p.player_id, parent_key=g.home_team_id,
                                    node_status='IN PROGRESS')

                        player_measure_count = process_player(player_id=p.player_id)
                        if p.player_id == 201142:
                            raise Exception('Debugging game completion')

                        c.end_log(run_id=g_run_id, node='player', key=p.player_id, status='COMPLETED',
                                  group_status='N/A')

                except Exception, e:  # this is home team players' exception
                    c.log.error('error processing home team players:{0}'.format(g.home_team_id))
                    c.log.error(e)
                    c.log.error('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
                    c.end_log(run_id=g_run_id, node='player', key=p.player_id, status='FAILED', group_status='N/A')
                    raise Exception('Error processing player')

            except Exception, e:  # this is home team exception
                c.log.error('error processing home team:{0}'.format(g.home_team_id))
                c.log.error(e)
                c.log.error('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
                c.end_log(run_id=g_run_id, node='team', key=g.home_team_id, status='FAILED', group_status='N/A')
                raise Exception('Error processing home team')

            try:
                c.log.info('processing visitor team:{0}'.format(g.visitor_team_id))
                c.start_log(run_id=g_run_id, node='team', node_key=g.visitor_team_id, parent_key=g.game_id,
                            node_status='IN PROGRESS')

                visitor_team_measure_count = process_team(g.visitor_team_id)
                c.end_log(run_id=g_run_id, node='team', key=g.visitor_team_id, status='COMPLETED',
                          group_status='N/A')

                try:  # this is visitor team players' try
                    c.log.info('processing visitor team players')
                    players = get_players_from_game(game_id=g.game_id, team_id=g.visitor_team_id)
                    for player in players.fetchall():
                        p = c.reg(players, player)
                        c.log.info('Processing player {0} ({1}) id=>{2}'.format(p.player_name, p.team_abbreviation,
                                                                                p.player_id))
                        c.start_log(run_id=g_run_id, node='player', node_key=p.player_id, parent_key=g.visitor_team_id,
                                    node_status='N/A')

                        player_measure_count = process_player(player_id=p.player_id)
                        c.end_log(run_id=g_run_id, node='player', key=p.player_id, status='COMPLETED',
                                  group_status='N/A')

                except Exception, e:  # this is visitor team players' exception
                    c.log.error('error processing home team players:{0}'.format(g.home_team_id))
                    c.log.error(e)
                    c.log.error('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
                    c.end_log(run_id=g_run_id, node='player', key=p.player_id, status='FAILED', group_status='N/A')
                    raise Exception('Error processing player')

            except Exception, e:  # this is visitor team exception
                c.log.error('error processing visitor team:{0}'.format(g.visitor_team_id))
                c.log.error(e)
                c.log.error('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
                c.end_log(run_id=g_run_id, node='team', key=g.visitor_team_id, status='FAILED', group_status='N/A')
                raise Exception('Error processing visitor team')

            c.end_log(run_id=g_run_id, node='game', key=g.game_id, status='COMPLETED', group_status='COMPLETED')
            #  at this point all the teams and players processed so game group_status can be updated as completed
        except Exception, e:  # this is game exception
            c.log.error('error processing game:{0}'.format(g.gamecode))
            c.log.error(e)
            c.log.error('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
            c.end_log(run_id=g_run_id, node='game', key=g.game_id, status='COMPLETED', group_status='FAILED')
            c.end_run(g_run_id, 'FAILED')
            sys.exit(1)
    c.end_run(g_run_id, 'COMPLETED')


if __name__ == '__main__':
    sys.exit(main())
