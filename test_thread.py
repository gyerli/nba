import sys
import nba_py
import datetime
import time
import pandas as pd

# from nba_py import player as _player
from nba_py import constants as _constants
from nba_py import shotchart as _shotchart

import gamev2 as _game
import teamv2 as _team
import playerv2 as _player
import common as c
import go

from Queue import Queue
from threading import Thread


class Worker(Thread):
    """Thread executing tasks from a given tasks queue"""

    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kargs = self.tasks.get()
            try:
                func(*args, **kargs)
            except Exception, e:
                print e
            finally:
                self.tasks.task_done()


class ThreadPool:
    """Pool of threads consuming tasks from a queue"""

    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads): Worker(self.tasks)

    def add_task(self, func, *args, **kargs):
        """Add a task to the queue"""
        self.tasks.put((func, args, kargs))

    def wait_completion(self):
        """Wait for completion of all the tasks in the queue"""
        self.tasks.join()

players = [2210,2544,202681,2594,201567,1627770,203895,2592,202697,202732,101112]
team_id = 1610612739

c.g_season = '2016-17'
c.g_season_type = 'Regular Season'

go.g_season = '2016-17'
go.g_season_type = 'Regular Season'

pool = ThreadPool(len(players))

for player_id in players:
    print 'processing player {id}'.format(id=player_id)
    pool.add_task(go.process_player, player_id, team_id)

pool.wait_completion()