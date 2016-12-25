#!/usr/bin/python

import common as c
import time
import os
from nba_py import player

# "dreb_pct_rank" of relation "player_general_splits_location_adv"

endpoint = 'PlayerGeneralSplits'
measure = 'location'
measure_type = 'Advanced'
table_name = 'player_general_splits_location_adv'

e = player.PlayerGeneralSplits(player_id='1515',season='2006-07',season_type='Playoffs',measure_type=measure_type)
df = e.location()

df.columns = map(unicode.lower, df.columns)

cur = c.conn.cursor()
cur.execute("select column_name from information_schema.columns where table_schema = 'lnd' and table_name='{0}'".format(table_name))
column_names = [row[0] for row in cur]

for dcol in df.columns:
    if dcol in column_names:
        bc = str(df[dcol].dtype)
        print dcol.name
