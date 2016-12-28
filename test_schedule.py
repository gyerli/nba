#!/usr/bin/python

import pandas as pd
import json
import requests 

url_schedule = 'http://data.nba.com/data/10s/v2015/json/mobile_teams/nba/2016/league/00_full_schedule.json'
url_news = 'http://stats-prod.nba.com/wp-json/statscms/v1/rotowire/player/'

response = requests.get(url_news)
data = json.loads(response.text)

pn = data['ListItems']

df_news = pd.DataFrame(pn)

print df_news
