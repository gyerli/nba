import pandas as pd
import json
from requests import get

url = 'http://data.nba.com/data/10s/v2015/json/mobile_teams/nba/2016/league/00_full_schedule.json'

_json = get(url).json()
_df = pd.DataFrame(_json)

x = json.loads(_js)

for i in _df:
    print i

