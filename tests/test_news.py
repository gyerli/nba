import common as c
import json
from requests import get
import pandas as pd

url = 'http://stats.nba.com/fantasynews/'

_json = get(url).json()
_df = pd.DataFrame(_json)

print _df