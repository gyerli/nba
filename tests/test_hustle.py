import playerv2 as _player

endpoint = _player.HustleStatsPlayer(season='2016-17')
df = endpoint.overall()

print df