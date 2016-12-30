import playerv2 as _player

endpoint = _player.PlayerDefenseTracking(player_id='201143')
df = endpoint.defending_shots()

print df