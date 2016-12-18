select job.truncate_tables('lnd')

select job.create_mviews()

select job.refresh_mviews()

select rpt.refresh_dim_player()

select rpt.refresh_dim_game()

select rpt.refresh_fct_game_player()

select rpt.refresh_fct_game_team()