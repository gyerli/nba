CREATE MATERIALIZED VIEW rpt.mvw_player_schedule
AS
SELECT 
p.display_first_last,
p.id player_id,
g.id AS game_id,
g.game_date - p.birthdate age_days,
'Home' AS loc,
t.id team_id,
t.team_abbreviation,
tv.id AS opp_team_id,
tv.team_abbreviation as opp_team_abbreviation,
'Road' AS opp_loc,
g.season,
g.game_date
FROM rpt.dim_game g
  JOIN rpt.dim_team t ON g.home_team_id = t.id
  JOIN rpt.dim_team tv ON g.visitor_team_id = tv.id
  LEFT JOIN rpt.fct_game_player gp on g.id = gp.game_id
  LEFT JOIN rpt.dim_player p on gp.player_id = p.id
UNION
SELECT 
p.display_first_last,
p.id player_id,
g.id AS game_id,
g.game_date - p.birthdate age_days,
'Road' AS loc,
t.id team_id,
t.team_abbreviation,
th.id AS opp_team_id,
th.team_abbreviation as opp_team_abbreviation,
'Home' AS opp_loc,
g.season,
g.game_date
FROM rpt.dim_game g
  JOIN rpt.dim_team t ON g.visitor_team_id = t.id
  JOIN rpt.dim_team th ON g.home_team_id = th.id
  LEFT JOIN rpt.fct_game_player gp on g.id = gp.game_id
  LEFT JOIN rpt.dim_player p on gp.player_id = p.id