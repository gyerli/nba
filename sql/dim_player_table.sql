CREATE TABLE rpt.dim_player (
	dim_player_guid int8 NOT NULL,
	crc_str varchar(100) NULL,
	id int4 NULL,
	first_name varchar(20) NULL,
	last_name varchar(20) NULL,
	display_first_last varchar(50) NULL,
	display_last_comma_first varchar(50) NULL,
	display_fi_last varchar(50) NULL,
	birthdate date NULL,
	school varchar(50) NULL,
	country varchar(32) NULL,
	last_affiliation varchar(50) NULL,
	height int4 NULL,
	weight int4 NULL,
	season_exp int4 NULL,
	jersey int4 NULL,
	nba_pos varchar(14) NULL,
	rosterstatus varchar(8) NULL,
	playercode varchar(24) NULL,
	from_year int4 NULL,
	to_year int4 NULL,
	is_dleague bool NULL,
	is_games_played bool NULL,
	draft_year varchar(20) NULL,
	draft_round varchar(20) NULL,
	draft_number varchar(20) NULL,
	rec_start_date date NOT NULL,
	rec_end_date date NULL,
	is_rec_active bool NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	updated_at timestamptz NOT NULL DEFAULT now(),
	CONSTRAINT pk_dim_player PRIMARY KEY (dim_player_guid)
)
WITH (
	OIDS=FALSE
);
