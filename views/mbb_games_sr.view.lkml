view: mbb_games_sr {
  sql_table_name: `looker_scratch.mbb_games_sr`
    ;;

  dimension: a_alias {
    type: string
    sql: ${TABLE}.a_alias ;;
  }

  dimension: a_assists {
    type: number
    sql: ${TABLE}.a_assists ;;
  }

  dimension: a_assists_turnover_ratio {
    type: number
    sql: ${TABLE}.a_assists_turnover_ratio ;;
  }

  dimension: a_blocked_att {
    type: number
    sql: ${TABLE}.a_blocked_att ;;
  }

  dimension: a_blocks {
    type: number
    sql: ${TABLE}.a_blocks ;;
  }

  dimension: a_coach_tech_fouls {
    type: number
    sql: ${TABLE}.a_coach_tech_fouls ;;
  }

  dimension: a_conf_alias {
    type: string
    sql: ${TABLE}.a_conf_alias ;;
  }

  dimension: a_conf_id {
    type: string
    sql: ${TABLE}.a_conf_id ;;
  }

  dimension: a_conf_name {
    type: string
    sql: ${TABLE}.a_conf_name ;;
  }

  dimension: a_defensive_rebounds {
    type: number
    sql: ${TABLE}.a_defensive_rebounds ;;
  }

  dimension: a_division_alias {
    type: string
    sql: ${TABLE}.a_division_alias ;;
  }

  dimension: a_division_id {
    type: string
    sql: ${TABLE}.a_division_id ;;
  }

  dimension: a_division_name {
    type: string
    sql: ${TABLE}.a_division_name ;;
  }

  dimension: a_ejections {
    type: number
    sql: ${TABLE}.a_ejections ;;
  }

  dimension: a_fast_break_pts {
    type: number
    sql: ${TABLE}.a_fast_break_pts ;;
  }

  dimension: a_field_goals_att {
    type: number
    sql: ${TABLE}.a_field_goals_att ;;
  }

  dimension: a_field_goals_made {
    type: number
    sql: ${TABLE}.a_field_goals_made ;;
  }

  dimension: a_field_goals_pct {
    type: number
    sql: ${TABLE}.a_field_goals_pct ;;
  }

  dimension: a_flagrant_fouls {
    type: number
    sql: ${TABLE}.a_flagrant_fouls ;;
  }

  dimension: a_foulouts {
    type: number
    sql: ${TABLE}.a_foulouts ;;
  }

  dimension: a_free_throws_att {
    type: number
    sql: ${TABLE}.a_free_throws_att ;;
  }

  dimension: a_free_throws_made {
    type: number
    sql: ${TABLE}.a_free_throws_made ;;
  }

  dimension: a_free_throws_pct {
    type: number
    sql: ${TABLE}.a_free_throws_pct ;;
  }

  dimension: a_id {
    type: string
    sql: ${TABLE}.a_id ;;
  }

  dimension: a_league_alias {
    type: string
    sql: ${TABLE}.a_league_alias ;;
  }

  dimension: a_league_id {
    type: string
    sql: ${TABLE}.a_league_id ;;
  }

  dimension: a_league_name {
    type: string
    sql: ${TABLE}.a_league_name ;;
  }

  dimension: a_logo_large {
    type: string
    sql: ${TABLE}.a_logo_large ;;
  }

  dimension: a_logo_medium {
    type: string
    sql: ${TABLE}.a_logo_medium ;;
  }

  dimension: a_logo_small {
    type: string
    sql: ${TABLE}.a_logo_small ;;
  }

  dimension: a_market {
    type: string
    sql: ${TABLE}.a_market ;;
  }

  dimension: a_minutes {
    type: string
    sql: ${TABLE}.a_minutes ;;
  }

  dimension: a_name {
    type: string
    sql: ${TABLE}.a_name ;;
  }

  dimension: a_offensive_rebounds {
    type: number
    sql: ${TABLE}.a_offensive_rebounds ;;
  }

  dimension: a_personal_fouls {
    type: number
    sql: ${TABLE}.a_personal_fouls ;;
  }

  dimension: a_player_tech_fouls {
    type: number
    sql: ${TABLE}.a_player_tech_fouls ;;
  }

  dimension: a_points {
    type: number
    sql: ${TABLE}.a_points ;;
  }

  dimension: a_points_game {
    type: number
    sql: ${TABLE}.a_points_game ;;
  }

  dimension: a_points_off_turnovers {
    type: number
    sql: ${TABLE}.a_points_off_turnovers ;;
  }

  dimension: a_rank {
    type: number
    sql: ${TABLE}.a_rank ;;
  }

  dimension: a_rebounds {
    type: number
    sql: ${TABLE}.a_rebounds ;;
  }

  dimension: a_second_chance_pts {
    type: number
    sql: ${TABLE}.a_second_chance_pts ;;
  }

  dimension: a_steals {
    type: number
    sql: ${TABLE}.a_steals ;;
  }

  dimension: a_team_rebounds {
    type: number
    sql: ${TABLE}.a_team_rebounds ;;
  }

  dimension: a_team_tech_fouls {
    type: number
    sql: ${TABLE}.a_team_tech_fouls ;;
  }

  dimension: a_team_turnovers {
    type: number
    sql: ${TABLE}.a_team_turnovers ;;
  }

  dimension: a_three_points_att {
    type: number
    sql: ${TABLE}.a_three_points_att ;;
  }

  dimension: a_three_points_made {
    type: number
    sql: ${TABLE}.a_three_points_made ;;
  }

  dimension: a_three_points_pct {
    type: number
    sql: ${TABLE}.a_three_points_pct ;;
  }

  dimension: a_turnovers {
    type: number
    sql: ${TABLE}.a_turnovers ;;
  }

  dimension: a_two_points_att {
    type: number
    sql: ${TABLE}.a_two_points_att ;;
  }

  dimension: a_two_points_made {
    type: number
    sql: ${TABLE}.a_two_points_made ;;
  }

  dimension: a_two_points_pct {
    type: number
    sql: ${TABLE}.a_two_points_pct ;;
  }

  dimension: attendance {
    type: number
    sql: ${TABLE}.attendance ;;
  }

  dimension: conference_game {
    type: yesno
    sql: ${TABLE}.conference_game ;;
  }

  dimension: coverage {
    type: string
    sql: ${TABLE}.coverage ;;
  }

  dimension_group: created {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.created ;;
  }

  dimension: game_id {
    type: string
    sql: ${TABLE}.game_id ;;
  }

  dimension_group: gametime {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.gametime ;;
  }

  dimension: h_alias {
    type: string
    sql: ${TABLE}.h_alias ;;
  }

  dimension: h_assists {
    type: number
    sql: ${TABLE}.h_assists ;;
  }

  dimension: h_assists_turnover_ratio {
    type: number
    sql: ${TABLE}.h_assists_turnover_ratio ;;
  }

  dimension: h_blocked_att {
    type: number
    sql: ${TABLE}.h_blocked_att ;;
  }

  dimension: h_blocks {
    type: number
    sql: ${TABLE}.h_blocks ;;
  }

  dimension: h_coach_tech_fouls {
    type: number
    sql: ${TABLE}.h_coach_tech_fouls ;;
  }

  dimension: h_conf_alias {
    type: string
    sql: ${TABLE}.h_conf_alias ;;
  }

  dimension: h_conf_id {
    type: string
    sql: ${TABLE}.h_conf_id ;;
  }

  dimension: h_conf_name {
    type: string
    sql: ${TABLE}.h_conf_name ;;
  }

  dimension: h_defensive_rebounds {
    type: number
    sql: ${TABLE}.h_defensive_rebounds ;;
  }

  dimension: h_division_alias {
    type: string
    sql: ${TABLE}.h_division_alias ;;
  }

  dimension: h_division_id {
    type: string
    sql: ${TABLE}.h_division_id ;;
  }

  dimension: h_division_name {
    type: string
    sql: ${TABLE}.h_division_name ;;
  }

  dimension: h_ejections {
    type: number
    sql: ${TABLE}.h_ejections ;;
  }

  dimension: h_fast_break_pts {
    type: number
    sql: ${TABLE}.h_fast_break_pts ;;
  }

  dimension: h_field_goals_att {
    type: number
    sql: ${TABLE}.h_field_goals_att ;;
  }

  dimension: h_field_goals_made {
    type: number
    sql: ${TABLE}.h_field_goals_made ;;
  }

  dimension: h_field_goals_pct {
    type: number
    sql: ${TABLE}.h_field_goals_pct ;;
  }

  dimension: h_flagrant_fouls {
    type: number
    sql: ${TABLE}.h_flagrant_fouls ;;
  }

  dimension: h_foulouts {
    type: number
    sql: ${TABLE}.h_foulouts ;;
  }

  dimension: h_free_throws_att {
    type: number
    sql: ${TABLE}.h_free_throws_att ;;
  }

  dimension: h_free_throws_made {
    type: number
    sql: ${TABLE}.h_free_throws_made ;;
  }

  dimension: h_free_throws_pct {
    type: number
    sql: ${TABLE}.h_free_throws_pct ;;
  }

  dimension: h_id {
    type: string
    sql: ${TABLE}.h_id ;;
  }

  dimension: h_league_alias {
    type: string
    sql: ${TABLE}.h_league_alias ;;
  }

  dimension: h_league_id {
    type: string
    sql: ${TABLE}.h_league_id ;;
  }

  dimension: h_league_name {
    type: string
    sql: ${TABLE}.h_league_name ;;
  }

  dimension: h_logo_large {
    type: string
    sql: ${TABLE}.h_logo_large ;;
  }

  dimension: h_logo_medium {
    type: string
    sql: ${TABLE}.h_logo_medium ;;
  }

  dimension: h_logo_small {
    type: string
    sql: ${TABLE}.h_logo_small ;;
  }

  dimension: h_market {
    type: string
    sql: ${TABLE}.h_market ;;
  }

  dimension: h_minutes {
    type: string
    sql: ${TABLE}.h_minutes ;;
  }

  dimension: h_name {
    type: string
    sql: ${TABLE}.h_name ;;
  }

  dimension: h_offensive_rebounds {
    type: number
    sql: ${TABLE}.h_offensive_rebounds ;;
  }

  dimension: h_personal_fouls {
    type: number
    sql: ${TABLE}.h_personal_fouls ;;
  }

  dimension: h_player_tech_fouls {
    type: number
    sql: ${TABLE}.h_player_tech_fouls ;;
  }

  dimension: h_points {
    type: number
    sql: ${TABLE}.h_points ;;
  }

  dimension: h_points_game {
    type: number
    sql: ${TABLE}.h_points_game ;;
  }

  dimension: h_points_off_turnovers {
    type: number
    sql: ${TABLE}.h_points_off_turnovers ;;
  }

  dimension: h_rank {
    type: number
    sql: ${TABLE}.h_rank ;;
  }

  dimension: h_rebounds {
    type: number
    sql: ${TABLE}.h_rebounds ;;
  }

  dimension: h_second_chance_pts {
    type: number
    sql: ${TABLE}.h_second_chance_pts ;;
  }

  dimension: h_steals {
    type: number
    sql: ${TABLE}.h_steals ;;
  }

  dimension: h_team_rebounds {
    type: number
    sql: ${TABLE}.h_team_rebounds ;;
  }

  dimension: h_team_tech_fouls {
    type: number
    sql: ${TABLE}.h_team_tech_fouls ;;
  }

  dimension: h_team_turnovers {
    type: number
    sql: ${TABLE}.h_team_turnovers ;;
  }

  dimension: h_three_points_att {
    type: number
    sql: ${TABLE}.h_three_points_att ;;
  }

  dimension: h_three_points_made {
    type: number
    sql: ${TABLE}.h_three_points_made ;;
  }

  dimension: h_three_points_pct {
    type: number
    sql: ${TABLE}.h_three_points_pct ;;
  }

  dimension: h_turnovers {
    type: number
    sql: ${TABLE}.h_turnovers ;;
  }

  dimension: h_two_points_att {
    type: number
    sql: ${TABLE}.h_two_points_att ;;
  }

  dimension: h_two_points_made {
    type: number
    sql: ${TABLE}.h_two_points_made ;;
  }

  dimension: h_two_points_pct {
    type: number
    sql: ${TABLE}.h_two_points_pct ;;
  }

  dimension: lead_changes {
    type: number
    sql: ${TABLE}.lead_changes ;;
  }

  dimension: neutral_site {
    type: yesno
    sql: ${TABLE}.neutral_site ;;
  }

  dimension: periods {
    type: number
    sql: ${TABLE}.periods ;;
  }

  dimension: possession_arrow {
    type: string
    sql: ${TABLE}.possession_arrow ;;
  }

  dimension_group: scheduled {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.scheduled_date ;;
  }

  dimension: season {
    type: number
    sql: ${TABLE}.season ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: times_tied {
    type: number
    sql: ${TABLE}.times_tied ;;
  }

  dimension: tournament {
    type: string
    sql: ${TABLE}.tournament ;;
  }

  dimension: tournament_game_no {
    type: string
    sql: ${TABLE}.tournament_game_no ;;
  }

  dimension: tournament_round {
    type: string
    sql: ${TABLE}.tournament_round ;;
  }

  dimension: tournament_type {
    type: string
    sql: ${TABLE}.tournament_type ;;
  }

  dimension: venue_address {
    type: string
    sql: ${TABLE}.venue_address ;;
  }

  dimension: venue_capacity {
    type: number
    sql: ${TABLE}.venue_capacity ;;
  }

  dimension: venue_city {
    type: string
    sql: ${TABLE}.venue_city ;;
  }

  dimension: venue_country {
    type: string
    sql: ${TABLE}.venue_country ;;
  }

  dimension: venue_id {
    type: string
    sql: ${TABLE}.venue_id ;;
  }

  dimension: venue_name {
    type: string
    sql: ${TABLE}.venue_name ;;
  }

  dimension: venue_state {
    type: string
    sql: ${TABLE}.venue_state ;;
  }

  dimension: venue_zip {
    type: string
    sql: ${TABLE}.venue_zip ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  # ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      h_conf_name,
      a_conf_name,
      a_league_name,
      h_division_name,
      a_name,
      h_league_name,
      a_division_name,
      venue_name,
      h_name
    ]
  }
}
