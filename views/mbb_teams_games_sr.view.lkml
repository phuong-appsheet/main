view: mbb_teams_games_sr {
  sql_table_name: `looker_scratch.mbb_teams_games_sr`
    ;;

  dimension: alias {
    type: string
    sql: ${TABLE}.alias ;;
  }

  dimension: assists {
    type: number
    sql: ${TABLE}.assists ;;
  }

  dimension: assists_turnover_ratio {
    type: number
    sql: ${TABLE}.assists_turnover_ratio ;;
  }

  dimension: attendance {
    type: number
    sql: ${TABLE}.attendance ;;
  }

  dimension: blocked_att {
    type: number
    sql: ${TABLE}.blocked_att ;;
  }

  dimension: blocks {
    type: number
    sql: ${TABLE}.blocks ;;
  }

  dimension: coach_tech_fouls {
    type: number
    sql: ${TABLE}.coach_tech_fouls ;;
  }

  dimension: conf_alias {
    type: string
    sql: ${TABLE}.conf_alias ;;
  }

  dimension: conf_id {
    type: string
    sql: ${TABLE}.conf_id ;;
  }

  dimension: conf_name {
    type: string
    sql: ${TABLE}.conf_name ;;
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

  dimension: defensive_rebounds {
    type: number
    sql: ${TABLE}.defensive_rebounds ;;
  }

  dimension: division_alias {
    type: string
    sql: ${TABLE}.division_alias ;;
  }

  dimension: division_id {
    type: string
    sql: ${TABLE}.division_id ;;
  }

  dimension: division_name {
    type: string
    sql: ${TABLE}.division_name ;;
  }

  dimension: ejections {
    type: number
    sql: ${TABLE}.ejections ;;
  }

  dimension: fast_break_pts {
    type: number
    sql: ${TABLE}.fast_break_pts ;;
  }

  dimension: field_goals_att {
    type: number
    sql: ${TABLE}.field_goals_att ;;
  }

  dimension: field_goals_made {
    type: number
    sql: ${TABLE}.field_goals_made ;;
  }

  dimension: field_goals_pct {
    type: number
    sql: ${TABLE}.field_goals_pct ;;
  }

  dimension: flagrant_fouls {
    type: number
    sql: ${TABLE}.flagrant_fouls ;;
  }

  dimension: foulouts {
    type: number
    sql: ${TABLE}.foulouts ;;
  }

  dimension: free_throws_att {
    type: number
    sql: ${TABLE}.free_throws_att ;;
  }

  dimension: free_throws_made {
    type: number
    sql: ${TABLE}.free_throws_made ;;
  }

  dimension: free_throws_pct {
    type: number
    sql: ${TABLE}.free_throws_pct ;;
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

  dimension: home_team {
    type: yesno
    sql: ${TABLE}.home_team ;;
  }

  dimension: lead_changes {
    type: number
    sql: ${TABLE}.lead_changes ;;
  }

  dimension: league_alias {
    type: string
    sql: ${TABLE}.league_alias ;;
  }

  dimension: league_id {
    type: string
    sql: ${TABLE}.league_id ;;
  }

  dimension: league_name {
    type: string
    sql: ${TABLE}.league_name ;;
  }

  dimension: logo_large {
    type: string
    sql: ${TABLE}.logo_large ;;
  }

  dimension: logo_medium {
    type: string
    sql: ${TABLE}.logo_medium ;;
  }

  dimension: logo_small {
    type: string
    sql: ${TABLE}.logo_small ;;
  }

  dimension: market {
    type: string
    sql: ${TABLE}.market ;;
  }

  dimension: minutes {
    type: string
    sql: ${TABLE}.minutes ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: neutral_site {
    type: yesno
    sql: ${TABLE}.neutral_site ;;
  }

  dimension: offensive_rebounds {
    type: number
    sql: ${TABLE}.offensive_rebounds ;;
  }

  dimension: opp_alias {
    type: string
    sql: ${TABLE}.opp_alias ;;
  }

  dimension: opp_assists {
    type: number
    sql: ${TABLE}.opp_assists ;;
  }

  dimension: opp_assists_turnover_ratio {
    type: number
    sql: ${TABLE}.opp_assists_turnover_ratio ;;
  }

  dimension: opp_blocked_att {
    type: number
    sql: ${TABLE}.opp_blocked_att ;;
  }

  dimension: opp_blocks {
    type: number
    sql: ${TABLE}.opp_blocks ;;
  }

  dimension: opp_coach_tech_fouls {
    type: number
    sql: ${TABLE}.opp_coach_tech_fouls ;;
  }

  dimension: opp_conf_alias {
    type: string
    sql: ${TABLE}.opp_conf_alias ;;
  }

  dimension: opp_conf_id {
    type: string
    sql: ${TABLE}.opp_conf_id ;;
  }

  dimension: opp_conf_name {
    type: string
    sql: ${TABLE}.opp_conf_name ;;
  }

  dimension: opp_defensive_rebounds {
    type: number
    sql: ${TABLE}.opp_defensive_rebounds ;;
  }

  dimension: opp_division_alias {
    type: string
    sql: ${TABLE}.opp_division_alias ;;
  }

  dimension: opp_division_id {
    type: string
    sql: ${TABLE}.opp_division_id ;;
  }

  dimension: opp_division_name {
    type: string
    sql: ${TABLE}.opp_division_name ;;
  }

  dimension: opp_ejections {
    type: number
    sql: ${TABLE}.opp_ejections ;;
  }

  dimension: opp_fast_break_pts {
    type: number
    sql: ${TABLE}.opp_fast_break_pts ;;
  }

  dimension: opp_field_goals_att {
    type: number
    sql: ${TABLE}.opp_field_goals_att ;;
  }

  dimension: opp_field_goals_made {
    type: number
    sql: ${TABLE}.opp_field_goals_made ;;
  }

  dimension: opp_field_goals_pct {
    type: number
    sql: ${TABLE}.opp_field_goals_pct ;;
  }

  dimension: opp_flagrant_fouls {
    type: number
    sql: ${TABLE}.opp_flagrant_fouls ;;
  }

  dimension: opp_foulouts {
    type: number
    sql: ${TABLE}.opp_foulouts ;;
  }

  dimension: opp_free_throws_att {
    type: number
    sql: ${TABLE}.opp_free_throws_att ;;
  }

  dimension: opp_free_throws_made {
    type: number
    sql: ${TABLE}.opp_free_throws_made ;;
  }

  dimension: opp_free_throws_pct {
    type: number
    sql: ${TABLE}.opp_free_throws_pct ;;
  }

  dimension: opp_id {
    type: string
    sql: ${TABLE}.opp_id ;;
  }

  dimension: opp_league_alias {
    type: string
    sql: ${TABLE}.opp_league_alias ;;
  }

  dimension: opp_league_id {
    type: string
    sql: ${TABLE}.opp_league_id ;;
  }

  dimension: opp_league_name {
    type: string
    sql: ${TABLE}.opp_league_name ;;
  }

  dimension: opp_logo_large {
    type: string
    sql: ${TABLE}.opp_logo_large ;;
  }

  dimension: opp_logo_medium {
    type: string
    sql: ${TABLE}.opp_logo_medium ;;
  }

  dimension: opp_logo_small {
    type: string
    sql: ${TABLE}.opp_logo_small ;;
  }

  dimension: opp_market {
    type: string
    sql: ${TABLE}.opp_market ;;
  }

  dimension: opp_minutes {
    type: string
    sql: ${TABLE}.opp_minutes ;;
  }

  dimension: opp_name {
    type: string
    sql: ${TABLE}.opp_name ;;
  }

  dimension: opp_offensive_rebounds {
    type: number
    sql: ${TABLE}.opp_offensive_rebounds ;;
  }

  dimension: opp_personal_fouls {
    type: number
    sql: ${TABLE}.opp_personal_fouls ;;
  }

  dimension: opp_player_tech_fouls {
    type: number
    sql: ${TABLE}.opp_player_tech_fouls ;;
  }

  dimension: opp_points {
    type: number
    sql: ${TABLE}.opp_points ;;
  }

  dimension: opp_points_game {
    type: number
    sql: ${TABLE}.opp_points_game ;;
  }

  dimension: opp_points_off_turnovers {
    type: number
    sql: ${TABLE}.opp_points_off_turnovers ;;
  }

  dimension: opp_rebounds {
    type: number
    sql: ${TABLE}.opp_rebounds ;;
  }

  dimension: opp_second_chance_pts {
    type: number
    sql: ${TABLE}.opp_second_chance_pts ;;
  }

  dimension: opp_steals {
    type: number
    sql: ${TABLE}.opp_steals ;;
  }

  dimension: opp_team_rebounds {
    type: number
    sql: ${TABLE}.opp_team_rebounds ;;
  }

  dimension: opp_team_tech_fouls {
    type: number
    sql: ${TABLE}.opp_team_tech_fouls ;;
  }

  dimension: opp_team_turnovers {
    type: number
    sql: ${TABLE}.opp_team_turnovers ;;
  }

  dimension: opp_three_points_att {
    type: number
    sql: ${TABLE}.opp_three_points_att ;;
  }

  dimension: opp_three_points_made {
    type: number
    sql: ${TABLE}.opp_three_points_made ;;
  }

  dimension: opp_three_points_pct {
    type: number
    sql: ${TABLE}.opp_three_points_pct ;;
  }

  dimension: opp_turnovers {
    type: number
    sql: ${TABLE}.opp_turnovers ;;
  }

  dimension: opp_two_points_att {
    type: number
    sql: ${TABLE}.opp_two_points_att ;;
  }

  dimension: opp_two_points_made {
    type: number
    sql: ${TABLE}.opp_two_points_made ;;
  }

  dimension: opp_two_points_pct {
    type: number
    sql: ${TABLE}.opp_two_points_pct ;;
  }

  dimension: periods {
    type: number
    sql: ${TABLE}.periods ;;
  }

  dimension: personal_fouls {
    type: number
    sql: ${TABLE}.personal_fouls ;;
  }

  dimension: player_tech_fouls {
    type: number
    sql: ${TABLE}.player_tech_fouls ;;
  }

  dimension: points {
    type: number
    sql: ${TABLE}.points ;;
  }

  dimension: points_game {
    type: number
    sql: ${TABLE}.points_game ;;
  }

  dimension: points_off_turnovers {
    type: number
    sql: ${TABLE}.points_off_turnovers ;;
  }

  dimension: possession_arrow {
    type: string
    sql: ${TABLE}.possession_arrow ;;
  }

  dimension: rebounds {
    type: number
    sql: ${TABLE}.rebounds ;;
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

  dimension: second_chance_pts {
    type: number
    sql: ${TABLE}.second_chance_pts ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: steals {
    type: number
    sql: ${TABLE}.steals ;;
  }

  dimension: team_id {
    type: string
    sql: ${TABLE}.team_id ;;
  }

  dimension: team_rebounds {
    type: number
    sql: ${TABLE}.team_rebounds ;;
  }

  dimension: team_tech_fouls {
    type: number
    sql: ${TABLE}.team_tech_fouls ;;
  }

  dimension: team_turnovers {
    type: number
    sql: ${TABLE}.team_turnovers ;;
  }

  dimension: three_points_att {
    type: number
    sql: ${TABLE}.three_points_att ;;
  }

  dimension: three_points_made {
    type: number
    sql: ${TABLE}.three_points_made ;;
  }

  dimension: three_points_pct {
    type: number
    sql: ${TABLE}.three_points_pct ;;
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

  dimension: turnovers {
    type: number
    sql: ${TABLE}.turnovers ;;
  }

  dimension: two_points_att {
    type: number
    sql: ${TABLE}.two_points_att ;;
  }

  dimension: two_points_made {
    type: number
    sql: ${TABLE}.two_points_made ;;
  }

  dimension: two_points_pct {
    type: number
    sql: ${TABLE}.two_points_pct ;;
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

  dimension: win {
    type: yesno
    sql: ${TABLE}.win ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  # ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      opp_division_name,
      conf_name,
      league_name,
      opp_league_name,
      opp_conf_name,
      division_name,
      name,
      venue_name,
      opp_name
    ]
  }
}
