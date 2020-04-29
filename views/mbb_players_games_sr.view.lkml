view: mbb_players_games_sr {
  sql_table_name: `looker_scratch.mbb_players_games_sr`
    ;;

  dimension: abbr_name {
    type: string
    sql: ${TABLE}.abbr_name ;;
  }

  dimension: active {
    type: yesno
    sql: ${TABLE}.active ;;
  }

  dimension: assists {
    type: number
    sql: ${TABLE}.assists ;;
  }

  dimension: assists_turnover_ratio {
    type: number
    sql: ${TABLE}.assists_turnover_ratio ;;
  }

  dimension: birth_place {
    type: string
    sql: ${TABLE}.birth_place ;;
  }

  dimension: birthplace_city {
    type: string
    sql: ${TABLE}.birthplace_city ;;
  }

  dimension: birthplace_country {
    type: string
    sql: ${TABLE}.birthplace_country ;;
  }

  dimension: birthplace_state {
    type: string
    sql: ${TABLE}.birthplace_state ;;
  }

  dimension: blocked_att {
    type: number
    sql: ${TABLE}.blocked_att ;;
  }

  dimension: blocks {
    type: number
    sql: ${TABLE}.blocks ;;
  }

  dimension: class {
    type: string
    sql: ${TABLE}.class ;;
  }

  dimension: conf_alias {
    type: string
    sql: ${TABLE}.conf_alias ;;
  }

  dimension: conf_name {
    type: string
    sql: ${TABLE}.conf_name ;;
  }

  dimension: defensive_rebounds {
    type: number
    sql: ${TABLE}.defensive_rebounds ;;
  }

  dimension: division_alias {
    type: string
    sql: ${TABLE}.division_alias ;;
  }

  dimension: division_name {
    type: string
    sql: ${TABLE}.division_name ;;
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

  dimension: first_name {
    type: string
    sql: ${TABLE}.first_name ;;
  }

  dimension: flagrant_fouls {
    type: number
    sql: ${TABLE}.flagrant_fouls ;;
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

  dimension: full_name {
    type: string
    sql: ${TABLE}.full_name ;;
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

  dimension: height {
    type: number
    sql: ${TABLE}.height ;;
  }

  dimension: home_team {
    type: yesno
    sql: ${TABLE}.home_team ;;
  }

  dimension: jersey_number {
    type: number
    sql: ${TABLE}.jersey_number ;;
  }

  dimension: last_name {
    type: string
    sql: ${TABLE}.last_name ;;
  }

  dimension: league_name {
    type: string
    sql: ${TABLE}.league_name ;;
  }

  dimension: minutes {
    type: string
    sql: ${TABLE}.minutes ;;
  }

  dimension: minutes_int64 {
    type: number
    sql: ${TABLE}.minutes_int64 ;;
  }

  dimension: neutral_site {
    type: yesno
    sql: ${TABLE}.neutral_site ;;
  }

  dimension: offensive_rebounds {
    type: number
    sql: ${TABLE}.offensive_rebounds ;;
  }

  dimension: personal_fouls {
    type: number
    sql: ${TABLE}.personal_fouls ;;
  }

  dimension: played {
    type: yesno
    sql: ${TABLE}.played ;;
  }

  dimension: player_id {
    type: string
    sql: ${TABLE}.player_id ;;
  }

  dimension: points {
    type: number
    sql: ${TABLE}.points ;;
  }

  dimension: position {
    type: string
    sql: ${TABLE}.position ;;
  }

  dimension: primary_position {
    type: string
    sql: ${TABLE}.primary_position ;;
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

  dimension_group: sp_created {
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
    sql: ${TABLE}.sp_created ;;
  }

  dimension: starter {
    type: yesno
    sql: ${TABLE}.starter ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: steals {
    type: number
    sql: ${TABLE}.steals ;;
  }

  dimension: team_alias {
    type: string
    sql: ${TABLE}.team_alias ;;
  }

  dimension: team_id {
    type: string
    sql: ${TABLE}.team_id ;;
  }

  dimension: team_market {
    type: string
    sql: ${TABLE}.team_market ;;
  }

  dimension: team_name {
    type: string
    sql: ${TABLE}.team_name ;;
  }

  dimension: tech_fouls {
    type: number
    sql: ${TABLE}.tech_fouls ;;
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

  dimension: weight {
    type: number
    sql: ${TABLE}.weight ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  # ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      team_name,
      full_name,
      conf_name,
      first_name,
      abbr_name,
      league_name,
      last_name,
      division_name
    ]
  }
}
