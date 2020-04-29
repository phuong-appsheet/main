view: mbb_pbp_sr {
  sql_table_name: `looker_scratch.mbb_pbp_sr`
    ;;

  dimension: attendance {
    type: number
    sql: ${TABLE}.attendance ;;
  }

  dimension: away_alias {
    type: string
    sql: ${TABLE}.away_alias ;;
  }

  dimension: away_conf_alias {
    type: string
    sql: ${TABLE}.away_conf_alias ;;
  }

  dimension: away_conf_name {
    type: string
    sql: ${TABLE}.away_conf_name ;;
  }

  dimension: away_division_alias {
    type: string
    sql: ${TABLE}.away_division_alias ;;
  }

  dimension: away_division_name {
    type: string
    sql: ${TABLE}.away_division_name ;;
  }

  dimension: away_id {
    type: string
    sql: ${TABLE}.away_id ;;
  }

  dimension: away_league_name {
    type: string
    sql: ${TABLE}.away_league_name ;;
  }

  dimension: away_market {
    type: string
    sql: ${TABLE}.away_market ;;
  }

  dimension: away_name {
    type: string
    sql: ${TABLE}.away_name ;;
  }

  dimension: conference_game {
    type: yesno
    sql: ${TABLE}.conference_game ;;
  }

  dimension: elapsed_time_sec {
    type: number
    sql: ${TABLE}.elapsed_time_sec ;;
  }

  dimension: event_coord_x {
    type: number
    sql: ${TABLE}.event_coord_x ;;
  }

  dimension: event_coord_y {
    type: number
    sql: ${TABLE}.event_coord_y ;;
  }

  dimension: event_description {
    type: string
    sql: ${TABLE}.event_description ;;
  }

  dimension: event_id {
    type: string
    sql: ${TABLE}.event_id ;;
  }

  dimension: event_type {
    type: string
    sql: ${TABLE}.event_type ;;
  }

  dimension: game_clock {
    type: string
    sql: ${TABLE}.game_clock ;;
  }

  dimension: game_id {
    type: string
    sql: ${TABLE}.game_id ;;
  }

  dimension: game_no {
    type: string
    sql: ${TABLE}.game_no ;;
  }

  dimension: home_alias {
    type: string
    sql: ${TABLE}.home_alias ;;
  }

  dimension: home_conf_alias {
    type: string
    sql: ${TABLE}.home_conf_alias ;;
  }

  dimension: home_conf_name {
    type: string
    sql: ${TABLE}.home_conf_name ;;
  }

  dimension: home_division_alias {
    type: string
    sql: ${TABLE}.home_division_alias ;;
  }

  dimension: home_division_name {
    type: string
    sql: ${TABLE}.home_division_name ;;
  }

  dimension: home_id {
    type: string
    sql: ${TABLE}.home_id ;;
  }

  dimension: home_league_name {
    type: string
    sql: ${TABLE}.home_league_name ;;
  }

  dimension: home_market {
    type: string
    sql: ${TABLE}.home_market ;;
  }

  dimension: home_name {
    type: string
    sql: ${TABLE}.home_name ;;
  }

  dimension: jersey_num {
    type: number
    sql: ${TABLE}.jersey_num ;;
  }

  dimension_group: load_timestamp {
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
    sql: ${TABLE}.load_timestamp ;;
  }

  dimension: neutral_site {
    type: yesno
    sql: ${TABLE}.neutral_site ;;
  }

  dimension: period {
    type: number
    sql: ${TABLE}.period ;;
  }

  dimension: player_full_name {
    type: string
    sql: ${TABLE}.player_full_name ;;
  }

  dimension: player_id {
    type: string
    sql: ${TABLE}.player_id ;;
  }

  dimension: points_scored {
    type: number
    sql: ${TABLE}.points_scored ;;
  }

  dimension: possession_arrow {
    type: string
    sql: ${TABLE}.possession_arrow ;;
  }

  dimension: possession_team_id {
    type: string
    sql: ${TABLE}.possession_team_id ;;
  }

  dimension: rebound_type {
    type: string
    sql: ${TABLE}.rebound_type ;;
  }

  dimension: round {
    type: string
    sql: ${TABLE}.round ;;
  }

  dimension_group: scheduled {
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
    sql: ${TABLE}.scheduled_date ;;
  }

  dimension: season {
    type: number
    sql: ${TABLE}.season ;;
  }

  dimension: shot_made {
    type: yesno
    sql: ${TABLE}.shot_made ;;
  }

  dimension: shot_subtype {
    type: string
    sql: ${TABLE}.shot_subtype ;;
  }

  dimension: shot_type {
    type: string
    sql: ${TABLE}.shot_type ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: team_alias {
    type: string
    sql: ${TABLE}.team_alias ;;
  }

  dimension: team_basket {
    type: string
    sql: ${TABLE}.team_basket ;;
  }

  dimension: team_conf_alias {
    type: string
    sql: ${TABLE}.team_conf_alias ;;
  }

  dimension: team_conf_name {
    type: string
    sql: ${TABLE}.team_conf_name ;;
  }

  dimension: team_division_alias {
    type: string
    sql: ${TABLE}.team_division_alias ;;
  }

  dimension: team_division_name {
    type: string
    sql: ${TABLE}.team_division_name ;;
  }

  dimension: team_id {
    type: string
    sql: ${TABLE}.team_id ;;
  }

  dimension: team_league_name {
    type: string
    sql: ${TABLE}.team_league_name ;;
  }

  dimension: team_market {
    type: string
    sql: ${TABLE}.team_market ;;
  }

  dimension: team_name {
    type: string
    sql: ${TABLE}.team_name ;;
  }

  dimension: three_point_shot {
    type: yesno
    sql: ${TABLE}.three_point_shot ;;
  }

  dimension: timeout_duration {
    type: number
    sql: ${TABLE}.timeout_duration ;;
  }

  dimension_group: timestamp {
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
    sql: ${TABLE}.timestamp ;;
  }

  dimension: tournament {
    type: string
    sql: ${TABLE}.tournament ;;
  }

  dimension: tournament_type {
    type: string
    sql: ${TABLE}.tournament_type ;;
  }

  dimension: turnover_type {
    type: string
    sql: ${TABLE}.turnover_type ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
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
      away_name,
      away_conf_name,
      team_name,
      team_division_name,
      venue_name,
      team_conf_name,
      player_full_name,
      home_name,
      home_league_name,
      team_league_name,
      away_division_name,
      home_conf_name,
      away_league_name,
      home_division_name
    ]
  }
}
