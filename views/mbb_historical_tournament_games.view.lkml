view: mbb_historical_tournament_games {
  sql_table_name: `looker_scratch.mbb_historical_tournament_games`
    ;;

  dimension: academic_year {
    type: number
    sql: ${TABLE}.academic_year ;;
  }

  dimension: day {
    type: string
    sql: ${TABLE}.day ;;
  }

  dimension: days_from_epoch {
    type: number
    sql: ${TABLE}.days_from_epoch ;;
  }

  dimension_group: game {
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
    sql: ${TABLE}.game_date ;;
  }

  dimension: lose_alias {
    type: string
    sql: ${TABLE}.lose_alias ;;
  }

  dimension: lose_code_ncaa {
    type: number
    sql: ${TABLE}.lose_code_ncaa ;;
  }

  dimension: lose_kaggle_team_id {
    type: number
    sql: ${TABLE}.lose_kaggle_team_id ;;
  }

  dimension: lose_market {
    type: string
    sql: ${TABLE}.lose_market ;;
  }

  dimension: lose_name {
    type: string
    sql: ${TABLE}.lose_name ;;
  }

  dimension: lose_pts {
    type: number
    sql: ${TABLE}.lose_pts ;;
  }

  dimension: lose_region {
    type: string
    sql: ${TABLE}.lose_region ;;
  }

  dimension: lose_school_ncaa {
    type: string
    sql: ${TABLE}.lose_school_ncaa ;;
  }

  dimension: lose_seed {
    type: string
    sql: ${TABLE}.lose_seed ;;
  }

  dimension: lose_team_id {
    type: string
    sql: ${TABLE}.lose_team_id ;;
  }

  dimension: num_ot {
    type: number
    sql: ${TABLE}.num_ot ;;
  }

  dimension: round {
    type: number
    sql: ${TABLE}.round ;;
  }

  dimension: season {
    type: number
    sql: ${TABLE}.season ;;
  }

  dimension: win_alias {
    type: string
    sql: ${TABLE}.win_alias ;;
  }

  dimension: win_code_ncaa {
    type: number
    sql: ${TABLE}.win_code_ncaa ;;
  }

  dimension: win_kaggle_team_id {
    type: number
    sql: ${TABLE}.win_kaggle_team_id ;;
  }

  dimension: win_market {
    type: string
    sql: ${TABLE}.win_market ;;
  }

  dimension: win_name {
    type: string
    sql: ${TABLE}.win_name ;;
  }

  dimension: win_pts {
    type: number
    sql: ${TABLE}.win_pts ;;
  }

  dimension: win_region {
    type: string
    sql: ${TABLE}.win_region ;;
  }

  dimension: win_school_ncaa {
    type: string
    sql: ${TABLE}.win_school_ncaa ;;
  }

  dimension: win_seed {
    type: string
    sql: ${TABLE}.win_seed ;;
  }

  dimension: win_team_id {
    type: string
    sql: ${TABLE}.win_team_id ;;
  }

  measure: count {
    type: count
    drill_fields: [lose_name, win_name]
  }
}
