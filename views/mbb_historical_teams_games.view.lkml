view: mbb_historical_teams_games {
  sql_table_name: `looker_scratch.mbb_historical_teams_games`
    ;;

  dimension: alias {
    type: string
    sql: ${TABLE}.alias ;;
  }

  dimension: attendance {
    type: number
    sql: ${TABLE}.attendance ;;
  }

  dimension: current_division {
    type: string
    sql: ${TABLE}.current_division ;;
  }

  dimension: market {
    type: string
    sql: ${TABLE}.market ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: opp_alias {
    type: string
    sql: ${TABLE}.opp_alias ;;
  }

  dimension: opp_code {
    type: number
    sql: ${TABLE}.opp_code ;;
  }

  dimension: opp_current_division {
    type: string
    sql: ${TABLE}.opp_current_division ;;
  }

  dimension: opp_id {
    type: string
    sql: ${TABLE}.opp_id ;;
  }

  dimension: opp_market {
    type: string
    sql: ${TABLE}.opp_market ;;
  }

  dimension: opp_name {
    type: string
    sql: ${TABLE}.opp_name ;;
  }

  dimension: opp_points_game {
    type: number
    sql: ${TABLE}.opp_points_game ;;
  }

  dimension: points_game {
    type: number
    sql: ${TABLE}.points_game ;;
  }

  dimension: scheduled_date {
    type: string
    sql: ${TABLE}.scheduled_date ;;
  }

  dimension: season {
    type: number
    sql: ${TABLE}.season ;;
  }

  dimension: team_code {
    type: string
    sql: ${TABLE}.team_code ;;
  }

  dimension: team_id {
    type: string
    sql: ${TABLE}.team_id ;;
  }

  dimension: win {
    type: yesno
    sql: ${TABLE}.win ;;
  }

  measure: count {
    type: count
    drill_fields: [opp_name, name]
  }

  measure: average_game_attendance{
    type:  average
    sql: ${attendance} ;;
  }


}
