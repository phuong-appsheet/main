view: mbb_historical_teams_seasons {
  sql_table_name: `looker_scratch.mbb_historical_teams_seasons`
    ;;

  dimension: alias {
    type: string
    sql: ${TABLE}.alias ;;
  }

  dimension: current_division {
    type: string
    sql: ${TABLE}.current_division ;;
  }

  dimension: division {
    type: number
    sql: ${TABLE}.division ;;
  }

  dimension: losses {
    type: number
    sql: ${TABLE}.losses ;;
  }

  dimension: market {
    type: string
    sql: ${TABLE}.market ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: season {
    type: number
    sql: ${TABLE}.season ;;
  }

  dimension: team_code {
    type: number
    sql: ${TABLE}.team_code ;;
  }

  dimension: team_id {
    type: string
    sql: ${TABLE}.team_id ;;
  }

  dimension: ties {
    type: number
    sql: ${TABLE}.ties ;;
  }

  dimension: wins {
    type: number
    sql: ${TABLE}.wins ;;
  }

  measure: count {
    type: count
    drill_fields: [name]
  }
}
