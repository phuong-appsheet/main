view: team_colors {
  sql_table_name: `looker_scratch.team_colors`
    ;;
  drill_fields: [id]

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: code_ncaa {
    type: number
    sql: ${TABLE}.code_ncaa ;;
  }

  dimension: color {
    type: string
    sql: ${TABLE}.color ;;
  }

  dimension: market {
    type: string
    sql: ${TABLE}.market ;;
  }

  measure: count {
    type: count
    drill_fields: [id]
  }
}
