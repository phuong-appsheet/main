view: mascots {
  sql_table_name: `looker_scratch.mascots`
    ;;
  drill_fields: [id]

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: market {
    type: string
    sql: ${TABLE}.market ;;
  }

  dimension: mascot {
    type: string
    sql: ${TABLE}.mascot ;;
  }

  dimension: mascot_common_name {
    type: string
    sql: ${TABLE}.mascot_common_name ;;
  }

  dimension: mascot_name {
    type: string
    sql: ${TABLE}.mascot_name ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: non_tax_type {
    type: string
    sql: ${TABLE}.non_tax_type ;;
  }

  dimension: tax_class {
    type: string
    sql: ${TABLE}.tax_class ;;
  }

  dimension: tax_domain {
    type: string
    sql: ${TABLE}.tax_domain ;;
  }

  dimension: tax_family {
    type: string
    sql: ${TABLE}.tax_family ;;
  }

  dimension: tax_genus {
    type: string
    sql: ${TABLE}.tax_genus ;;
  }

  dimension: tax_kingdom {
    type: string
    sql: ${TABLE}.tax_kingdom ;;
  }

  dimension: tax_order {
    type: string
    sql: ${TABLE}.tax_order ;;
  }

  dimension: tax_phylum {
    type: string
    sql: ${TABLE}.tax_phylum ;;
  }

  dimension: tax_species {
    type: string
    sql: ${TABLE}.tax_species ;;
  }

  dimension: tax_subspecies {
    type: string
    sql: ${TABLE}.tax_subspecies ;;
  }

  measure: count {
    type: count
    drill_fields: [id, name, mascot_name, mascot_common_name]
  }
}
