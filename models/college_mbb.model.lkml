connection: "appsheet_bigquery_db" #specifies the database connection from which the model will retrieve data. A model can only have one connection.

# include all the views
include: "/views/**/*.view" #specifies LookML files that will be available to a model, a view, or an Explore

datagroup: college_mbb_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;; #specify when an ETL happened
  max_cache_age: "1 hour" #amount of time before the cached results become invalid. If the query rerun before max_cache_age expires, Looker pulls data from the cache.
                          #If the query is rerun after max_cache_age expires, Looker pulls the data directly from the database and resets max_cache_age timer

}

persist_with: college_mbb_default_datagroup #points to the datagroup "college_mbb_default_datagroup" caching policy


explore: connection_reg_r3 {}




#mascots can be mapped to mbb_teams and team_colors
explore: mascots {  #add an option to the Explore menu based on the view mascots. Explore needs to reference the view name, not its file name
  from:  mascots    #not necessary in this case, but you can indicate which view the Explore is referring to if it has a different name than the view.
  join:  mbb_teams { #includes explore name that you're joining
    type: left_outer
    sql_on:  ${mascots.id} = ${mbb_teams.id} ;;
    relationship: one_to_one #order goes from this explore to the other explore. In this case, mascots to mbb_teams.
  }
  join: team_colors {
    type: left_outer
    sql_on:  ${mascots.id} = ${team_colors.id} ;;
    relationship:  one_to_one
  }
}

explore: mbb_teams {
  from:  mbb_teams
  join:  mascots {
    type: left_outer
    sql_on:  ${mascots.id} = ${mbb_teams.id} ;;
    relationship: one_to_one
  }
  join: mbb_historical_teams_games {
    type:  left_outer
    sql_on: ${mbb_teams.id} = ${mbb_historical_teams_games.team_id};;
    relationship: one_to_many
  }

}



explore: mbb_games_sr {}

explore: mbb_historical_teams_games {}

explore: mbb_historical_teams_seasons {}

explore: mbb_historical_tournament_games {}

explore: mbb_pbp_sr {}

explore: mbb_players_games_sr {}

explore: mbb_teams_games_sr {}

explore: team_colors {}
