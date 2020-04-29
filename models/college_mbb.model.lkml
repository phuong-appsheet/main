connection: "appsheet_bigquery_db" #specifies the database connection from which the model will retrieve data. A model can only have one connection.

# include all the views
include: "/views/**/*.view" #specifies LookML files that will be available to a model, a view, or an Explore

datagroup: college_mbb_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;; #specify when an ETL happened
  max_cache_age: "1 hour" #amount of time before the cached results become invalid. If the query rerun before max_cache_age expires, Looker pulls data from the cache.
                          #If the query is rerun after max_cache_age expires, Looker pulls the data directly from the database and resets max_cache_age timer

}

persist_with: college_mbb_default_datagroup #points to the datagroup "college_mbb_default_datagroup" caching policy


explore: mascots {}

explore: mbb_teams {}










explore: connection_reg_r3 {}

explore: mbb_games_sr {}

explore: mbb_historical_teams_games {}

explore: mbb_historical_teams_seasons {}

explore: mbb_historical_tournament_games {}

explore: mbb_pbp_sr {}

explore: mbb_players_games_sr {}

explore: mbb_teams_games_sr {}

explore: team_colors {}
