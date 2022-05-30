# Author: Saeesh Mangwani
# Date: 2022-05-30

# Description: Helper functions for interactions with the postgres hydat
# database

# ==== Libraries ====
library(RPostgres)
library(DBI)

# ==== Helper Functions ====

# Resets the hydat database with a newly published version of the hydat.sqlite
# database
reset_hydat_postgres <- function(conn, creds, hydat_path){
  
  print('Reading published flow and level data...')
  flow <- hy_daily_flows(hydat_path = paste0(hydat_path,"/Hydat.sqlite3"), 
                         prov_terr_state_loc = "BC") %>% tibble()
  level <- hy_daily_levels(hydat_path = paste0(hydat_path,"/Hydat.sqlite3"), 
                           prov_terr_state_loc = "BC") %>% tibble()
  
  
  print('Dropping existing tables and schema...')
  dbExecute(conn, 'drop table if exists bchydat.flow')
  dbExecute(conn, 'drop table if exists bchydat.level')
  dbExecute(conn, 'drop schema if exists bchydat ')
  
  print('Re-creating schema...')
  dbExecute(conn, 'create schema bchydat')
  dbExecute(conn, paste0('grant all on schema ', creds$schema,
                         ' to postgres, ', creds$user, ';'))
  
  print('Posting newly published hydat data...')
  dbWriteTable(conn, 
               DBI::Id(schema = creds$schema, table = "flow"),
               # Adding a column indicating publication status
               flow %>% mutate(pub_status = 'Published'),
               append = F,
               overwrite = T)
  dbWriteTable(conn, 
               DBI::Id(schema = creds$schema, table = "level"),
               # Adding a column indicating publication status
               level %>% mutate(pub_status = 'Published'),
               append = F,
               overwrite = T)
  print('Reset complete!')
}
