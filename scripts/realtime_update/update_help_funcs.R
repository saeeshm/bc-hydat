# Author: Saeesh Mangwani
# Date: 2022-08-15

# Description: Helper functions for the BC hydat download and scraping programs

# ==== Loading libraries ====
library(dplyr)
library(tidyr)
library(stringr)
library(readr)
library(tidyhydat)
library(DBI)
library(RPostgres)

# ==== Helper functions ====

# Checks whether the current year is a leap year
is_leap <- function(year){
  if((year %% 4) == 0) {
    if((year %% 100) == 0) {
      if((year %% 400) == 0) {
        return(TRUE)
      } else {
        return(FALSE)
      }
    } else {
      return(TRUE)
    }
  } else {
    return(FALSE)
  }
}

# Updates flow data on the postgres database with newly published data from the
# hydat.sqlite database
update_new_published <- function(pub_hydat_path, creds, conn){
  print('Reading data from published Hydat...')
  # Station metadata
  hystations <- hy_stations(hydat_path = pub_hydat_path, 
                            prov_terr_state_loc = 'BC') %>% 
    tibble()
  # Published flow data
  hyflow <- hy_daily_flows(hydat_path = pub_hydat_path, 
                           prov_terr_state_loc = 'BC') %>% 
    tibble() %>% 
    mutate(pub_status = 'Published')
  # Published level data
  hylevel <- hy_daily_levels(hydat_path = pub_hydat_path, 
                            prov_terr_state_loc = 'BC') %>% 
    tibble() %>% 
    mutate(pub_status = 'Published')
  
  print('Writing updated station metadata...')
  # Updating CSV
  write_csv(hystations, 'data/bc_hydat_station_metadata.csv', append = F)
  # Updating postgres
  dbExecute(conn, paste0('drop table if exists ', 
                         creds$schema, '.station_metadata'))
  dbWriteTable(conn, 
               DBI::Id(schema = creds$schema, table = "station_metadata"),
               hystations,
               append = F,
               overwrite = T)
  
  # Reading all data from the postgres database
  print("Reading existing data from postgres...")
  dbflow <- dbGetQuery(conn, 
                       paste0("select * from ", creds$schema, ".flow"))
  dblevel <- dbGetQuery(conn, 
                        paste0("select * from ", creds$schema, ".level"))
  
  # Getting all rows from the database that don't exist in the published hydat
  # dataset (i.e there are realtime data that shouldn't be overwritten)
  print('Filtering un-changed realtime data...')
  unpub_flow <- anti_join(dbflow, hyflow, 
                          by = c('STATION_NUMBER', 'Date', 'Parameter'))
  unpub_level <- anti_join(dblevel, hylevel, 
                          by = c('STATION_NUMBER', 'Date', 'Parameter'))
  
  # Joining unpublished with published data to get a complete data set,
  # integrating any changes from the new publication
  bcflow <- bind_rows(hyflow, unpub_flow)
  bclevel <- bind_rows(hylevel, unpub_level)
  
  # Resetting postgres tables with the newly published data
  print('Writing updated database to postgres...')
  dbExecute(conn, paste0('drop table if exists ', creds$schema,'.flow'))
  dbWriteTable(conn, 
               DBI::Id(schema = creds$schema, table = "flow"),
               bcflow,
               append = F,
               overwrite = T)
  dbExecute(conn, paste0('drop table if exists ', creds$schema,'.level'))
  dbWriteTable(conn, 
               DBI::Id(schema = creds$schema, table = "level"),
               bclevel,
               append = F,
               overwrite = T)
  # Print return
  return('Database updates complete')
}

