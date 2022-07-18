# !usr/bin/env Rscript

# Author: Saeesh Mangwani
# Date: 2020-06-25
# Description: append downloaded data to hydat datasets

# ==== Loading libraries ====
library(dplyr)
library(readr)
library(DBI)
library(RSQLite)
library(optparse)
library(tidyhydat)
library(rjson)
source("scripts/_help_funcs.R")

# ==== Initializing option parsing ====
option_list <-  list(
  make_option(c("-s", "--scraped"), type="logical", default=F, 
              help="T/F: Whether there are data gathered through the scraping app that also need to updated in the hydat database [Default= %default]", 
              metavar="logical"))

# Parse any provided options and store them in a list
opt_parser = OptionParser(option_list=option_list)
opt = parse_args(opt_parser)

# ==== File paths files ====

# Reading globally defined file paths
paths <- rjson::fromJSON(file = 'options/filepaths.json')

# Path to postgres database credientials
creds_path <- paths$postgres_creds_path

# Folder containing published hydat.sqlite file
hydat_path <- paths$hydat_path

# Folder containing data scraped using the selenium-download process
scraped_data_path <- paths$selenium_out_path

# Folder where the realtime data files will be stored (i.e outputs from this
# script)
realtime_path <- paths$realtime_out_path

# Status report path
report_path <- file.path(paths$logs_path, 'last_update_report.txt')

# ==== Opening database connection ====

# Reading credentials
creds <- fromJSON(file = creds_path)

# Openining database connection
conn <- dbConnect(
  RPostgres::Postgres(), 
  host = creds$host, 
  dbname = creds$dbname,
  user = creds$user, 
  password = creds$password
)

# ==== Checking published hydat status ====

# Downloading a new hydat.sqlite if published
my_download_hydat(dl_hydat_here = hydat_path, download_new = T)

# Creating a realtime version of the hydat database if it doesn't already exist
file.copy(
  from = paste0(hydat_path, "/Hydat.sqlite3"), 
  to = paste0(realtime_path, "/Hydat_realtime.sqlite3"), 
  overwrite = F
)

# Checking the publication dates
published_date <- hy_version(paste0(hydat_path,"/Hydat.sqlite3"))$Date
curr_date <- hy_version(paste0(realtime_path,"/Hydat_realtime.sqlite3"))$Date

# If a new version has been published, overwriting the hydat_realtime dataset
# and the postgres database with the new published data. Any realtime data past
# the published date will be added later
if(published_date != curr_date){
  print('A new hydat version has been published!')
  # Updating hydat_realtime.sqlite
  print("Resetting the Hydat_current dataset")
  file.copy(from = paste0(hydat_path, "/Hydat.sqlite3"), 
            to = paste0(realtime_path, "/Hydat_realtime.sqlite3"), 
            overwrite = T)
  
  # Resetting postgres database with new hydat data
  print('Resetting the bchydat postgres schema')
  reset_hydat_postgres(conn, creds, hydat_path)
} 

# ==== Getting realtime data ====

# Downloading last 30-days of realtime data from tidyhydat
print("Downloading realtime (last 30 days) data...")
bc_realtime <- realtime_dd(prov_terr_state_loc = "BC") %>% 
  realtime_daily_mean()

# Filtering only flow data
flow_30day <- bc_realtime %>% 
  select(STATION_NUMBER, Date, Parameter, Value) %>% 
  filter(Parameter == "Flow") %>% 
  mutate(pub_status = 'Unpublished')

# Filtering only level data
level_30day <- bc_realtime %>% 
  select(STATION_NUMBER, Date, Parameter, Value) %>% 
  filter(Parameter == "Level") %>% 
  mutate(pub_status = 'Unpublished')

# If scraped data from the web scraping application is also being added,
# reading it here
if(opt$scraped){
  # Reading scraped data
  realtime_flow <- read_csv(paste0(scraped_data_path, "/BCflow_realtime.csv"), 
                       col_types='Dcdc') %>% 
    select(STATION_NUMBER, Date, Parameter, Value) %>% 
    mutate(pub_status = 'Unpublished') %>% 
    # Joining it with 30-day download data
    bind_rows(flow_30day) %>% 
    # Removing overlapping time periods of data
    group_by(STATION_NUMBER, Date) %>% 
    distinct(Date, .keep_all = T)
  
  # Same for level data
  realtime_level <- read.csv(paste0(scraped_data_path, "/BClevel_prim_realtime.csv"), 
                            col_types='Dcdc') %>% 
    select(STATION_NUMBER, Date, Parameter, Value) %>% 
    mutate(pub_status = 'Unpublished') %>% 
    bind_rows(level_30day) %>% 
    group_by(STATION_NUMBER, Date) %>% 
    distinct(Date, .keep_all = T)
}else{
  realtime_flow <- flow_30day
  realtime_level <- level_30day
}

# ==== Removing potential duplication from downloaded realtime data ====

# Reading all unpublished data if present
BCflow <- dbGetQuery(conn, "select * from bchydat.flow where pub_status = 'Unpublished'")
BClevel <- dbGetQuery(conn, "select * from bchydat.level where pub_status = 'Unpublished'")

# Locating all the data that isn't already in the Hydat dataset using an
# anti-join, to ensure that there are no duplicates. This is the "new" data
# that we'll be adding to the databases
new_flow <- realtime_flow %>% 
  group_by(STATION_NUMBER) %>% 
  anti_join(BCflow, by = c("STATION_NUMBER" = "STATION_NUMBER", 
                           "Date" = "Date")) %>% 
  ungroup()

new_level <- realtime_level %>% 
  group_by(STATION_NUMBER) %>% 
  anti_join(BClevel, by = c("STATION_NUMBER" = "STATION_NUMBER", 
                            "Date" = "Date")) %>% 
  ungroup()

# Transforming the new data to hydat table format
new_flow_hydat <- format_hydat_flow(new_flow %>% select(-pub_status))
new_level_hydat <- format_hydat_level(new_level %>% select(-pub_status))

# ==== Writing new realtime data to hydat and postgres ====

# Hydat.sqlite ----------

# opening sqlite connections
connSqlite <- dbConnect(RSQLite::SQLite(), paste0(hydat_path, "/Hydat_current.sqlite3"))

# Appending unpublished data to hydat
dbWriteTable(connSqlite, "DLY_FLOWS", new_flow_hydat, append = T, overwrite = F)
dbWriteTable(connSqlite, "DLY_LEVELS", new_level_hydat, append = T, overwrite = F)

# Closing connection
dbDisconnect(connSqlite)

# Postgres ----------
dbWriteTable(
  conn, 
  DBI::Id(schema = creds$schema, table = "flow"),
  new_flow,
  append = T
)
dbWriteTable(
  conn, 
  DBI::Id(schema = creds$schema, table = "level"),
  new_level,
  append = T
)

dbDisconnect(conn)

# ==== Updating logs ====

# Updating the current status text file
curr_time <- Sys.time()
sink(report_path, append = F)
cat("Hydat database update status:\n")
cat("\n")
cat("Date of last HYDAT publication (hydat.sqlite):", as.character(published_date), "\n")
cat("Last successful realtime update (hydat_realtime.sqlite):", as.character(curr_date), "\n")
cat("\n")
cat("Update is ahead of published data by", round(as.numeric(curr_date - published_date), 2), "days")
sink()



