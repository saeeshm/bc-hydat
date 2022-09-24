# Author: Saeesh Mangwani 
# Date: 2022-08-15

# Description: Update the hydat databases with new realtime hydat data, as well
# as integrating any changes to published/validated hydat data with new
# publications of the Hydat.sqlite database. This script is the workhorse for
# continual Hydat updates.

# ==== Loading libraries ====
library(dplyr)
library(readr)
library(lubridate)
library(DBI)
library(RSQLite)
library(optparse)
library(tidyhydat)
library(rjson)
source('scripts/_fixed_tidyhdat_dbase_download.R')
source("scripts/realtime_update/update_help_funcs.R")

# ==== Initializing option parsing ====
option_list <-  list(
  make_option(c("-s", "--scraped"), type="logical", default=F, 
              help="T/F: Whether there are data gathered through the scraping app that also need to updated in the hydat database [Default= %default]", 
              metavar="logical"))

# Parse any provided options and store them in a list
opt_parser = OptionParser(option_list=option_list)
opt = parse_args(opt_parser)

# ==== File paths and global variables ====

# Reading globally defined file paths
paths <- rjson::fromJSON(file = 'options/filepaths.json')

# Path to postgres database credientials
creds_path <- paths$postgres_creds_path

# Folder containing published hydat.sqlite file
hydat_dir <- paths$pub_hydat_out_path

# Folder where the realtime data files will be stored (i.e outputs from this
# script)
# realtime_dir <- paths$realtime_out_path

# Folder containing data scraped using the selenium-download process
scraped_data_path <- paths$selenium_out_path

# Path to CSV holding hydat publication status information
pub_status_csv_path <- paths$pub_status_csv_path

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

# ==== Checking for changes to published hydat status ====

# Downloading a new hydat.sqlite if published
my_download_hydat(dl_hydat_here = hydat_dir, ask = F)

# Getting file path to the published hydat version
pub_hydat_path <- list.files(hydat_dir, pattern = 'Hydat_sqlite.+', 
                              full.names = T)
# Getting path to the realtime-updated hydat version
# realtime_hydat_path <- paste0(realtime_dir, "/Hydat_realtime.sqlite3")
  
# Creating a realtime version of the hydat database if it doesn't already exist
# file.copy(
#   from = pub_hydat_path, 
#   to = realtime_hydat_path, 
#   overwrite = F
# )

# Comparinig publication dates
published_date <- hy_version(pub_hydat_path)$Date %>% 
  as.character() %>% 
  ymd_hms()
stored_date <- read_csv(pub_status_csv_path, col_types = 'ccTD')$pub_dttm %>% 
  as.character() %>% 
  ymd_hms()

# If a new version has been published, overwriting the hydat_realtime dataset
# and the postgres database with the new published data, preserving any realtime
# data that hasn't been affected by validation
if(published_date != stored_date){
  print('A new hydat version has been published!')
  print("Updating database to integrate published changes to Hydat...")
  # Calling function to update databases. This takes a while...
  update_new_published(pub_hydat_path, creds, conn)
  # Updating publication status CSV on disk
  write_csv(
    tibble(
      'filename' = basename(pub_hydat_path),
      'path' = pub_hydat_path,
      'pub_dttm' = hy_version(pub_hydat_path)$Date,
      'pub_date' = lubridate::date(hy_version(pub_hydat_path)$Date),
    ),
    paths$pub_status_csv_path
  )
  print('Updates complete')
}else{
  'No new validated data published since last update'
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
  realtime_flow <- read_csv(paste0(scraped_data_path, "/flow_current.csv"), 
                       col_types='Dcdccc') %>% 
    select(STATION_NUMBER, Date, Parameter, Value) %>% 
    mutate(pub_status = 'Unpublished') %>% 
    # Joining it with 30-day download data
    bind_rows(flow_30day) %>% 
    # Removing overlapping time periods of data
    group_by(STATION_NUMBER, Date) %>% 
    distinct(Date, .keep_all = T)
  
  # Same for level data
  realtime_level <- read_csv(paste0(scraped_data_path, "/level_prim_current.csv"), 
                            col_types='Dcdccc') %>% 
    select(STATION_NUMBER, Date, Parameter, Value) %>% 
    mutate(Parameter = 'Level') %>% 
    mutate(pub_status = 'Unpublished') %>% 
    bind_rows(level_30day) %>% 
    group_by(STATION_NUMBER, Date) %>% 
    distinct(Date, .keep_all = T)
}else{
  realtime_flow <- flow_30day
  realtime_level <- level_30day
}

# ==== Removing potential duplication from downloaded realtime data ====

# Getting the earliest date of the update data
mindate = min(c(realtime_flow$Date, realtime_level$Date))

# Reading all data from the database since that date
BCflow <- dbGetQuery(conn, 
                     paste0("select * from bchydat.flow where \"Date\" >= '", mindate, "'"))
BClevel <- dbGetQuery(conn, 
                      paste0("select * from bchydat.level where \"Date\" >= '", mindate, "'"))

# Filtering only for data that aren't already in the Hydat dataset using an
# anti-join, to ensure that there are no duplicates. These are the "new" data
# that we'll be adding to the databases
new_flow <- realtime_flow %>% 
  group_by(STATION_NUMBER) %>% 
  anti_join(BCflow, by = c("STATION_NUMBER", "Date")) %>% 
  ungroup()

new_level <- realtime_level %>% 
  group_by(STATION_NUMBER) %>% 
  anti_join(BClevel, by = c("STATION_NUMBER", "Date")) %>% 
  ungroup()

# Transforming the new data to hydat table format
# new_flow_hydat <- format_hydat_flow(new_flow %>% select(-pub_status))
# new_level_hydat <- format_hydat_level(new_level %>% select(-pub_status))

# ==== Writing new realtime data to postgres ====

# Hydat.sqlite ----------

# # opening sqlite connections
# connSqlite <- dbConnect(RSQLite::SQLite(), realtime_hydat_path)
# 
# # Appending unpublished data to hydat
# dbWriteTable(connSqlite, "DLY_FLOWS", new_flow_hydat, append = T, overwrite = F)
# dbWriteTable(connSqlite, "DLY_LEVELS", new_level_hydat, append = T, overwrite = F)
# 
# # Closing connection
# dbDisconnect(connSqlite)

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
cat("Last successful realtime update (hydat_realtime.sqlite):", as.character(curr_time), "\n")
cat("\n")
cat("Update is ahead of published data by", round(as.numeric(curr_time - published_date), 2), "days")
sink()
 


