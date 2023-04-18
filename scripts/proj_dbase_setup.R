# Author: Saeesh Mangwani
# Date: 2022-08-15

# Description: Reset/Initialize a postgres database containing data from the BC
# hydrometric data monitoring network (published as the Hydat.sqlite file)

# ==== Libraries ====
library(RPostgres)
library(DBI)
library(tidyhydat)
library(dplyr)
library(rjson)
library(lubridate)
library(readr)
# A rewritten download_hydat function from tidyhydat which works better for this
# use case than the base
source('scripts/_fixed_tidyhdat_dbase_download.R')

# ==== Paths and global variables ====

# Reading the filepaths JSON that contains all paths for setting up the required
# directory structure for the program
paths <- rjson::fromJSON(file = 'options/filepaths.json')

# ==== Creating required project directories ====
dir_create_soft <- function(path){
  if (!dir.exists(path)) {
    dir.create(path)
  }else{
    print(paste0('< ',path, ' >', ' already exists!'))
  }
}
dir_create_soft(paths$data_path)
dir_create_soft(paths$realtime_out_path)
dir_create_soft(paths$selenium_out_path)
dir_create_soft(paths$tempdir_path)
dir_create_soft(paths$temp_download_path)
dir_create_soft(paths$temp_zip_path)
dir_create_soft(paths$logs_path)

# ==== Downloading published Hydat data ====

# Path to where published hydat should be stored
hydat_dir <- paths$pub_hydat_out_path

# Downloading published hydat
my_download_hydat(
  dl_hydat_here = hydat_dir, 
  ask = F
)

# Getting path to downloaded dataset
hydat_path <- list.files(hydat_dir, pattern = '(H|h)ydat', full.names = T)

# ==== Initializing/resetting postgres database ====

# Path to postgres database credentials
creds_path <- paths$postgres_creds_path

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

# Reading published flow and level data with tidyhydat from the sqlite database
print('Reading published flow and level data...')
flow <- hy_daily_flows(hydat_path = hydat_path, prov_terr_state_loc = "BC") %>% 
  tibble() %>% 
  mutate(pub_status = 'Published')
level <- hy_daily_levels(hydat_path = hydat_path, prov_terr_state_loc = "BC") %>% 
  tibble() %>% 
  mutate(pub_status = 'Published')

# Dropping existing tables
print('Dropping existing tables and schema if present...')
dbExecute(conn, paste0('drop table if exists ', creds$schema,'.flow'))
dbExecute(conn, paste0('drop table if exists ', creds$schema,'.level'))
dbExecute(conn, paste0('drop schema if exists ', creds$schema))

# (Re)creating the schema to host hydat data
print('Re-creating schema...')
dbExecute(conn, paste0('create schema ', creds$schema))
dbExecute(conn, paste0('grant all on schema ', creds$schema,
                       ' to postgres, ', creds$user, ';'))

# Posting published hydat data to postgres
print('Posting published hydat data...')
dbWriteTable(conn, 
             DBI::Id(schema = creds$schema, table = "flow"),
             # Adding a column indicating publication status
             flow,
             append = F,
             overwrite = T)
dbWriteTable(conn, 
             DBI::Id(schema = creds$schema, table = "level"),
             # Adding a column indicating publication status
             level,
             append = F,
             overwrite = T)
print('Reset complete!')

# Writing date of current hydat publication status to disk
write_csv(
  tibble(
    'filename' = basename(hydat_path),
    'path' = hydat_path,
    'pub_dttm' = hy_version(hydat_path)$Date,
    'pub_date' = date(hy_version(hydat_path)$Date),
  ),
  paths$pub_status_csv_path
)

# Closing connection
dbDisconnect(conn)
