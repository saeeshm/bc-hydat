# Author: Saeesh Mangwani
# Date: 2022-05-30

# Description: Reset/Initialize a postgres database containing data from the BC
# hydrometric data monitoring network (published as the Hydat.sqlite file)

# ==== Libraries ====
library(RPostgres)
library(DBI)
library(tidyhydat)
library(dplyr)
library(rjson)
library(lubridate)
source('scripts/30day-realtime-update/realtime_help_funcs.R')

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
hydat_path <- paths$pub_hydat_out_path

# Downloading published hydat
my_download_hydat(
  dl_hydat_here = hydat_path, 
  download_new = T
)

# ==== Initializing postgres database ====

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

# Calling the reset dbase function, which initialzes the dbase if not already
# present
reset_hydat_postgres(conn, creds, hydat_path)

# Closing connection
dbDisconnect(conn)
