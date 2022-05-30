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
source('scripts/_postgres_help_funcs.R')

# ==== Paths and global variables ====

# Program global options
paths <- rjson::fromJSON(file = 'options/filepaths.json')

# Published hydat path
hydat_path <- paths$hydat_path

# Postgres database credentials
creds_path <- paths$postgres_creds_path

# ==== Creating required project directories ====
dir_create_soft <- function(path){
  if (!dir.exists(path)) {
    dir.create(path)
  }else{
    print(paste0('< ',path, ' >', ' already exists!'))
  }
}
dir_create_soft('data')
dir_create_soft('data/realtime')
dir_create_soft('data/selenium-download')
dir_create_soft('tempdirs')
dir_create_soft('tempdirs/download')
dir_create_soft('tempdirs/zip')
dir_create_soft('logs')

# ==== Downloading published Hydat data ====
tidyhydat::download_hydat(
  dl_hydat_here = file.path(hydat_path, 'Hydat.sqlite'), 
  ask = F
)

# # Getting the date of publication
# dateJSON <- list()
# dateJSON$last_publish <- hydat_path %>% 
#   file.path(., "Hydat.sqlite3") %>% 
#   tidyhydat::hy_version() %>% 
#   pull(Date) %>% 
#   lubridate::ymd_hms() %>% 
#   lubridate::date() %>% 
#   as.character()
# # Saving last date of update as equal to last date of publication as these are
# # the same when setting up the project
# dateJSON$last_update <- dateJSON$last_publish
# dateJSON <- toJSON(dateJSON)
# write(dateJSON, file = 'options/date.json')

# ==== Initializing postgres database ====

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

# Closing connection
dbDisconnect(conn)
