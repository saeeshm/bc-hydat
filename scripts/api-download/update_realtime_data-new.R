# !usr/bin/env Rscript

# Author: Saeesh Mangwani
# Date: 2020-06-25
# Description: append downloaded data to hydat datasets

# ==== Loading libraries ====
library(dplyr)
library(DBI)
library(RSQLite)
library(optparse)
library(tidyhydat)
source("scripts/api-download/help_funcs.R")

# ==== Paths and global variables ====

# Folder containing published hydat.sqlite file
hydat_path <- "data"

# Folder containing data scraped using the selenium-download process
scraped_data_path <-  "data/output"

# Folder where the realtime data files will be stored (i.e outputs from this
# script)
realtime_path <- "data/output"

# ==== Checking published hydat ====
my_download_hydat(dl_hydat_here = hydat_path, download_new = T)

# Creating a realtime version of the hydat database. If it does exist, leaving
# it as-is
file.copy(from = paste0(hydat_path, "/Hydat.sqlite3"), 
          to = paste0(realtime_path, "/Hydat_realtime.sqlite3"), 
          overwrite = F)
# ==== Getting realtime data ====

# Downloading last 30-days of realtime data from tidyhydat
print("Downloading realtime (last 30 days) data...")
bc_realtime <- realtime_dd(prov_terr_state_loc = "BC") %>% 
  realtime_daily_mean()

# Filtering only flow data
realtime_flow <- bc_realtime %>% 
  select(STATION_NUMBER, Date, Parameter, Value) %>% 
  filter(Parameter == "Flow")

# Filtering only level data
realtime_level <- bc_realtime %>% 
  select(STATION_NUMBER, Date, Parameter, Value) %>% 
  filter(Parameter == "Level")

# A named vector to store column types to ensure type consistency across all
# dataframes with format for realtime flow %>%
col_types <- c("X" = "character", "Date" = "Date", 
               "Parameter" = "character", "Value" = "numeric", 
               "STATION_NUMBER" = "character")

# If scraped data from the web scraping application is also being added,
# reading it here
if(scraped){
  # Flow
  scraped_flow <- read.csv(paste0(scraped_data_path, "/BCflow_current.csv"), 
                           colClasses=col_types) %>% 
    select(STATION_NUMBER, Date, Parameter, Value)
  # Level
  scraped_level <- read.csv(paste0(scraped_data_path, "/BClevel_prim_current.csv"), 
                            colClasses=col_types) %>% 
    select(STATION_NUMBER, Date, Parameter, Value)
}
