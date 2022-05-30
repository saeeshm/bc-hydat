#!/usr/bin/env Rscript

# Author: Saeesh Mangwani 
# Date: 2020-06-15 
# Description: A file that completes all of the pre-requisite tasks for the data scraping process, includingreading in data files, defining global
# variables and intiatializing helper functions

# ==== Loading libraries ====
library(dplyr)
library(stringr)
library(bcdata)
# Temporary libraries
library(readr)
library(sf)

# Reading chrome options from the chromeOptions.R script
source("scripts/selenium-download/00_set_program_options.R")

# ==== Reading Station list ====

# Downloading the BC hydrometric database that indiciates which stations ar
# presently active (see this link for more information
# https://github.com/bcgov/hydrometric_stations)
stations <- bcdc_get_data('4c169515-6c41-4f6a-bd30-19a1f45cad1f') %>% 
  st_transform(crs = 4326) %>% 
  mutate(longitude = st_coordinates(.)[,1],
         latitude = st_coordinates(.)[,2]) %>% 
  st_drop_geometry()

# Getting station ids as a vector
stat_ids <- stations$STATION_NUMBER

# Writing to station-metadata file
write_csv(stations, 'output/bc_hydat_station_metadata.csv')
detach(package:sf)
detach(package:readr)
# ==== Initializing global variables ====

# Creating an empty vector that stores the ids of the stations for whom the data
# download process fails, if at all
prob_stations <- tibble(station_id = character(0), issue = character(0))

# Creating empty master datasets for each of the default features that we want
# and saving them in a named list that can then be iterated over
masters <- list("flow" = tibble(Date = character(0), 
                                Parameter = character(0), 
                                Value = numeric(0), 
                                STATION_NUMBER = character(0)),
                "level" = tibble(Date = character(0), 
                                 Parameter = character(0), 
                                 Value = numeric(0), 
                                 STATION_NUMBER = character(0)),
                "air_temp" = tibble(Date = character(0),
                                    Parameter = character(0), 
                                    Value = numeric(0), 
                                    STATION_NUMBER = character(0)),
                "water_temp" = tibble(Date = character(0), 
                                      Parameter = character(0), 
                                      Value = numeric(0), 
                                      STATION_NUMBER = character(0)),
                "precip" = tibble(Date = character(0), 
                                  Parameter = character(0), 
                                  Value = numeric(0), 
                                  STATION_NUMBER = character(0)))

# A dataframe that contains a summary of what sorts of data are available for
# each station - a sort of short report about what sorts of data was extracted
summ_table <- tibble(station_id = character(0), tables_extracted = character(0))

# Getting the current date correctly formatted (we'll use this and the following
# data to create the url object during iteration)
curr_date <- as.Date(format(Sys.time(), "%Y-%m-%d"))

# Getting the approximate number of days that exist within the month range
# provided (rounded down) and using that to set the past date that delimits the
# time range for which data is to be downloaded
past_date <- curr_date - floor(opt$months * 30.4167)

# Creating an iteration variable to help print how much of the process is complete
countIter <- 1

# ==== Running pre-requisite processes  ====

# Clearing the required download and data folders of any older files to ensure
# smooth download and exporting --------
unlink(normalizePath("tempdirs/zip"), recursive = T)
dir.create(normalizePath("tempdirs/zip"))

unlink(normalizePath("tempdirs/download"), recursive = T)
dir.create(normalizePath("tempdirs/download"))

unlink(normalizePath("output/selenium-download"), recursive = T)
dir.create(normalizePath("output/selenium-download"))



