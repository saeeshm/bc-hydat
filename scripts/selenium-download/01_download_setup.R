#!/usr/bin/env Rscript

# Author: Saeesh Mangwani 
# Date: 2020-06-15 
# Description: A file that completes all of the pre-requisite tasks for the data scraping process, includingreading in data files, defining global
# variables and intiatializing helper functions

# ==== Loading libraries ====
library(dplyr)
library(stringr)
library(bcdata)

# Reading chrome options from the chromeOptions.R script
source("scripts/selenium-download/00_set_program_options.R")

# ==== Reading data ====

# Downloading the BC hydrometric database that indiciates which stations ar
# presently active (see this link for more information
# https://github.com/bcgov/hydrometric_stations), and selecting only the active
# station ids
stations <- bcdc_get_data('4c169515-6c41-4f6a-bd30-19a1f45cad1f') %>% 
  filter(str_detect(STATION_OPERATING_STATUS, "ACTIVE")) %>% 
  pull(STATION_NUMBER)

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
unlink("data\\zip", recursive = T)
dir.create("data\\zip")

unlink("data\\download", recursive = T)
dir.create("data\\download")

unlink("data\\output", recursive = T)
dir.create("data\\output")

# Setting up the Selenium Server --------

# Getting a free port to run the selenium server on
port <- netstat::free_port()

# Initialize the Selenium Server. Be sure to define the browser you want to use.
rD <- RSelenium::rsDriver(port = port,
                          browser = "chrome",
                          chromever = '98.0.4758.102',
                          verbose = F,
                          check = T,
                          extraCapabilities = eCaps)

# Assigning the client to a new variable
remDr <- rD$client

