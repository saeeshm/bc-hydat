#!/usr/bin/env Rscript

# Author: Saeesh Mangwani 
# Date: 2020-06-15 
# Description: A file that completes all of the pre-requisite tasks for the data scraping process, includingreading in data files, defining global
# variables and intiatializing helper functions

# ==== Loading libraries ====
if (!require("pacman")) install.packages("pacman")
pacman::p_load(tidyverse, bcdata, netstat, RSelenium, optparse, zip)

# ==== Setting up Option Parsing ====

# Creating a list of flags that the user can pass to store the raw datafiles generated or not. The default behaviour is no.
option_list <-  list(
  make_option(c("-k", "--keep"), type="logical", default=F, 
              help="T/F: Whether to keep extracted csv files or not (Takes up a LOT more storage if yes) [Default= %default]", 
              metavar="logical"),
  make_option(c("-v", "--version"), type="character", default="98.0.4758.102", 
              help="What chrome version to use for initializing the Selenium driver (run binman::list_versions('chromedriver') to see the list of available versions. Also see documentation for RSelenium::rsDriver() for more details) [Default= %default]", 
              metavar="character"),
  make_option(c("-s", "--sample"), type="numeric", default=Inf, 
              help="If you would prefer to download data only for a random sample of the total wells, rather than all of them, specify the size of the sample you want (useful if you want to test the program on your system and so would prefer to only get a small subset of the data) [Default= No sampling]", 
              metavar="numeric"),
  make_option(c("-m", "--months"), type="numeric", default=18, 
              help="The number of months for which we would like to download data. [Default= %default, the maximum allowed range for which data can be downloaded]",
              metavar="numeric"))

# Parse any provided options and store them in a list
opt_parser = OptionParser(option_list=option_list)
opt = parse_args(opt_parser)

# Reading chrome options from the chromeOptions.R script
source("scripts/selenium-download/00_set_chrome_options.R")

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

# ==== Defining helper functions ====

# A function that takes a vector of filenames for the downloaded csvs and
# returns a transformed set of object names (names that are easier to work with
# as objects in R)
transformNames <- function(x){
  # Iterating over all the names and applying the appropriate transformation as
  # defined by a case_when set of conditions:
  for(i in 1:length(x)){
    x[i] <- case_when(str_detect(tolower(x[i]), "precipitation|precip") ~ "precip",
                      str_detect(tolower(x[i]), "air") &  str_detect(tolower(x[i]), "temperature|temp") ~ "air_temp",
                      str_detect(tolower(x[i]), "water") &  str_detect(tolower(x[i]), "temperature|temp") ~ "water_temp",
                      # If no air/water indiciation is given, we assume it to be
                      # water temperature
                      str_detect(tolower(x[i]), "temperature|temp") ~ "water_temp",
                      str_detect(tolower(x[i]), "discharge|flow")   ~ "flow",
                      str_detect(tolower(x[i]), "level") &  str_detect(tolower(x[i]), "prim")  ~ "level_prim",
                      str_detect(tolower(x[i]), "level") &  str_detect(tolower(x[i]), "sec")  ~ "level_sec",
                      TRUE~ str_replace(str_remove_all(tolower(x[i]), ".csv"), "\\s", "_"))
  }
  return(x)
}

# A function that takes a 5-minute interval dataframe and transforms it into a
# daily means dataframe presented in the correct format
formatData <- function(df, station, varName){
  df <- df %>% 
    select(Date = contains("Date"), Parameter, Value = contains("Value"), everything()) %>% 
    mutate("STATION_NUMBER" = station) %>% 
    mutate(Parameter = as.character(Parameter)) %>% 
    mutate(Parameter = case_when(varName == "precip" ~ "Precipitation",
                                 varName == "air_temp" ~ "Air Temperature",
                                 varName == "water_temp" ~ "Water Temperature",
                                 varName == "flow" ~ "Flow",
                                 varName == "level_prim" ~ "Level (Primary Sensor)",
                                 varName == "level_sec" ~ "Level (Secondary Sensor)",
                                 TRUE ~ varName)) %>% 
    mutate(Date = format.Date(Date, "%Y-%m-%d"))
  if(nrow(df) > 0){
    df <- df %>% 
      group_by(Date) %>% 
      group_modify(~ {
        .x %>% 
          mutate(Value = mean(Value, na.rm = T)) %>% 
          head(n = 1)
      }) %>% 
      ungroup()
  }
  # Return the dataframe
  return(df)
}

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

