#!/usr/bin/env Rscript

# Author: Saeesh Mangwani 
# Date: 2020-06-12 

# Description: Running the Selenium web scraper to download realtime hydrometric
# data from the BC hydat database. This program gathers all verified and
# unverified data for each station upto 18 months prior to the current date. To
# get only verified data, or to get just the past 30-days of realtime data more
# quickly, see the `tidyhydat` library.

# ==== Loading libraries ====
library(dplyr)
library(readr)
library(purrr)
library(stringr)
library(RSelenium)
library(optparse)
library(zip)
library(rjson)
source('scripts/selenium_download/selenium_help_funcs.R')

# ==== Program setup ====

# Reading program options definied in the options script
source("scripts/selenium_download/00_set_program_options.R")

# Initializing option parsing
option_list <-  list(
  make_option(c("-s", "--sample"), type="numeric", default=Inf, 
              help="If you would prefer to download data only for a random sample of the total wells, rather than all of them, specify the size of the sample you want (useful if you want to test the program on your system and so would prefer to only get a small subset of the data) [Default= No sampling]", 
              metavar="numeric"),
  make_option(c("-m", "--months"), type="numeric", default=18, 
              help="The number of months for which we would like to download data. [Default= %default, the maximum allowed range for which data can be downloaded]",
              metavar="numeric"))

# Parse any provided options and store them in a list
opt_parser = OptionParser(option_list=option_list)
opt = parse_args(opt_parser)
# opt$months <- 2

# Reading file paths from JSON
paths <- rjson::fromJSON(file = 'options/filepaths.json')

# Variable storing error status
iserror <- F

# ==== Getting setup objects ====
print("Starting setup...")

tryCatch({
  source("scripts/selenium_download/01_scraping_setup.R")
}, error = function(e){
  print("Error in the setup stage")
  print(e)
  iserror <<- T 
})

print("Setup complete")

# ==== Starting selenium server ====
print('Initializing Selenium server...')

tryCatch({
  if(iserror) stop("Can't proceed due to unresolved error in a previous stage")
  
  # Checking if a container named hydat_selenium_scraper already exists
  containerExists <- system('docker ps -a', intern = T) %>% 
    str_detect('hydat_selenium_scraper') %>% 
    any()
  
  # If yes, running it. Otherwise, initializing with the correct parameters:
  if(length(containerExists) > 0 && containerExists){
    system('docker start hydat_selenium_scraper')
  }else{
    # Initialize the docker container to run the selenium server (with a VNC viewer
    # to visualize the operation)
    system(paste0('docker run -d ', 
                  '--name hydat_selenium_scraper ',
                  '-p 5901:5900 ', 
                  '-p 127.0.0.1:4445:4444 ', 
                  '-v ', normalizePath(paths$temp_zip_path), ':/home/seluser/Downloads:rw,z ',
                  'selenium/standalone-firefox-debug'))
  }
  system('docker ps')
  
  # Initializing a remote driver in the docker container
  remDr <- RSelenium::remoteDriver(
    remoteServerAddr = "localhost", 
    port = 4445L, 
    browserName = "firefox",
    extraCapabilities = extraCaps
  )
  
  Sys.sleep(3)
  # Starting the remote driver (Use a VNC viewer to see the browser, pointing the
  # IP address to 127.0.0.1:5901)
  remDr$open()
}, error = function(e){
  print("Error while initializing the selenium server with docker")
  print(e)
  iserror <<- T 
})

print('Selenium server initalized')
# ==== Scraping data for active stations ====
print("Scraping realtime hydrometric data and formatting...")

tryCatch({
  if(iserror) stop("Can't proceed due to unresolved error in a previous stage")
  source("scripts/selenium_download/02_selenium_data_scraping.R")
  print("Completed data extraction")
}, error = function(e){
  # Printing the error message and location
  print("Fatal error during data download, data extraction may have failed")
  print(e)
  iserror <<- T 
})

print("Data scraping and formatting complete")

# ==== Exporting Data ====
print('Exporting downloaded data...')

tryCatch({
  if(iserror) stop("Can't proceed due to unresolved error in a previous stage")
  source("scripts/selenium_download/03_export_downloaded_data.R")
}, error = function(e){
  # Printing the error message and location
  print("Error during data export, export may have failed.")
  print(e)
  iserror <<- T 
})

print('Data export complete')

# ==== Closing Selenium Server ====
print('Closing selenium server...')

tryCatch({
  if(iserror) stop("Can't proceed due to unresolved error in a previous stage")
  source("scripts/selenium_download/04_closing_selenium.R")
  print("Server closed")
}, error = function(e){
  # Printing the error message and location
  print("Error while closing the Selenium server.")
  print(e)
})

print('Selenium server closed')
