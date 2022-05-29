#!/usr/bin/env Rscript

# Author: Saeesh Mangwani
# Date: 2020-06-12
# Description: Manipulating hydro data

# ==== Loading libraries ====
library(dplyr)
library(netstat)
library(RSelenium)
library(optparse)
library(zip)

# ==== Program setup ====


# Reading program options definied in the options script
source("scripts/selenium-download/00_set_program_options.R")

option_list <-  list(
  # make_option(c("-v", "--version"), type="character", default="98.0.4758.102", 
  #             help="What chrome version to use for initializing the Selenium driver (run binman::list_versions('chromedriver') to see the list of available versions. Also see documentation for RSelenium::rsDriver() for more details) [Default= %default]", 
  #             metavar="character"),
  make_option(c("-s", "--sample"), type="numeric", default=Inf, 
              help="If you would prefer to download data only for a random sample of the total wells, rather than all of them, specify the size of the sample you want (useful if you want to test the program on your system and so would prefer to only get a small subset of the data) [Default= No sampling]", 
              metavar="numeric"),
  make_option(c("-m", "--months"), type="numeric", default=18, 
              help="The number of months for which we would like to download data. [Default= %default, the maximum allowed range for which data can be downloaded]",
              metavar="numeric"))

# Parse any provided options and store them in a list
opt_parser = OptionParser(option_list=option_list)
opt = parse_args(opt_parser)


# ==== Setting up pre-requisite functions and objects and initializing option
# parsing (see setup.R) ====

print("Starting setup...")

tryCatch({
  source("setup.R")
}, error = function(e){
  print("Error in the initialization stage")
  print(e)
})

print("Setup complete. Starting data scraping...")

# ==== Running the selenium server to download Hydrometric data for active
# stations (see scraping.R) ====

tryCatch({
  source("scraping.R")
  print("Completed data extraction")
}, error = function(e){
  # Printing the error message and location
  print("Fatal error during data download, data extraction may have failed")
  print(e)
})

print("Closing Selenium server...")

# ==== Closing Selenium Server ====
tryCatch({
  source("closing.R")
  print("Server closed")
}, error = function(e){
  # Printing the error message and location
  print("Error while closing the Selenium server.")
  print(e)
})

print("Beginning Data Export...")

# ==== Exporting Data ====
tryCatch({
  source("export.R")
  print("Data Exported. Data download complete")
}, error = function(e){
  # Printing the error message and location
  print("Error during data export, export may have failed.")
  print(e)
})





