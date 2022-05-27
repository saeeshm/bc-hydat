#!/usr/bin/env Rscript

# Author: Saeesh Mangwani
# Date: 2020-06-12
# Description: Manipulating hydro data

# ==== Loading libraries ====
if (!require("pacman")) install.packages("pacman")
pacman::p_load(tidyverse, bcdata, netstat, RSelenium, optparse, zip)

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





