# Author: Saeesh Mangwani
# Date: 2022-08-15

# Description: Checks for a new version of the Hydat Sqlite database. If there
# is a new publication, overwrites the existing one and updates data on the
# database with the new publication status

# ==== Libraries ====
library(RPostgres)
library(DBI)
library(tidyhydat)
library(dplyr)
library(rjson)
library(lubridate)

# ==== Paths and global variables ====

# Reading the filepaths JSON that contains all paths for setting up the required
# directory structure for the program
paths <- rjson::fromJSON(file = 'options/filepaths.json')

# Path to postgres database credientials
creds_path <- paths$postgres_creds_path

# Folder containing published hydat.sqlite file
hydat_dir <- paths$pub_hydat_out_path

# Path to publication status CSV
pub_stats_path <- paths$pub_status_csv_path

# ==== Comparing publication status ====


