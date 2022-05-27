# !usr/bin/env Rscript

# Author: Saeesh Mangwani
# Date: 2020-06-25
# Description: append downloaded data to hydat datasets

# ==== Loading libraries ====
if (!require("pacman")) install.packages("pacman")
pacman::p_load(dplyr, tidyhydat, DBI, RSQLite, optparse)
source("help_funcs.R")

# Flag for success
success <- T
gen_err <- NA_character_
specific_err <- NA_character_

# ==== Setting up option parsing ====
tryCatch({
  # Creating a list of flags that the user can pass to store the raw datafiles
  # generated or not. The default behaviour is no.
  option_list <-  list(
    make_option(c("-s", "--scraped"), type="logical", default=F, 
                help="T/F: Whether there are data gathered through the scraping app that also need to updated in the hydat database [Default= %default]", 
                metavar="logical"))
  
  # Parse any provided options and store them in a list
  opt_parser = OptionParser(option_list=option_list)
  opt = parse_args(opt_parser)
  
  # ==== Defining helper functions and global options ==== 
  
  # Setting the paths to the Hydat databases and the scraped datasets (if any).
  # Ensure that these are correct before proceeding with the next steps
  hydat_path <- "Z:/GWSI server Master Share Entry/GWSI Library and Resources/DATABASES/Hydrometric"
  scraped_data_path <-  "E:/saeeshProjects/BC_Hydrometric_Data_Scraping/data/output"
  curr_data_path <- "Z:/GWSI server Master Share Entry/GWSI Library and Resources/DATABASES/Hydrometric"
  
  # A boolean indicating whether or not the data scraping application was run
  # and scraped data was obtained. Defaults to false, since the scraping program
  # will only be required if one is looking for data beyond 1 month
  scraped <- opt$scraped
  
  # Ensuring directories are valid
  if(scraped & !dir.exists(scraped_data_path)) stop("Directory containing scraped data does not exist")
  if(!dir.exists(hydat_path)) stop("Directory to the published hydat dataset does not exist")
  if(!dir.exists(curr_data_path)) stop("Directory requested for storing the updated hydat dataset does not exist")
  
}, error = function(e){
  success <<- F
  gen_err <<- "Error during initialization stage"
  specific_err <<- e
  print(gen_err)
  print(e)
})

# ==== Checking for an updating hydat dataset ====

tryCatch({
  # Checking to ensure that we have the most recent hydat version available and if not downloading it
  my_download_hydat(dl_hydat_here = hydat_path, download_new = T)
  
  # If there is no Hydat_current.sqlite3 database, creating one. If it does exist, leaving it as-is
  file.copy(from = paste0(hydat_path, "/Hydat.sqlite3"), to = paste0(hydat_path, "/Hydat_current.sqlite3"), overwrite = F)
  
}, error = function(e){
  success <<- F
  gen_err <<- "Error while checking for valid Hydat.sqlite file"
  specific_err <<- e
  print(gen_err)
  print(e)
})

# ==== Gathering all downloaded data ====

# Getting the recent (last 30 days) realtime data
tryCatch({
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
  
  # Combining all the newly downloaded realtime data into a single csv
  # (ensuring that there is no duplication). If we're using both scraped and
  # realtime downloaded data then combining them into 1 file. Otherwise just
  # using the downloaded data
  if(scraped){
    new_flow <- bind_rows(scraped_flow, realtime_flow) %>% 
      group_by(STATION_NUMBER, Date) %>% 
      distinct(Date, .keep_all = T)
    new_level <- bind_rows(scraped_level, realtime_level) %>% 
      group_by(STATION_NUMBER, Date) %>% 
      distinct(Date, .keep_all = T)
  }else{
    new_flow <- realtime_flow
    new_level <- realtime_level
  }
  
},error = function(e){
  success <<- F
  gen_err <<- "Error during gathering new realtime data"
  specific_err <<- e
  print(gen_err)
  print(e)
})



# ==== Reading stored "current" (i.e unpublished realtime) data if any ====

tryCatch({
  # For flow
  if(file.exists((paste0(curr_data_path, "/BCflow_current.csv")))){
    curr_flow <- read.csv(paste0(curr_data_path, "/BCflow_current.csv"), colClasses = col_types) %>% 
      select(STATION_NUMBER, Date, Parameter, Value)
  }else{
    curr_flow <- new_flow[0,]
  }
  
  # For level
  if(file.exists((paste0(curr_data_path, "/BClevel_current.csv")))){
    curr_level <- read.csv(paste0(curr_data_path, "/BClevel_current.csv"), colClasses=col_types) %>% 
      select(STATION_NUMBER, Date, Parameter, Value)
  }else{
    curr_level <- new_level[0,]
  }
  
}, error = function(e){
  success <<- F
  gen_err <<- "Error while reading stored 'current' data"
  specific_err <<- e
  print(gen_err)
  print(e)
})

# ==== Joining newly downloaded realtime data with stored realtime data ===

# Also removing duplicated data rows and adding an empty "symbol" row for
# consistency with the published HYDAT format
tryCatch({
  # Flow
  flow <- bind_rows(curr_flow, new_flow) %>% 
    group_by(STATION_NUMBER) %>% 
    distinct(Date, .keep_all = T)
  
  # Level
  level <- bind_rows(curr_level, new_level) %>% 
    group_by(STATION_NUMBER) %>% 
    distinct(Date, .keep_all = T)
}, error = function(e){
  success <<- F
  gen_err <<- "Error while joining stored and downloaded realtime data"
  specific_err <<- e
  print(gen_err)
  print(e)
})

# Checking for an update on the hydat database. If the versions are the same,
# there has been no update, in which case we can simply append the downloaded
# dataset to the hydat dataset. If the versions are different, the hydat_current
# dataset needs to be updated with the newly published data

# Getting publication dates
published_date <- hy_version(paste0(hydat_path,"/Hydat.sqlite3"))$Date
curr_date <- hy_version(paste0(hydat_path,"/Hydat_current.sqlite3"))$Date

# If the dates are not the same
if(published_date != curr_date){
  # Overwriting the "current" dataset with the updated published data by simply
  # copying it over, since the hydat sqlite dataset is always checked for update
  # version at the start of this script
  print("Updating the Hydat_current dataset")
  file.copy(from = paste0(hydat_path, "/Hydat.sqlite3"), 
            to = paste0(hydat_path, "/Hydat_current.sqlite3"), 
            overwrite = T)
} 

# ==== Updating the hydat and the "current" csvs with the new realtime data ====

tryCatch({
  # Reading the currently saved BCflow and level data in the published Hydat dataset 
  BCflow <- hy_daily_flows(hydat_path = paste0(hydat_path,"/Hydat.sqlite3"), prov_terr_state_loc = "BC") %>% tibble()
  BClevel <- hy_daily_levels(hydat_path = paste0(hydat_path,"/Hydat.sqlite3"), prov_terr_state_loc = "BC") %>% tibble()
  
  # Locating all the data that isn't already in the Hydat dataset using an anti-join, to ensure that there are no duplicates. This is the "current" data
  # that we'll be storing
  unpublished_flow <- flow %>% 
    group_by(STATION_NUMBER) %>% 
    anti_join(BCflow, by = c("STATION_NUMBER" = "STATION_NUMBER", "Date" = "Date")) %>% 
    ungroup()
  
  unpublished_level <- level %>% 
    group_by(STATION_NUMBER) %>% 
    anti_join(BClevel, by = c("STATION_NUMBER" = "STATION_NUMBER", "Date" = "Date")) %>% 
    ungroup()
  
  # Transforming the unpublished datasets to the hydat table format
  new_flow_hydat <- format_hydat_flow(unpublished_flow)
  new_level_hydat <- format_hydat_level(unpublished_level)
  
}, error = function(e){
  success <<- F
  gen_err <<- "Error while gathering and formatting the currently unpublished realtime data"
  specific_err <<- e
  print(gen_err)
  print(e)
})

# Appending these data to the hydat data table from the original set, and using
# this to overwrite the tables in the "current" database. The "current" database
# thus contains both the published data as well as the downloaded realtime data

tryCatch({
  # opening file connections
  con_og <- dbConnect(RSQLite::SQLite(), paste0(hydat_path, "/Hydat.sqlite3"))
  con_current <- dbConnect(RSQLite::SQLite(), paste0(hydat_path, "/Hydat_current.sqlite3"))
  
  # reading tables and joining the new rows to them
  temp_flow <- dbReadTable(con_og, "DLY_FLOWS") %>% bind_rows(new_flow_hydat)
  temp_level <- dbReadTable(con_og, "DLY_LEVELS") %>% bind_rows(new_level_hydat)
  
  # Overwriting the updated hydat dataset tables
  dbWriteTable(con_current, "DLY_FLOWS", temp_flow, overwrite = T)
  dbWriteTable(con_current, "DLY_LEVELS", temp_level, overwrite = T)
  
  # Closing file connections
  dbDisconnect(con_og)
  dbDisconnect(con_current)
  
  # Writing the data to csv to update the "current" datasets
  write.csv(unpublished_flow, file=paste0(curr_data_path, "/BCflow_current.csv"))
  write.csv(unpublished_level, file=paste0(curr_data_path, "/BClevel_current.csv"))
}, error = function(e){
  success <<- F
  gen_err <<- "Error while writing the unpublished data to file"
  specific_err <<- e
  print(gen_err)
  print(e)
})

# ==== Updating the log ====

# Adding summary details of the current run to the log file
curr_log <- read.csv("E:/saeeshProjects/BC_Hydrometric_Data_Scraping/hydat_update_log.csv") %>% 
  select(Date, update_status, general_error, specific_error)
update_row <- data.frame(
  "Date" = as.character(Sys.time()),
  "update_status" = ifelse(success, "Successful", "Failed"),
  "general_error" = gen_err,
  "specific_error" = specific_err
)
curr_log <- rbind(update_row, curr_log)
write.csv(curr_log, "E:/saeeshProjects/BC_Hydrometric_Data_Scraping/hydat_update_log.csv", append = F, row.names = F)

# If the run was succesful, updating the current status text file
if(success){
  curr_time <- Sys.time()
  sink("E:/saeeshProjects/BC_Hydrometric_Data_Scraping/curr_update_status.txt", append = F)
  cat("Hydat_current.sqlite status update:\n")
  cat("\n")
  cat("Date of last HYDAT publication (hydat.sqlite): ", as.character(published_date), "\n")
  cat("Last successful realtime update (hydat_current.sqlite): ", as.character(curr_time), "\n")
  cat("\n")
  cat("Update is ahead of published data by ", round(as.numeric(curr_time - published_date), 2), "days")
  sink()
}


