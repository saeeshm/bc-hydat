# !usr/bin/env Rscript

# Author: Saeesh Mangwani
# Date: 2020-06-25
# Description: Helper functions for the BC hydat download and scraping programs

# ==== Loading libraries ====
library(dplyr)
library(tidyr)
library(stringr)
library(tidyhydat)
library(DBI)
library(RPostgres)

# ==== Helper functions ====

# Checks whether the current year is a leap year
is_leap <- function(year){
  if((year %% 4) == 0) {
    if((year %% 100) == 0) {
      if((year %% 400) == 0) {
        return(TRUE)
      } else {
        return(FALSE)
      }
    } else {
      return(TRUE)
    }
  } else {
    return(FALSE)
  }
}

# Formating flow data to Hydat.sqlite file specifications
format_hydat_flow <- function(df){
  vars <- c("STATION_NUMBER", "YEAR", "MONTH", 
            "FULL_MONTH", "NO_DAYS", "MONTHLY_MEAN", "MONTHLY_TOTAL", 
            "FIRST_DAY_MIN", "MIN", "FIRST_DAY_MAX", "MAX", 
            "FLOW1", "FLOW_SYMBOL1", "FLOW2", "FLOW_SYMBOL2", 
            "FLOW3", "FLOW_SYMBOL3", "FLOW4", "FLOW_SYMBOL4", 
            "FLOW5", "FLOW_SYMBOL5", "FLOW6", "FLOW_SYMBOL6", 
            "FLOW7", "FLOW_SYMBOL7", "FLOW8", "FLOW_SYMBOL8", 
            "FLOW9", "FLOW_SYMBOL9", "FLOW10", "FLOW_SYMBOL10", 
            "FLOW11", "FLOW_SYMBOL11", "FLOW12", "FLOW_SYMBOL12", 
            "FLOW13", "FLOW_SYMBOL13", "FLOW14", "FLOW_SYMBOL14", 
            "FLOW15", "FLOW_SYMBOL15", "FLOW16", "FLOW_SYMBOL16", 
            "FLOW17", "FLOW_SYMBOL17", "FLOW18", "FLOW_SYMBOL18", 
            "FLOW19", "FLOW_SYMBOL19", "FLOW20", "FLOW_SYMBOL20", 
            "FLOW21", "FLOW_SYMBOL21", "FLOW22", "FLOW_SYMBOL22", 
            "FLOW23", "FLOW_SYMBOL23", "FLOW24", "FLOW_SYMBOL24", 
            "FLOW25", "FLOW_SYMBOL25", "FLOW26", "FLOW_SYMBOL26", 
            "FLOW27", "FLOW_SYMBOL27", "FLOW28", "FLOW_SYMBOL28")
  
  df <- df %>%
    separate(Date, sep = "-", into = c("YEAR", "MONTH", "DAY")) %>% 
    group_by(STATION_NUMBER, YEAR, MONTH) %>% 
    spread(DAY, Value)
  
  # Getting the number of days for which there are data by seeing how many names
  # associated with day-number have been generated in the spread process
  num_names <- length(names(df)[5:length(df)])
  
  # if num_names is more than 28 (the lowest possible num_names), createing
  # extra column names to be stored in the vars vector, corresponding to the
  # number of days in this month
  if(num_names > 28){
    flws <- paste0("FLOW", 29:num_names)
    flw_syms <- paste0("FLOW_SYMBOL", 29:num_names)
    vars <- append(vars, c(flws, flw_syms))
  }
  
  # Renaming the spread variables
  names(df)[5:length(df)] <- paste0("FLOW", 1:num_names)
  
  # Reshaping the dataframe to match hydat format
  df <- df %>% 
    # Getting number of days based on the month
    mutate(NO_DAYS = case_when(as.numeric(MONTH) %in% c(1, 3, 5, 7, 8, 10, 12) ~ 31,
                               as.numeric(MONTH) %in% c(4, 6, 8, 9, 11) ~ 30,
                               as.numeric(MONTH) == 2 & is_leap(as.numeric(YEAR)) ~ 29,
                               TRUE ~ 28)) %>% 
    # Can't calculate first-day min and max from available data so setting to NA
    mutate(FIRST_DAY_MIN = NA_integer_) %>% 
    mutate(FIRST_DAY_MAX = NA_integer_) %>% 
    rowwise() %>% 
    mutate(across(contains("FLOW"), 
                  ~ NA_character_, 
                  .names = paste0("FLOW_SYMBOL{1:",num_names,"}"))) %>% 
    mutate(
      MIN = ifelse(
        is.infinite(min(c_across(FLOW1:paste0("FLOW",num_names)), na.rm = T)), 
        NA_real_, 
        min(c_across(FLOW1:paste0("FLOW", num_names)), na.rm = T)
      ),
      MAX = ifelse(
        is.infinite(max(c_across(FLOW1:paste0("FLOW",num_names)), na.rm = T)), 
        NA_real_, 
        max(c_across(FLOW1:paste0("FLOW",num_names)), na.rm = T)
      ),
      MONTHLY_TOTAL = sum(c_across(FLOW1:paste0("FLOW",num_names)), na.rm = T),
      MONTHLY_MEAN = ifelse(
        is.nan(mean(c_across(FLOW1:paste0("FLOW",num_names)), na.rm = T)), 
        NA_real_, 
        mean(c_across(FLOW1:paste0("FLOW",num_names)), na.rm = T)
      ),
      FULL_MONTH = if_else(
        (31 - NO_DAYS) == sum(is.na(c_across(FLOW1:paste0("FLOW",num_names)))), 
        1, 
        0
      )
    ) %>% 
    ungroup() %>% 
    select(all_of(vars)) %>% 
    mutate(YEAR = as.integer(YEAR),
           MONTH = as.integer(MONTH))
  return(df)
}

# Formatting level data to Hydat.sqlite file specifications
format_hydat_level <- function(df){
  vars <- c("STATION_NUMBER", "YEAR", "MONTH", 
            "FULL_MONTH", "NO_DAYS", "MONTHLY_MEAN", "MONTHLY_TOTAL", 
            "FIRST_DAY_MIN", "MIN", "FIRST_DAY_MAX", "MAX", 
            "LEVEL1", "LEVEL_SYMBOL1", "LEVEL2", "LEVEL_SYMBOL2", 
            "LEVEL3", "LEVEL_SYMBOL3", "LEVEL4", "LEVEL_SYMBOL4", 
            "LEVEL5", "LEVEL_SYMBOL5", "LEVEL6", "LEVEL_SYMBOL6", 
            "LEVEL7", "LEVEL_SYMBOL7", "LEVEL8", "LEVEL_SYMBOL8", 
            "LEVEL9", "LEVEL_SYMBOL9", "LEVEL10", "LEVEL_SYMBOL10", 
            "LEVEL11", "LEVEL_SYMBOL11", "LEVEL12", "LEVEL_SYMBOL12", 
            "LEVEL13", "LEVEL_SYMBOL13", "LEVEL14", "LEVEL_SYMBOL14", 
            "LEVEL15", "LEVEL_SYMBOL15", "LEVEL16", "LEVEL_SYMBOL16", 
            "LEVEL17", "LEVEL_SYMBOL17", "LEVEL18", "LEVEL_SYMBOL18", 
            "LEVEL19", "LEVEL_SYMBOL19", "LEVEL20", "LEVEL_SYMBOL20", 
            "LEVEL21", "LEVEL_SYMBOL21", "LEVEL22", "LEVEL_SYMBOL22", 
            "LEVEL23", "LEVEL_SYMBOL23", "LEVEL24", "LEVEL_SYMBOL24", 
            "LEVEL25", "LEVEL_SYMBOL25", "LEVEL26", "LEVEL_SYMBOL26", 
            "LEVEL27", "LEVEL_SYMBOL27", "LEVEL28", "LEVEL_SYMBOL28")
  
  df <- df %>%
    separate(Date, sep = "-", into = c("YEAR", "MONTH", "DAY")) %>% 
    group_by(STATION_NUMBER, YEAR, MONTH) %>% 
    spread(DAY, Value)
  
  # Getting the number of days for which there are data by seeing how many names
  # associated with day-number have been generated in the spread process
  num_names <- length(names(df)[5:length(df)])
  
  # if num_names is more than 28 (the lowest possible num_names), createing
  # extra column names to be stored in the vars vector, corresponding to the
  # number of days in this month
  if(num_names > 28){
    levs <- paste0("LEVEL", 29:num_names)
    lev_syms <- paste0("LEVEL_SYMBOL", 29:num_names)
    vars <- append(vars, c(levs, lev_syms))
  }
  
  # Renaming the spread variables
  names(df)[5:length(df)] <- paste0("LEVEL", 1:num_names)
  
  # Reshaping the dataframe to match hydat format
  df <- df %>% 
    # Getting number of days based on the month
    mutate(NO_DAYS = case_when(as.numeric(MONTH) %in% c(1, 3, 5, 7, 8, 10, 12) ~ 31,
                               as.numeric(MONTH) %in% c(4, 6, 8, 9, 11) ~ 30,
                               as.numeric(MONTH) == 2 & is_leap(as.numeric(YEAR)) ~ 29,
                               TRUE ~ 28)) %>% 
    # Can't calculate first-day min and max from available data so setting to NA
    mutate(FIRST_DAY_MIN = NA_integer_) %>% 
    mutate(FIRST_DAY_MAX = NA_integer_) %>% 
    rowwise() %>% 
    mutate(across(contains("LEVEL"), 
                  ~ NA_character_, 
                  .names = paste0("LEVEL_SYMBOL{1:",num_names,"}"))) %>% 
    mutate(
      MIN = ifelse(
        is.infinite(min(c_across(LEVEL1:paste0("LEVEL",num_names)), na.rm = T)), 
        NA_real_, 
        min(c_across(LEVEL1:paste0("LEVEL",num_names)), na.rm = T)
      ),
      MAX = ifelse(
        is.infinite(max(c_across(LEVEL1:paste0("LEVEL",num_names)), na.rm = T)), 
        NA_real_, 
        max(c_across(LEVEL1:paste0("LEVEL",num_names)), na.rm = T)
      ),
      MONTHLY_TOTAL = sum(c_across(LEVEL1:paste0("LEVEL",num_names)), na.rm = T),
      MONTHLY_MEAN = ifelse(
        is.nan(mean(c_across(LEVEL1:paste0("LEVEL",num_names)), na.rm = T)), 
        NA_real_, 
        mean(c_across(LEVEL1:paste0("LEVEL",num_names)), na.rm = T)
      ),
      FULL_MONTH = if_else(
        (31 - NO_DAYS) == sum(is.na(c_across(LEVEL1:paste0("LEVEL",num_names)))), 
        1, 
        0
      )
    ) %>% 
    ungroup() %>% 
    select(all_of(vars)) %>% 
    mutate(YEAR = as.integer(YEAR),
           MONTH = as.integer(MONTH))
  return(df)
}

# A customized  download_hydat function to take an argument for
# whether or not download a new hydat dataset rather than ask the user in the
# interactive prompt (prevents stoppage when running from the command line)
my_download_hydat <- function (dl_hydat_here = NULL, download_new = T){
  if (is.null(dl_hydat_here)) {
    dl_hydat_here <- hy_dir()
  }else {
    if (!dir.exists(dl_hydat_here)) {
      dir.create(dl_hydat_here)
      message(crayon::blue("You have downloaded hydat to", 
                           dl_hydat_here))
      message(crayon::blue("See ?hy_set_default_db to change where tidyhydat looks for HYDAT"))
    }
  }
  on.exit(closeAllConnections())
  ans <- download_new
  if (!ans) {
    stop("Maybe another day...", call. = FALSE)
  }
  info(paste0("Downloading HYDAT.sqlite3 to ", crayon::blue(dl_hydat_here)))
  hydat_path <- file.path(dl_hydat_here, "Hydat.sqlite3")
  if (file.exists(hydat_path)) {
    existing_hydat <- hy_version(hydat_path) %>% 
      dplyr::mutate(condensed_date = paste0(substr(.data$Date,1, 4), 
                                            substr(.data$Date, 6, 7), 
                                            substr(.data$Date, 9, 10))) %>% 
      dplyr::pull(.data$condensed_date)
  }else {
    existing_hydat <- "HYDAT not present"
  }
  base_url <- "http://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/"
  network_check(base_url)
  x <- httr::GET(base_url)
  httr::stop_for_status(x)
  new_hydat <- substr(gsub("^.*\\Hydat_sqlite3_", "", 
                           httr::content(x, "text")), 1, 8)
  if (new_hydat == existing_hydat) {
    handle_error(stop(not_done(paste0("The existing local version of hydat, published on ", 
                                      lubridate::ymd(existing_hydat), ", is the most recent version available."))))
    return(invisible(T))
  }else {
    info(paste0("Downloading version of HYDAT created on ", 
                crayon::blue(lubridate::ymd(new_hydat))))
  }
  
  url <- paste0(base_url, "Hydat_sqlite3_", new_hydat, 
                ".zip")
  tmp <- tempfile("hydat_")
  res <- httr::GET(url, httr::write_disk(tmp), httr::progress("down"), 
                   httr::user_agent("https://github.com/ropensci/tidyhydat"))
  on.exit(file.remove(tmp))
  httr::stop_for_status(res)
  if (file.exists(tmp)) {
    info("Extracting HYDAT")
  }
  
  utils::unzip(tmp, exdir = dl_hydat_here, overwrite = TRUE)
  
  if (file.exists(hydat_path)) {
    congrats("HYDAT successfully downloaded")
  }else{
    (not_done("HYDAT not successfully downloaded"))
  }
  return(invisible(TRUE))
}

# Passing the custom function to the tidyhydat namespace to allow it use all of
# the namespace dependencies when being called
environment(my_download_hydat) <- asNamespace('tidyhydat')
assignInNamespace("download_hydat", my_download_hydat, ns = "tidyhydat")

# Resets the hydat database with a newly published version of the hydat.sqlite
# database
reset_hydat_postgres <- function(conn, creds, hydat_path){
  
  print('Reading published flow and level data...')
  flow <- hy_daily_flows(hydat_path = paste0(hydat_path,"/Hydat.sqlite3"), 
                         prov_terr_state_loc = "BC") %>% tibble()
  level <- hy_daily_levels(hydat_path = paste0(hydat_path,"/Hydat.sqlite3"), 
                           prov_terr_state_loc = "BC") %>% tibble()
  
  
  print('Dropping existing tables and schema...')
  dbExecute(conn, 'drop table if exists bchydat.flow')
  dbExecute(conn, 'drop table if exists bchydat.level')
  dbExecute(conn, 'drop schema if exists bchydat ')
  
  print('Re-creating schema...')
  dbExecute(conn, 'create schema bchydat')
  dbExecute(conn, paste0('grant all on schema ', creds$schema,
                         ' to postgres, ', creds$user, ';'))
  
  print('Posting newly published hydat data...')
  dbWriteTable(conn, 
               DBI::Id(schema = creds$schema, table = "flow"),
               # Adding a column indicating publication status
               flow %>% mutate(pub_status = 'Published'),
               append = F,
               overwrite = T)
  dbWriteTable(conn, 
               DBI::Id(schema = creds$schema, table = "level"),
               # Adding a column indicating publication status
               level %>% mutate(pub_status = 'Published'),
               append = F,
               overwrite = T)
  print('Reset complete!')
}
