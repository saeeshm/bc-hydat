# !usr/bin/env Rscript

# Author: Saeesh Mangwani
# Date: 2022-05-29

# Description: Helper functions for the selenium download program

# ==== Loading libraries ====
library(dplyr)
library(stringr)

# ==== Functions ====

# Takes a vector of filenames for the downloaded csvs and returns a transformed
# set of object names (names that are easier to work with as objects in R)
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
formatDataAsDaily <- function(df, station, varName){
  df <- df %>% 
    select(Date = contains("Date"), Parameter, 
           Value = contains("Value"), everything()) %>% 
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
