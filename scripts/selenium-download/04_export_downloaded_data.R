# !usr/bin/env Rscript

# Author: Saeesh Mangwani
# Date: 2020-06-19
# Description: Exporting master datasets following data download

# ==== Loading libraries ====
if (!require("pacman")) install.packages("pacman")
pacman::p_load(tidyverse)

# ==== Exporting Data ====
print("Server closed. Starting data export...")

# Once all the data has been scraped, writing the master datasets to csvs:
for (name in names(masters)){
  path <- paste0("data\\output\\BC",name,"_current.csv")
  write.csv(masters[[name]], path)
}

# Writing the problem and summary tables
write.csv(summ_table, "data\\output\\extraction_summary.csv")
write.csv(distinct(prob_stations), "data\\output\\problem_stations.csv")