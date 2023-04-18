# !usr/bin/env Rscript

# Author: Saeesh Mangwani
# Date: 2020-06-19
# Description: Exporting master datasets following data download

# ==== Exporting Data ====
print("Starting data export...")

# Once all the data has been scraped, writing the master datasets to csvs:
for (name in names(masters)){
  path <- paste0(paths$selenium_out_path, '/', name, "_current.csv")
  readr::write_csv(masters[[name]], path)
}

# Writing the problem and summary tables
readr::write_csv(summ_table, file.path(paths$selenium_out_path, "extraction_summary.csv"))
readr::write_csv(dplyr::distinct(prob_stations), file.path(paths$selenium_out_path, "problem_stations.csv"))