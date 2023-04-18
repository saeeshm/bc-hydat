# !usr/bin/env Rscript

# Author: Saeesh Mangwani
# Date: 2020-06-19
# Description: Closing the data downloading program

# ==== Closing the Selenium server since we're done scraping ====

# Closing the window and stopping the server
remDr$close()
system("docker stop hydat_selenium_scraper")

# ==== Clearing the temporary download and zip folders ====
unlink(paths$temp_zip_path, recursive = T)
dir.create(paths$temp_zip_path)

unlink(paths$temp_download_path, recursive = T)
dir.create(paths$temp_download_path)
