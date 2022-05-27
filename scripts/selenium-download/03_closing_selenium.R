# !usr/bin/env Rscript

# Author: Saeesh Mangwani
# Date: 2020-06-19
# Description: Closing the data downloading program

# ==== Loading libraries ====
if (!require("pacman")) install.packages("pacman")
pacman::p_load(tidyverse)

# ==== Closing the Selenium server since we're done scraping ====

# Closing the window and stopping the server
remDr$close()
rD$server$stop()
# Removing the server object and garbage collecting
rm(rD)
gc()
# Killing the java process that runs it
system("taskkill /im java.exe /f", intern=FALSE, ignore.stdout=FALSE)

# ==== Clearing the temporary download and zip folders ====
unlink("data\\zip", recursive = T)
dir.create("data\\zip")

unlink("data\\download", recursive = T)
dir.create("data\\download")
