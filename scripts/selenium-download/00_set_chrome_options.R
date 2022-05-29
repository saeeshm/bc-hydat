# !usr/bin/env Rscript

# Author: Saeesh Mangwani 
# Date: 2020-06-22 

# Ensure that the default download directory always points to the "zip"
# subdirectory in the data folder using an absolute path. Otherwise the program
# will fail

# Setting chrome driver options to ensure that downloads are redirected to our
# directory of interest (note that the syntax for providing file paths differs
# between Windows and LINUX/UNIX systems. This is the syntax for Windows)


# eCaps <- list(
#   chromeOptions = 
#     list(prefs = list(
#       ### ----- MAKE SURE THIS POINTS TO THE ZIP DIRECTORY USING AN ABSOLUTE PATH ----- ###
#       "download.default_directory" = normalizePath(file.path(getwd(), 'data/zip')),
#       "profile.default_content_settings.popups" = 0L,
#       "download.prompt_for_download" = FALSE,
#       "safebrowsing.disable_download_protection" = TRUE,
#       "plugins.plugins_disabled" = "Chrome PDF Viewer")
#     )
# )