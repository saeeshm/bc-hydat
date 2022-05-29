# !usr/bin/env Rscript

# Author: Saeesh Mangwani
# Date: 2022-05-29

# Description: Options for the initializing the selenium download program

# ==== Setting options ====

# Path to the firefox webdriver (geckodriver)
geckopath <- 'geckodriver/geckodriver'


# Setting up browser options - see
# https://www.browserstack.com/docs/automate/selenium/firefox-profile
extraCaps <- makeFirefoxProfile(
  list(
    ### MAKE SURE THIS POINTS TO THE ZIP DIRECTORY USING AN ABSOLUTE PATH ###
    "browser.download.dir" = normalizePath('data/zip'),
    "browser.download.manager.showWhenStarting" = F,
    "browser.download.manager.focusWhenStarting" = F
  )
)