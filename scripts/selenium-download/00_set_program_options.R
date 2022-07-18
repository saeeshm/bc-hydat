# !usr/bin/env Rscript

# Author: Saeesh Mangwani
# Date: 2022-05-29

# Description: Options for the initializing the selenium download program

# ==== Setting options ====

# Setting up browser options - see
# https://www.browserstack.com/docs/automate/selenium/firefox-profile
library(RSelenium)
extraCaps <- RSelenium::makeFirefoxProfile(
  list(
    # Tells firefox to default downloads to a custom folder -
    # https://stackoverflow.com/questions/37154746/how-to-avoid-window-download-popup-in-firefox-use-java-selenium-i-need-download
    "browser.downLoad.folderList" = 2,
    # Path to custom download folder - MAKE SURE THIS POINTS TO THE ZIP
    # DIRECTORY USING AN ABSOLUTE PATH
    "browser.download.dir" = '/home/seluser/Downloads',
    # Mix of options to tell firefox to disable the download helper 
    "browser.helperApps.neverAsk.saveToDisk" = "application/zip, text/csv",
    "browser.helperApps.alwaysAsk.force" = F,
    "browser.download.panel.shown" = F,
    "browser.download.manager.showWhenStarting" = F,
    "browser.download.manager.alertOnEXEOpen" = F,
    "browser.download.manager.useWindow" = F,
    "browser.download.manager.showAlertOnComplete" = F,
    "browser.download.manager.focusWhenStarting" = F
  )
)

