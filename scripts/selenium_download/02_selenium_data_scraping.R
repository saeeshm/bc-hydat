#!/usr/bin/env Rscript

# Author: Saeesh Mangwani
# Date: 2020-06-19

# Description: Running the Selenium Server to download and organize Hydrometric
# data

# ==== Scraping data for all active stations ====

# From the station table
stations_iter <- stations %>% 
  # Filtering stations that are not active
  filter(STATION_OPERATING_STATUS != 'DISCONTINUED') %>% 
  # Sampling stations if a sample was requested
  sample_n(ifelse(opt$sample > nrow(.), nrow(.), opt$sample)) %>% 
  arrange(STATION_NUMBER) %>% 
  # Getting station IDs as an iterable vector
  pull(STATION_NUMBER)

# Using the iteration variable to loop over all the requested stations
countIter <- 0
for (station in stations_iter) {
  # Trying a download for each station, and skipping that specific station in
  # case there is a data error)
  tryCatch({
    print('Navigating to home url...')
    # Navigating to the page and downloading the data --------
    # Creating the url: Note that we have already defined the date range as well
    # as station id we want through using this url (see setup.R)
    url <- paste0("https://wateroffice.ec.gc.ca/report/real_time_e.html?stn=",
                  station,
                  "&mode=Graph&startDate=",
                  past_date,
                  "&endDate=",
                  curr_date,
                  "&prm1=46&y1Max=&y1Min=&prm2=-1&y2Max=&y2Min=")
    
    # Navigating to the page of interest 
    remDr$navigate(url)
    
    # If the url returns us to the homepage, it means the navigation failed and
    # the data for this station does not exist. Adding the station id to the
    # problem list and skipping the iteration for this station
    if(remDr$getCurrentUrl()[[1]] == "https://wateroffice.ec.gc.ca/search/real_time_e.html"){
      print("Station did not exist or the data cannot be found")
      prob_stations <<- prob_stations %>% bind_rows(list("station_id" = station, "issue" = "No URL found"))
      next
    }
    
    # Checking to see if there is a disclaimer action and if yes, clicking to
    # agree with the conditions. If not, proceeding
    tryCatch({
      webElem <- suppressMessages(remDr$findElement(using = 'name', value = "disclaimer_action"))
      webElem$clickElement()
    },error = function(e){
      invisible()
    },warning = function(w){
      invisible()
    })
    
    # Navigtating to the download button on the url and then clicking on it.
    # Adding to the prob list in case of any issues.
    print('Navigating to download page...')
    tryCatch({
      attempt <- 0
      elemIsDisplayed <- T
      # If the button is successfully clicked or there are more than 3 attempts,
      # breaking the loop
      while(attempt < 4 || elemIsDisplayed){
        tryCatch({
          # Finding the download data button and clicking it
          webElem = remDr$findElement(using = 'id', value = "download")
          remDr$mouseMoveToLocation(webElement = webElem)
          remDr$click()
          # Saving whether the download button is still displayed (i.e has it
          # been successfully clicked or not)
          elemIsDisplayed <<- suppressMessages(webElem$isElementDisplayed()[[1]])
          attempt <<- attempt + 1
        }, error = function(e){
          elemIsDisplayed <<- F
          attempt <<- 5
        })
      }
      if (attempt == 4 && elemIsDisplayed) stop('Exceeded max attempts to try and reach the download page')
    },error = function(e){
      prob_stations <<- prob_stations %>% bind_rows(list("station_id" = station, 
                                                         "issue" = "issue with getting to the download links on the station's page"))
      print(e)
    })
    
    # At the download page, selecting only the elements that give an option for
    # Comma separated values, and getting the download link and the associated
    # headers, so we know what is what. If none are found, adding this station
    # to the problem set and skipping this iteration
    headElem <- remDr$findElements(using = "xpath", '//main//section//h2[not(@id) and not(@class)]')
    fileElem <- remDr$findElements(using = "xpath", '//section//a[text()="Comma Separated Values"]')
    
    if( (length(headElem) == 0) | (length(fileElem) == 0) ){
      prob_stations <<- prob_stations %>% 
        bind_rows(list("station_id" = station, 
                       "issue" = "Unable to find download links/no downloadable data found"))
      next
    }
    
    
    # Extracting the data --------
    
    # Creating a directory for the station we're getting data for (if the
    # directory already exists a warning is printed but nothing new happens
    dir.create(file.path(paths$temp_download_path, station))
    
    # Iterating through all the found links to download and rename the data in a
    # consistent fashion
    print('Downloading files...')
    for(i in seq_along(fileElem)){
      
      # Get the current time
      start_user_time <- as.numeric(format(Sys.time(), "%s"))
      timeVar <- 0
      success <- F
      
      # While the difference between time and the processed time is less than 20
      # and there is no success, trying again. Essentially the point is for
      # Selenium to click again in case the download times out (We set the
      # timeout to 30 seconds). Catching any errors.
      while(timeVar < 30 & success == F){
        # Resetting the time variable to measure the time elapsed from the start
        timeVar <- as.numeric(format(Sys.time(), "%s")) - start_user_time
        error <- F
        # Clicking to download the zip file associated with this dataset
        tryCatch({
          fileElem[[i]]$clickElement()
          Sys.sleep(1)
        },error = function(e){
          # If there is an error with the page, adding to the prob stations list
          prob_stations <<- prob_stations %>% 
            bind_rows(list("station_id" = station, 
                           "issue" = paste("issue with downloading data file for dataset number", i)))
          # In case we've been redirected to an error page we need to go back to
          # the download page (to not also break the next iteration), so
          # checking to see if any head elements have been found and asking to
          # keep going back until some are found
          while(length(remDr$findElements(using = "xpath", '//main//section//h2')) == 0){
            remDr$goBack()
          }
          # Getting the download links again
          headElem <<- remDr$findElements(using = "xpath", '//main//section//h2[not(@id) and not(@class)]')
          fileElem <<- remDr$findElements(using = "xpath", '//section//a[text()="Comma Separated Values"]')
          
          # Editing the necessary variables to skip this iteration in the loop
          error <<- T
        }, warning = function(w){
          invisible()
        })
        
        # Checking for the error variable to help us skip this iteration in case
        # of an error
        if(error){
          break
        }
        
        # Creating a temporary time variable
        temp_time <- as.numeric(format(Sys.time(), "%s"))
        
        # Waiting for the download to complete. Thus waiting either for all
        # these conditions to be met or for the time limit of 10s to elapse
        while( (length(list.files(path = paths$temp_zip_path)) == 0) & (as.numeric(format(Sys.time(), "%s")) - temp_time) < 15) invisible()
        
        # If a file is downloaded and it is not a timeout - 
        if((length(list.files(path = paths$temp_zip_path)) > 0)){
          # print("got a file") Waiting for the complete download by checking
          # for the right extension - first creating some helper variables
          downloaded <- F
          temp_time <- as.numeric(format(Sys.time(), "%s"))
          # As long as there is not an error in downloading or there is no
          # timeout, try again
          while(!downloaded & (as.numeric(format(Sys.time(), "%s")) - temp_time) < 30 ){
            tryCatch({
              while( (length(list.files(path = paths$temp_zip_path)) == 0) || !str_detect(list.files(path = paths$temp_zip_path), "(.zip)$")) {
                invisible()
              }
              # If we make it through the try block, resetting the success
              # variable such that we can break out of the parent loop
              print("File successfully downloaded")
              downloaded <- T
            },error = function(e){
              print("Download timeout, trying again")
              print(e)
            })
          }
          
          # Indicating success if the loop clears, since there has either been a
          # successful download or a timeout and in either case we want to move
          # on
          success <- T
        }else{
          # Resetting the time variable to measure the time elapsed from the
          # start and allowing the loop to try clicking again
          timeVar <- as.numeric(format(Sys.time(), "%s")) - start_user_time
          # print(paste0("time elapsed: ", timeVar))
        }
      }
      
      # If the loop times out and there is still no zip file, flagging this
      # station id and skipping out of this iteration
      if ((length(list.files(path = paths$temp_zip_path)) == 0)){
        prob_stations <<- prob_stations %>% 
          bind_rows(prob_stations, list("station_id" = station,
                                        "issue" = "File download timeout, file still exists"))
        next
      }
      
      # Once the file is downloaded, unzipping the file to a directory named
      # with the station Id to get the csv for this data
      utils::unzip(zipfile = file.path(
        paths$temp_zip_path, 
        list.files(path = paths$temp_zip_path, pattern = ".zip$")
      ), 
      exdir = file.path(paths$temp_download_path, station), 
      overwrite = T)
      # print("File unzipped")
      
      # Clearing the zip folder to prepare for the next iteration
      file.remove(paste0(paths$temp_zip_path, '/', list.files(path = paths$temp_zip_path)))
      # print("Zip File removed")
      
      # Renaming the extracted data files to clarify the attribute they contain
      # data for using the header elements extracted before
      oldfile <- list.files(path = file.path(paths$temp_download_path, station), pattern = station)
      file.rename(from = file.path(paths$temp_download_path, station, oldfile), 
                  to = paste0(paths$temp_download_path, '/', 
                              station, '/', 
                              headElem[[i]]$getElementText(),".csv"))
      # print("Data file renamed")
    }
    
    # Formatting, processing and storing the data --------
    print('Formatting downloaded data and saving...')
    # Reading in each each csv that was generated and assigning it to a new
    # variable Getting the number of files pulled, their names and a generated
    # list of object names to be associated with each name
    n <- list.files(path = file.path(paths$temp_download_path, station), pattern = "csv") %>% 
      length()
    fileNames <- list.files(path = file.path(paths$temp_download_path, station), pattern = "csv")
    varNames <- transformNames(fileNames)
    
    # Iteratively reading in each dataset and assigning it to the correct
    # variable (note that we skip the first 9 rows because these don't contain
    # any data)
    fileNames <- paste0(paths$temp_download_path, '/', station, '/', fileNames)
    for (i in 1:n){
      assign(varNames[i], read_csv(fileNames[i], skip = 9, col_types = cols()))
    }
    
    # If all the datasets are empty flagging this as a problem station and
    # moving on
    if (all(map_dbl(map(varNames, get), nrow) == 0, na.rm = T)){
      prob_stations <<- prob_stations %>% 
        bind_rows(prob_stations, list("station_id" = station,
                                      "issue" = "All data files are empty"))
      next
    }
    
    # For each of these datasets, calling a function (see Setup.R) which formats
    # them such that they contain DAILY mean values and have appropriate column
    # names
    for (name in varNames){
      assign(name, formatDataAsDaily(get(name), station, name))
    }
    
    # Appending each to the correct master dataset  using the "masters" list
    # object - a list that stores all of the master dataframes (see Setup.R)
    for (name in varNames){
      # if the dataframe already exists, then appending to it
      if(!is.null(masters[[name]])){
        masters[[name]] <- masters[[name]] %>% bind_rows(get(name))
        # otherwise creating it
      }else{
        masters[[name]] <- get(name)
      }
    }
    
    # Adding the summary of extracted data for this station to the summary table
    row <- list("station_id" = station,
                "tables_extracted" = paste(fileNames, collapse = ", "))
    summ_table <- summ_table %>% bind_rows(row)
    
    # Priting a status update and incrementing the counter variable
    countIter <<- countIter + 1
    print(paste0("Scraped: ", round((countIter/length(stations_iter)) * 100, 2), "% of total data"))
  },error = function(e){
    # In case of an error, adding the station to problem stations and breaking
    # the loop to go to the next iteration
    prob_stations <<- prob_stations %>% 
      bind_rows(list("station_id" = station, 
                     "issue" = paste("A fatal error occured during data download for this station")))
  })
}
