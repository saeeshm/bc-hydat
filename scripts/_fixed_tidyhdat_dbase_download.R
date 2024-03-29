# Author: Saeesh Mangwani
# Date: 2022-08-15

# Description: A rewrite of the download_hydat function in the tidyhydat library
# that first checks whether a local Hydat database already exists and only
# downloads a new one if the download version is newer than the local one.

# ==== Loading libraries ====
library(tidyhydat)

# ==== Helper functions ====

# A customized  download_hydat function to take an argument for
# whether or not download a new hydat dataset rather than ask the user in the
# interactive prompt (prevents stoppage when running from the command line)
my_download_hydat <- function(dl_hydat_here = NULL, ask = TRUE) {
  if(is.null(dl_hydat_here)){
    dl_hydat_here <- hy_dir()
  } else {
    if (!dir.exists(dl_hydat_here)) {
      dir.create(dl_hydat_here)
      message(crayon::blue("You have downloaded hydat to", dl_hydat_here))
      message(crayon::blue("See ?hy_set_default_db to change where tidyhydat looks for HYDAT"))
    }
  }
  
  if (!is.logical(ask)) stop("Parameter ask must be a logical")
  
  
  ## Create actual hydat_path
  hydat_path <- file.path(dl_hydat_here, "Hydat.sqlite3")
  
  ## If there is an existing hydat file get the date of release
  if (file.exists(hydat_path)) {
    hy_version(hydat_path) %>%
      dplyr::mutate(condensed_date = paste0(
        substr(.data$Date, 1, 4),
        substr(.data$Date, 6, 7),
        substr(.data$Date, 9, 10)
      )) %>%
      dplyr::pull(.data$condensed_date) -> existing_hydat
  } else {
    existing_hydat <- "HYDAT not present"
  }
  
  
  ## Create the link to download HYDAT
  base_url <-
    "https://collaboration.cmc.ec.gc.ca/cmc/hydrometrics/www/"
  
  # Run network check
  network_check(base_url)
  
  x <- httr::GET(base_url)
  httr::stop_for_status(x)
  new_hydat <- substr(gsub("^.*\\Hydat_sqlite3_", "",
                           httr::content(x, "text")), 1, 8)
  #Make the download URL
  url <- paste0(base_url, "Hydat_sqlite3_", new_hydat, ".zip")
  response = httr::HEAD(url)
  size <- round(as.numeric(httr::headers(response)[["Content-Length"]])/1000000, 0)
  
  
  ## Do we need to download a new version?
  if (new_hydat == existing_hydat) { #DB exists and no new version
    if(ask){
      dl_overwrite <- ask(paste0("The existing local version of HYDAT, published on ", lubridate::ymd(existing_hydat), ", is the most recent version available.  \nDo you wish to overwrite it?  \nDownloading HYDAT could take up to 10 minutes (", size, " MB)."))
    }else{
      return(
        paste0("The existing local version of HYDAT, published on ", 
               lubridate::ymd(existing_hydat), 
               ", is the most recent version available. Skipping download...")
        )
    } 
  } else {
    dl_overwrite <- TRUE
  }
  
  if (!dl_overwrite){
    info("HYDAT is updated on a quarterly basis, check again soon for an updated version.")
  }
  
  if (new_hydat != existing_hydat & ask) { #New DB available or no local DB at all
    ans <- ask(paste0("Downloading HYDAT will take up to 10 minutes (", size, " MB).  \nThis will remove any older versions of HYDAT, if applicable.  \nIs that okay?"))
  } else {
    ans <- TRUE
  }
  
  if (!ans) {
    stop("Maybe another day...", call. = FALSE)
  } else if (dl_overwrite) {
    green_message(paste0("Downloading HYDAT to ", normalizePath(dl_hydat_here)))
  }
  
  
  if (dl_overwrite){
    if (new_hydat == existing_hydat){
      info(paste0("Your local copy of HYDAT published on ", crayon::blue(lubridate::ymd(new_hydat)), " will be overwritten."))
    } else {
      info(paste0("Downloading new version of HYDAT created on ", crayon::blue(lubridate::ymd(new_hydat))))
    }
    
    ## temporary path to save
    tmp <- tempfile("hydat_", fileext = ".zip")
    
    ## Download the zip file
    res <- httr::GET(url, httr::write_disk(tmp), httr::progress("down"), 
                     httr::user_agent("https://github.com/ropensci/tidyhydat"))
    on.exit(file.remove(tmp), add = TRUE)
    httr::stop_for_status(res)
    
    ## Extract the file to a temporary dir
    if(file.exists(tmp)) info("Extracting HYDAT")
    tempdir <- paste0(tempdir(), "/extracted")
    dir.create(tempdir)
    utils::unzip(tmp, exdir = tempdir, overwrite = TRUE)
    on.exit(unlink(tempdir, recursive=TRUE))
    
    ## Move to final resting place and rename to consistent name
    file.rename(
      list.files(tempdir, pattern = "\\.sqlite3$", full.names = TRUE),
      hydat_path
    )
    
    
    if (file.exists(hydat_path)) {
      congrats("HYDAT successfully downloaded")
    } else {
      not_done("HYDAT not successfully downloaded")
    }
    
    hy_check <- function(hydat_path = NULL) {
      con <- hy_src(hydat_path)
      on.exit(hy_src_disconnect(con), add = TRUE)
      
      have_tbls <- dplyr::src_tbls(con)
      
      tbl_diff <- setdiff(hy_expected_tbls(), have_tbls)
      if (!rlang::is_empty(tbl_diff)) {
        red_message("The following tables are missing from HYDAT")
        red_message(paste0(tbl_diff, "\n"))
      }
      
      
      invisible(lapply(have_tbls, function(x) {
        tbl_rows <- dplyr::tbl(con, x) %>% 
          utils::head(1) %>% 
          dplyr::collect() %>% 
          nrow()
        
        if(tbl_rows == 0) {
          red_message(paste0(x, " table has no data."))
        } 
      }))
    }
    
    hy_check(hydat_path)
    
  } #End of DL and overwrite if statement
}

# Passing the custom function to the tidyhydat namespace to allow it use all of
# the namespace dependencies when being called
environment(my_download_hydat) <- asNamespace('tidyhydat')
assignInNamespace("download_hydat", my_download_hydat, ns = "tidyhydat")