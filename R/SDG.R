# function to format from OCADS to SDG14.3.1 format
# TODO update so that it can go from BCD to SDG


# update data format from OCADS to SDG



#' Convert data from OCADS to SDG
#'
#' @param OCADS_fn a file path to an OCADS format data file (csv)
#'
#' @return
#' @export
#'
convert_SDG <- function(OCADS_fn) {
  require(tidyverse)
  require(readxl)
  require(xlsx)

# load in data
# get templates
# load in sdg template
sdg_tmp <- read_xlsx("SDG_14_3_1-data_submission-2019.xlsx", sheet = "Example Template_discrete")
# load in data map
data_map <- read_xlsx("ocads_sdg_data.xlsx")

  # ocads_data <- read_csv('data/OCADS/HUD2021127/Export/18HU21127_data.csv', show_col_types = FALSE)
  ocads_data <- read_csv(OCADS_fn, show_col_types = FALSE, col_types = cols(.default = "c"))

  # initialize dataframe
  sdg_dat <- sdg_tmp %>%
    filter(!row_number() %in% c(1:4)) %>%
    add_row(., MOORING_NAME = rep(NA, length(ocads_data[[1]])))


  for (i in 1:length(names(ocads_data))) {
    datcol <- names(ocads_data)[i]

    # update data column names (where sdg template exists)
    if (datcol %in% data_map$ocads_name) {
      sdgdatcol <-
        na.omit(data_map$sdg_name[data_map$ocads_name == datcol])
      sdg_dat <- sdg_dat %>%
        dplyr::mutate(.,!!sdgdatcol := ocads_data[[datcol]])
    } else {
      # TODO add data columns where sdg template does not exist based on formula
      # replace '_FLAG_W' with " FLAG"
      if (length(grep(datcol, pattern = 'flag', ignore.case = TRUE)) != 0) {
        # fix flag name scheme
        sdgdatcol <-
          gsub(pattern = "_FLAG_W",
               replacement = " FLAG",
               x = datcol)
      } else {
        sdgdatcol <- datcol
      }
      sdg_dat <- sdg_dat %>%
        dplyr::mutate(.,!!sdgdatcol := ocads_data[[datcol]])

    }


  } # end loop through ocads columns

  # format time
  if(length(grep(sdg_dat$TIME_UTC, pattern = ':')) == 0) {
    sdg_dat$TIME_UTC <- format(strptime(sdg_dat$TIME_UTC, "%H%M"), "%H:%M")
  }
  if (length(str_split(sdg_dat$TIME_UTC, ':')[[4]]) == 3) {
    sdg_dat$TIME_UTC <- str_pad(sdg_dat$TIME_UTC, width = 8, pad = '0')
  }
  if (length(unique(sdg_dat$TIME_UTC)) == 1) {
    if (is.na(unique(sdg_dat$TIME_UTC))) {
      # try btl_time
      sdg_dat$TIME_UTC <- ocads_data$BTL_TIME
    }
  }

  if (length(grep(sdg_dat$DATE_UTC, pattern = '/')) != 0){
    warning("Incorrect date format found! Attempted to correct!")
    sdg_dat$DATE_UTC <- format(strptime(sdg_dat$DATE_UTC, '%m/%d/%Y'), '%Y-%m-%d')

  }
  if (any(nchar(sdg_dat$DATE_UTC) < 10, na.rm = TRUE)) {
    sdg_dat$DATE_UTC <- as.character(format(as.Date(sdg_dat$DATE_UTC), '%Y-%m-%d'))

  }

  # if there is no depth column, make one from pressure
  if (anyNA(sdg_dat$DEPTH_SAMPLING[3:nrow(sdg_dat)])) {
    warning('Generating depth from pressure!')
    sdg_dat$DEPTH_SAMPLING <- oce::swDepth(pressure = as.numeric(ocads_data$CTDPRS))
  }
  # fix dots and dashes in column names
  names(sdg_dat) <- gsub(names(sdg_dat), pattern = "/.", replacement = '-')



  # write as xlsx format
  exdir <- paste0('data/SDG/', sdg_dat$MOORING_NAME[3], '/')
  dir.create(exdir, recursive = TRUE, showWarnings = FALSE)
  write.xlsx(sdg_dat, file = file.path(exdir, paste0(sdg_dat$MOORING_NAME[3], "_data_SDG.xlsx")))

  cat("SDG data file created:", file.path(exdir, paste0(sdg_dat$MOORING_NAME[3], "_data_SDG.xlsx")))

}
