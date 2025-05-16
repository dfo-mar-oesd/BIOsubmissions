# function to format from BCD to OCADS


#' Convert BCD to OCADS format
#'
#' @param data a BCD file with columns
#' - MISSION_DESCRIPTOR (ICES Country code, ICES ship code, two digit year, unique 3 digit ID eg - 18QL23573)
#' - DATA_TYPE_METHOD (BioChem data type - biochem.BCDATATYPES)
#' - DIS_DETAIL_DATA_VALUE
#' - DIS_DETAIL_DATA_QC_CODE (0-9 BioChem Flag value)
#' - DIS_DETAIL_COLLECTOR_SAMP_ID (6 digit unique ID)
#' - EVENT_COLLECTOR_STN_NAME
#' - EVENT_COLLECTOR_EVENT_ID
#' - DIS_HEADER_SDATE (Date, format expected: %m/%d/%Y)
#' - DIS_HEADER_STIME (Time, format expected: HHMM)
#' - DIS_HEADER_SLAT (Latitude)
#' - DIS_HEADER_SLON (Longitude)
#' - DIS_HEADER_START_DEPTH (Depth - METERS)
#'
#' @param biochem.password chr, string with your BioChem password
#' @param biochem.username chr, string with your BioChem username
#'
#' Note: A bioChem connection is used in this function to pull the sounding data
#' which is not typically included in the BCD format. Future development should
#' include providing an alternative solution, such as a csv sounding file input
#' or the option to skip over an attempt to connect to BioChem and produce a data
#'  file without SOUNDING (although there should be a user warning)
#'
#' @return a dataframe formatted for upload to OCADS
#' @export
#'
#' @example
#' source("C:/users/ogradye/desktop/biochem_creds.R")
#' data <- read_csv("C:/Users/ogradye/Documents/AZMP_template/AZOMP/Reports/CAR2023573/CAR2023573_BCD_d.csv",
#'                  show_col_types = FALSE)
#' ocads_data <- convert_OCADS(data, biochem.password, biochem.user)

convert_OCADS <- function(data, biochem.password, biochem.username) {
  require(tidyverse)
  require(RSQLite)
  require(DBI)
  require(ROracle)

  # connect to databases ----
  # biochem
  con_biochem <- open_biochem(user = biochem.user, pass= biochem.password)

  # lookups
  con_lookup <- dbConnect(RSQLite::SQLite(), 'lookup.sqlite')


  # input validation ----
  if (is.data.frame(data) == FALSE) {
    stop("data must be a data frame")
  }
  # check that data is in BCD format
  bcdkeycols <- c("MISSION_DESCRIPTOR",
                  "DATA_TYPE_METHOD",
                  "DIS_DETAIL_DATA_VALUE",
                  "DIS_DETAIL_DATA_QC_CODE",
                  "DIS_DETAIL_COLLECTOR_SAMP_ID")
  if (!all(bcdkeycols %in% colnames(data))) {
    stop("data must be in BCD format")
  }
  # check that only one mission is in file
  if (length(unique(data$MISSION_DESCRIPTOR)) > 1) {
    stop("data must contain only one mission")
  }

  # Gather platform and expocode ----
  # WARNINGS:
  # - expo code automatically generated, assumes  mission descriptor country and platform codes are correct

  # GRAB PLATFORM NAME from lookup
  # Extract the first four characters of the mission descriptor
  ship_code <- substr(unique(data$MISSION_DESCRIPTOR), 1, 4)
  # Query lookup database for platform name
  query <- paste0("SELECT name FROM platforms WHERE ICES_SHIPC_ship_codes = '", ship_code, "'")
  platform_name <-  dbGetQuery(con_lookup, query)
  if (nrow(platform_name) == 0) {
    stop("Platform name not found in lookup table")
  }
  # GENERATE EXPOCODE
  expocode <- paste0(substr(unique(data$MISSION_DESCRIPTOR), 1, 4),
                     min(format(as.Date(
                       data$DIS_HEADER_SDATE, format = '%m/%d/%Y'),
                       '%Y%m%d')))
  if (length(grep('NA', x = expocode)) != 0) {
    stop("EXPOCODE not properly generated, ensure DATE format is %m/%d/%Y")
  }

  query <- paste0("SELECT DISTINCT biochem.bcdiscretehedrs.sounding, biochem.bcevents.collector_event_id FROM biochem.bcdiscretehedrs INNER JOIN biochem.bcevents ON biochem.bcevents.event_seq = biochem.bcdiscretehedrs.event_seq INNER JOIN biochem.bcmissions ON biochem.bcmissions.mission_seq = biochem.bcevents.mission_seq INNER JOIN biochem.bcdiscretedtails ON biochem.bcdiscretehedrs.discrete_seq = biochem.bcdiscretedtails.discrete_seq INNER JOIN biochem.bcdatatypes ON biochem.bcdatatypes.data_type_seq = biochem.bcdiscretedtails.data_type_seq WHERE biochem.bcmissions.descriptor = '", unique(data$MISSION_DESCRIPTOR), "' ")
  soundings <- dbGetQuery(con_biochem, query)
  if (nrow(soundings) == 0) {
    stop("No sounding data found in BioChem")
  }
  soundings <- soundings %>%
    mutate(
      COLLECTOR_EVENT_ID = as.numeric(as.character(COLLECTOR_EVENT_ID))
    )
  # reformat BCD to OCADS (wide) ----
  # Initial data processing
  dataw <- data %>%
    pivot_wider(names_from = DATA_TYPE_METHOD,
                values_from = c(DIS_DETAIL_DATA_VALUE, DIS_DETAIL_DATA_QC_CODE)) %>%
    mutate(
      EVENT_COLLECTOR_EVENT_ID = as.numeric(as.character(EVENT_COLLECTOR_EVENT_ID))
    ) %>%
    # RENAME EXISTING COLUMNS
    rename( NAME = "MISSION_DESCRIPTOR",
            STNNBR = "EVENT_COLLECTOR_STN_NAME",
            CASTNO = "EVENT_COLLECTOR_EVENT_ID",
            SAMPNO = "DIS_DETAIL_COLLECTOR_SAMP_ID",
            DATE = "DIS_HEADER_SDATE",
            TIME = "DIS_HEADER_STIME",
            LATITUDE = "DIS_HEADER_SLAT",
            LONGITUDE = "DIS_HEADER_SLON",
            DEPTH = "DIS_HEADER_START_DEPTH",
            BTL_LAT = "DIS_HEADER_SLAT",
            BTL_LON = "DIS_HEADER_SLON"
    ) %>%
    mutate(BTL_DATE = DATE,
           BTL_TIME = TIME) %>%
    # ADD COLUMNS FROM LOOKUP & BIOCHEM
    mutate(PLATFORM = platform_name$name) %>%
    mutate(EXPOCODE = expocode) %>%
    # join soundings by event id
    left_join(soundings, by = c("CASTNO" = "COLLECTOR_EVENT_ID")) %>%
    # Remove extra biochem columns
    select(-DIS_DATA_NUM,
           -DIS_HEADER_END_DEPTH,
           -DIS_DETAIL_DATA_TYPE_SEQ,
           -DIS_DETAIL_DETECTION_LIMIT,
           -DIS_DETAIL_DETAIL_COLLECTOR,
           -CREATED_BY,
           -CREATED_DATE,
           -DATA_CENTER_CODE,
           -PROCESS_FLAG,
           -BATCH_SEQ,
           -DIS_SAMPLE_KEY_VALUE)


  # translate methods ----
  datacols <- grep(names(dataw), pattern = "DIS_DETAIL_DATA_VALUE")
  og_methods <- gsub(names(dataw)[datacols], pattern = "DIS_DETAIL_DATA_VALUE_", replacement = "")

  for (i in datacols) {
    # grab data column and qc column
    bcmethod <- gsub("DIS_DETAIL_DATA_VALUE_", "", names(dataw)[i])
    qccol <- grep(paste0("DIS_DETAIL_DATA_QC_CODE_", bcmethod), names(dataw))

    # translate method name
    query <- paste0("SELECT CCHDO FROM methods WHERE BIOCHEM = '", bcmethod, "'")
    cchdo_method <- dbGetQuery(con_lookup, query)
    if (nrow(cchdo_method) == 0) {
      warning("Method name ", bcmethod, " not found in lookup table. Data discarded!")
    } else{
      dataw <- dataw %>%
        rename_at(vars(i), ~ cchdo_method[[1]]) %>%
        rename_at(vars(qccol), ~ paste0(cchdo_method[[1]], "_FLAG_W"))
    }
  }

  # remove any untranslated methods
  dataw <- dataw %>%
    select(-contains("DIS_DETAIL_DATA_VALUE_")) %>%
    select(-contains("DIS_DETAIL_DATA_QC_CODE_"))


  # translate QC flags ----
  dataw <- dataw %>%
    mutate_at(vars(contains("_FLAG_W")), ~ flag_mapping(.))

  # add 2 flags to CTD oxygen and salinity to denote calibration
  #   warning assumption: all ctd salinity and oxygen are calibrated
  dataw <- dataw %>%
    mutate(CTDSAL_FLAG_W = str_replace(CTDSAL_FLAG_W, '0', '2'))

  dataw <- dataw %>%
    mutate(CTDOXY_FLAG_W = str_replace(CTDOXY_FLAG_W, '0', '2'))

  # Apply 6 flags where there are multiple replicates that will be averaged in the next step
  methods <- grep('FLAG_W$', names(dataw), value = TRUE) %>%
    str_replace('_FLAG_W$', '')

  for (si in unique(dataw$SAMPNO)) {
    for (m in methods) {
      escaped_m <- escape_special_chars(m)
      datacol <- dataw %>%
        select(matches(str_glue("^{escaped_m}$"))) %>%
        pull()

      qccol <- str_glue("{m}_FLAG_W")

      if (length(na.omit(datacol[dataw$SAMPNO == si])) > 1) {
        qcvals <- unique(dataw[[qccol]][dataw$SAMPNO == si])
        # only apply 6 flags if data is good, do not overwrite other flags
        if (length(na.omit(unique(qcvals))) == 1 && na.omit(unique(qcvals)) %in% c('0', '1', '2')) {
          dataw <- dataw %>%
            mutate(!!qccol := if_else(SAMPNO == si & !is.na(!!sym(m)), '6', !!sym(qccol)))
        } else {
          # if there are multiple flags, choose the highest of the replicate flags
          # note this is not an ideal solution but it matches with current biochem protocol
          dataw <- dataw %>%
            mutate(!!qccol := if_else(SAMPNO == si & !is.na(!!sym(m)),
                                      as.character(max(as.numeric(qcvals), na.rm = TRUE)),
                                      !!sym(qccol)))
        }
      }
    }
  }

  # make qc cols numeric so they will combine properly with averaging
  qccols <- grep('FLAG', names(dataw), value = TRUE)
  dataw[qccols] <- lapply(dataw[qccols], as.numeric)


  # Average replicates ----
  # average values within sample IDs only for numeric columns
  dataavg <- dataw %>%
    group_by(SAMPNO) %>%
    summarise(across(everything(),
                     ~ if(is.numeric(.)) {mean(., na.rm = TRUE)
                     }else {unique(.)}))

  # fill NaNs with -999 and add 9 flags ----
  for (n in 1:ncol(dataavg)) {
    if (length(grep(names(dataavg)[n], pattern = 'FLAG')) > 0) {
      dataavg[[n]] <- gsub(dataavg[[n]], pattern = 'NA', replacement = '9')
      dataavg[[n]] <- gsub(dataavg[[n]], pattern = 'NaN', replacement = '9')

    } else {
      dataavg[[n]] <- gsub(dataavg[[n]], pattern = 'NA', replacement = '-999')
      dataavg[[n]] <- gsub(dataavg[[n]], pattern = 'NaN', replacement = '-999')
    }
  }

  # fix date and time formatting ----
  dataavg <- dataavg %>%
    mutate(DATE = format(as.Date(DATE, format = '%m/%d/%Y'), '%Y-%m-%d'),
           TIME = str_pad(TIME, 4, pad = "0"),
           BTL_DATE = format(as.Date(BTL_DATE, format = '%m/%d/%Y'), '%Y-%m-%d'),
           BTL_TIME = str_pad(TIME, 4, pad = "0"))
  if (length(na.omit(dataavg$DATE)) < nrow(dataavg)) {
    stop('NAs introduced to DATE column. Original data should be formatted %m/%d/%Y')
  }


  # Units ----

  # convert units
  data_conv <- perform_unit_conversions(data = dataavg)

  # Initialize unit_row as a character vector
  unit_row <- character(ncol(data_conv))

  # Gather unit row
  flag_columns <- grep("_FLAG_W$", colnames(data_conv), value = TRUE)
  data_columns <- sub("_FLAG_W$", "", flag_columns)


  for (i in seq_along(unit_row)) {
    if (names(data_conv)[i] %in% data_columns) {
      query <- paste0("select Unit from methods where CCHDO = '", names(data_conv)[i], "'")
      unit <- dbGetQuery(con_lookup, query)
      if (length(unit$Unit) > 1){
        # check original data name
        query <- paste0("select Unit from methods where CCHDO = '", names(data_conv)[i], "' and BIOCHEM in ('", str_c(og_methods, collapse = "', '"), "')")
        #WANRING will only work if there is only one data type per parameter in original data for example if there are multiple temps (ie. Temp_CTD_1990 and Temp_CTD_1968, this will fail)
        unit <- dbGetQuery(con_lookup, query)
      }
      if (nrow(unit) == 1){
        unit_row[i] <- unit$Unit
      } else {
        if (nrow(unit) == 0){
          stop(paste("Could not idenitfy unit for", names(data_conv)[i],'with query:', query))
        }
        if (nrow(unit) > 1){
          stop(paste("Multiple units found for", names(data_conv)[i],'with query:', query))
        }
      }
    } else {
      unit_row[i] <- ''
    }
    if (names(data_conv)[i] == 'SOUNDING') {
      unit_row[i] <- 'METERS'
    }
  }

  # Add units to data
  unit_row_df <- as.data.frame(t(unit_row), stringsAsFactors = FALSE)
  names(unit_row_df) <- names(data_conv)
  data_conv <- rbind(unit_row_df, data_conv)



  # order columns ----
  # Standard metadata columns
  metadata_columns <- c('EXPOCODE', 'NAME', 'PLATFORM', 'STNNBR', 'CASTNO', 'SAMPNO', 'DATE', 'TIME', 'SOUNDING')


  # Identify metadata columns (those without matching flag columns)
  other_columns <- setdiff(colnames(data_conv), c(metadata_columns, flag_columns, data_columns))

  # Create the new column order
  new_col_order <- c(metadata_columns, other_columns, unlist(lapply(data_columns, function(col) c(col, paste0(col, "_FLAG_W")))))

  # Reorder columns in data
  data_conv <- data_conv %>%
    select(all_of(new_col_order))

  # DATA ROUDNING? ----
  # REID HAD INCLUDED A LOT OF DATA ROUNDING UNDER DIFFERENT CONDITIONS, BUT IS THIS NECESSARY?


  # testing ----
  # Data integrity checks
  # Check that data$COLLECTOR_SAMPLE_ID and data_conv$SAMPNO have all the same unique values
  if (!all(sort(as.numeric(unique(data$DIS_DETAIL_COLLECTOR_SAMP_ID))) == sort(as.numeric(unique(data_conv$SAMPNO))))) {
    stop("Mismatch in unique values between data$COLLECTOR_SAMPLE_ID and data_conv$SAMPNO")
  }

  # Check that the columns CTDPRS, NO2+NO3, CHLORA are all present (warning if missing)
  required_columns <- c("CTDPRS", "NO2+NO3", "CHLORA")
  missing_columns <- setdiff(required_columns, names(data_conv))
  if (length(missing_columns) > 0) {
    warning("Missing columns: ", paste(missing_columns, collapse = ", "))
  }

  # Check no missing flags in nutrient or carbonate data
  nutrient_carbonate_cols <- c("NO2+NO3", "PHSPHT", "SILCAT", "SALNTY", "TCARBN", "PH_TOT", "ALKALI", "PCO2")

  # Check that NO2+NO3, PHSPHT, SILCAT, SALNTY, TCARBN, PH_TOT, ALKALI, PCO2 have all flags above 1 if the variable is present
  for (col in nutrient_carbonate_cols) {
    if (col %in% names(data_conv)) {
      if (any(na.omit(as.numeric(data_conv[[paste0(col, "_FLAG_W")]])) <= 1)) {
        stop(paste("Flags for", col, "are not all above 1"))
      }
    }
  }

  # Check that -999 data values have all 9 flags
  for (dc in data_columns) {
    qc <- str_glue("{dc}_FLAG_W")
    if (qc %in% names(data_conv)){
      mflags <- unique(data_conv[[qc]][data_conv[[dc]] == '-999'])
      if (length(mflags) > 1) {
        stop("Missing data flags are not properly assigned!")
      }
      if (length(mflags) == 1 && !'9' %in% mflags) {
        stop('Missing data flags are not properly assigned!')
      }
    }
  }


  # Check no NA values
  if (any(is.na(data_conv))) {
    stop("NA values found in data_conv")
  }

  # Check no missing metadata columns
  metadata_columns <- c('EXPOCODE', 'NAME', 'PLATFORM', 'STNNBR', 'CASTNO', 'SAMPNO', 'DATE', 'TIME', 'SOUNDING')
  if (!all(metadata_columns %in% names(data_conv))) {
    stop("Missing metadata columns")
  }

  # Random subset of 25 data points, check that data values, and flags match from data to data_conv
  set.seed(123) # For reproducibility
  sample_indices <- sample(nrow(data), 25)
  for (i in sample_indices) {
    si <- data$DIS_DETAIL_COLLECTOR_SAMP_ID[i]
    ogdat <- data %>%
      filter(DIS_DETAIL_COLLECTOR_SAMP_ID == si)
    cdat <- data_conv %>%
      filter(SAMPNO == si)

    # check an unconverted variable
    if (ogdat %>% filter(DATA_TYPE_METHOD == 'Pressure') %>% select(DIS_DETAIL_DATA_VALUE) !=
        cdat$CTDPRS) {
      stop('Data integrity error detected: Data values have been misassigned!')
    }

    # check a flag
    nitrate <- grep(unique(data$DATA_TYPE_METHOD), pattern = 'NO2NO3', value = TRUE)
    ogflags <- unique(ogdat %>% filter(DATA_TYPE_METHOD == nitrate) %>% select(DIS_DETAIL_DATA_QC_CODE) )
    cflags <- unique(cdat$`NO2+NO3_FLAG_W`)
    if (length(ogflags) >1) {
      ogflags <- max(as.numeric(ogflags))
    }
    if (flag_mapping(ogflags) != cflags && cflags != '6') {
      if (cflags != '9'){
        stop('Data integrity error detected: Data flags have been misassigned!')
      }
    }


  }



  # close all database connections

  return(data_conv)
}


# HELPER FUNCTIONS ----
# Function to escape special characters
escape_special_chars <- function(string) {
  str_replace_all(string, "([\\W])", "\\\\\\1")
}

# function to translate QC flags from BioChem to WOCE
flag_mapping <- function(qc_col) {
  # map flags from BioChem to WOCE
  qc_col %>%
    str_replace_all(c(
      "1" = "2",
      "5" = "2",
      "6" = "2"
    ))
}


# helper function to connect to biochem
open_biochem <- function(user=NA, pass=NA) {
  require(rstudioapi)
  require(ROracle)
  if (is.na(user)) {
    user <- showPrompt(title="Username", message="Input username:", default="ogradye")
  }
  if (is.na(pass)) {
    pass <- askForPassword(prompt="Input password:")
  }
  conn <- try(expr = {
    dbConnect(dbDriver("Oracle"), user, pass, "PTRAN")}, silent = TRUE)
  if (class(conn) == 'try-error'){
    stop('Cannot connect to BioChem!')
  }

  return(conn)
}

#' Perform Unit Conversions
#'
#' This function performs unit conversions on the provided dataset, including conversions for oxygen, nutrients, and chlorophyll.
#'
#' @param data A data frame containing the dataset to be converted.
#' @return A data frame with converted units.
#' @examples
#' \dontrun{
#' subm_data_qc_oa <- perform_unit_conversions(subm_data_qc_oa)
#' }
perform_unit_conversions <- function(data) {
  require(oce)
  #TODO makes some assumptions so data can be converted without exact tmp/sal/prs data (document assumptions)
  # Check for required columns
  required_cols <- c('CTDOXY', 'OXYGEN', 'NITRIT', 'NH3', 'NO2+NO3', 'PHSPHT', 'SILCAT', 'POC', 'PON', 'CHLORA')
  available_cols <- required_cols[required_cols %in% colnames(data)]

  # Calculate pressure if missing ----
  if (!'CTDPRS' %in% colnames(data)) {
    data <- data %>%
      mutate(CTDPRS = swPressure(DEPTH, BTL_LAT, eos = 'unesco'))
  }


  # Calculate potential density for O2 ----
  data <- data %>%
    mutate(
      potden_O2 = (swSigmaTheta(
        ifelse(CTDSAL == -999, NA, as.numeric(CTDSAL)),
        ifelse(CTDTMP == -999, NA, as.numeric(CTDTMP)),
        ifelse(CTDPRS == -999, NA, as.numeric(CTDPRS)),
        latitude = as.numeric(BTL_LAT),
        longitude = as.numeric(BTL_LON),
        eos = "unesco"
      ) + 1000) / 1000
    )

  if (anyNA(data$potden_O2)) {
    warning("Missing Data! Oxygen potential density contains NA values, this will erase oxygen data when attempting to convert!")
  }

  # Calculate potential density for nutrients ----
  data <- data %>%
    mutate(
      potden_nut = (swSigmaTheta(
        ifelse(CTDSAL == -999, NA, as.numeric(CTDSAL)),
        ifelse(CTDTMP == -999, 15, as.numeric(CTDTMP)), # assumes default of 15 deg if no temperature data is available
        ifelse(CTDPRS == -999, NA, as.numeric(CTDPRS)),
        latitude = as.numeric(BTL_LAT),
        longitude = as.numeric(BTL_LON),
        eos = "unesco"
      ) + 1000) / 1000
    )

  if (anyNA(data$potden_nut)) {
    warning("Missing Data! Nutrient potential density contains NA values, this will erase nutrient data when attempting to convert!")
  }

  # Convert oxygen from mL/L to µmol/kg ----
  if ('CTDOXY' %in% available_cols) {
    data <- data %>%
      mutate(
        CTDOXY = ifelse(
          CTDOXY != -999,
          (as.numeric(CTDOXY) * 44.66) / potden_O2,
          CTDOXY
        )
      )

    if (sum(is.na(data$CTDOXY)) > 10) {
      warning(paste0("More than 10 NA values generated in CTDOXY because of unit conversion! \n", sum(is.na(data$CTDOXY)), "/", nrow(data)))
    }
  }

  if ('OXYGEN' %in% available_cols) {
    data <- data %>%
      mutate(
        OXYGEN = ifelse(
          OXYGEN != -999,
          (as.numeric(OXYGEN) * 44.66) / potden_O2,
          OXYGEN
        )
      )

    if (sum(is.na(data$OXYGEN)) > 10) {
      warning(paste0("More than 10 NA values generated in BTL OXY because of unit conversion! \n", sum(is.na(data$OXYGEN)), "/", nrow(data)))
    }
  }

  # Convert nutrients from mmol/m³ (µmol/L) to µmol/kg ----
  nutrient_cols <- c('NITRIT', 'NO2+NO3', 'NH3', 'PHSPHT', 'SILCAT', 'POC', 'PON')
  for (col in nutrient_cols) {
    if (col %in% available_cols) {
      data <- data %>%
        mutate(
          !!sym(col) := ifelse(
            !!sym(col) != -999,
            as.numeric(!!sym(col)) / potden_nut,
            !!sym(col)
          )
        )
    }
  }

  # Convert chlorophyll from µg/L to µg/kg ----
  if ('CHLORA' %in% available_cols) {
    data <- data %>%
      mutate(
        CHLORA = ifelse(
          CHLORA != -999,
          as.numeric(CHLORA) / potden_nut,
          CHLORA
        )
      )
  }

  # to do remove temp potden columns
  data <- data %>%
    select(!c(potden_O2, potden_nut))

  return(data)
}
