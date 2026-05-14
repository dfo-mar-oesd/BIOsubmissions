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
#' @param prompt_notes logical, whether to prompt for submission notes after conversion (default: TRUE)
#'
#' All data values and quality control flags are extracted directly from BioChem, ensuring
#' complete traceability and consistency. The function performs automated validation checks for:
#' - Expected carbonate chemistry parameters (ALKALI, PH_TOT, TCARBN, PCO2)
#' - Expected tracer parameters (CFC-11, CFC-12, CFC-113, SF6)
#' - Missing or suspicious QC flags (e.g., all zeros indicating no QC applied)
#'
#' BioChem connection is required to pull sounding data (bottom depth) for each station.
#'
#' @return a dataframe formatted for upload to OCADS
#' @export
#'
#' @example
#' source("C:/users/ogradye/desktop/biochem_creds.R")
#' data <- read_csv("C:/Users/ogradye/Documents/AZMP_template/AZOMP/Reports/CAR2023573/CAR2023573_BCD_d.csv",
#'                  show_col_types = FALSE)
#' ocads_data <- convert_OCADS(data, biochem.password, biochem.user)

convert_OCADS <- function(data, biochem.password, biochem.username, prompt_notes = TRUE) {
  require(tidyverse)
  require(RSQLite)
  require(DBI)
  require(ROracle)

  # connect to databases ----
  con_biochem <- open_biochem(user = biochem.username, pass = biochem.password)
  con_lookup  <- dbConnect(RSQLite::SQLite(), 'lookup.sqlite')

  # input validation ----
  if (!is.data.frame(data)) {
    stop("data must be a data frame")
  }
  bcdkeycols <- c("MISSION_DESCRIPTOR",
                  "DATA_TYPE_METHOD",
                  "DIS_DETAIL_DATA_VALUE",
                  "DIS_DETAIL_DATA_QC_CODE",
                  "DIS_DETAIL_COLLECTOR_SAMP_ID")
  if (!all(bcdkeycols %in% colnames(data))) {
    stop("data must be in BCD format")
  }
  if (length(unique(data$MISSION_DESCRIPTOR)) > 1) {
    stop("data must contain only one mission")
  }

  # Gather platform and expocode ----
  ship_code    <- substr(unique(data$MISSION_DESCRIPTOR), 1, 4)
  query        <- paste0("SELECT name FROM platforms WHERE ICES_SHIPC_ship_codes = '", ship_code, "'")
  platform_name <- dbGetQuery(con_lookup, query)
  if (nrow(platform_name) == 0) {
    stop("Platform name not found in lookup table")
  }

  expocode <- paste0(substr(unique(data$MISSION_DESCRIPTOR), 1, 4),
                     min(format(as.Date(data$DIS_HEADER_SDATE, format = '%m/%d/%Y'), '%Y%m%d')))
  if (length(grep('NA', x = expocode)) != 0) {
    stop("EXPOCODE not properly generated, ensure DATE format is %m/%d/%Y")
  }

  query <- paste0(
    "SELECT DISTINCT biochem.bcdiscretehedrs.sounding, biochem.bcevents.collector_event_id ",
    "FROM biochem.bcdiscretehedrs ",
    "INNER JOIN biochem.bcevents ON biochem.bcevents.event_seq = biochem.bcdiscretehedrs.event_seq ",
    "INNER JOIN biochem.bcmissions ON biochem.bcmissions.mission_seq = biochem.bcevents.mission_seq ",
    "INNER JOIN biochem.bcdiscretedtails ON biochem.bcdiscretehedrs.discrete_seq = biochem.bcdiscretedtails.discrete_seq ",
    "INNER JOIN biochem.bcdatatypes ON biochem.bcdatatypes.data_type_seq = biochem.bcdiscretedtails.data_type_seq ",
    "WHERE biochem.bcmissions.descriptor = '", unique(data$MISSION_DESCRIPTOR), "' "
  )
  soundings <- dbGetQuery(con_biochem, query)
  if (nrow(soundings) == 0) {
    stop("No sounding data found in BioChem")
  }
  soundings <- soundings %>%
    mutate(COLLECTOR_EVENT_ID = as.numeric(as.character(COLLECTOR_EVENT_ID)))

  # reformat BCD to OCADS (wide) ----
  dataw <- data %>%
    pivot_wider(
      names_from  = DATA_TYPE_METHOD,
      values_from = c(DIS_DETAIL_DATA_VALUE, DIS_DETAIL_DATA_QC_CODE)
    ) %>%
    mutate(EVENT_COLLECTOR_EVENT_ID = as.numeric(as.character(EVENT_COLLECTOR_EVENT_ID))) %>%
    rename(
      NAME      = "MISSION_DESCRIPTOR",
      STNNBR    = "EVENT_COLLECTOR_STN_NAME",
      CASTNO    = "EVENT_COLLECTOR_EVENT_ID",
      SAMPNO    = "DIS_DETAIL_COLLECTOR_SAMP_ID",
      DATE      = "DIS_HEADER_SDATE",
      TIME      = "DIS_HEADER_STIME",
      LATITUDE  = "DIS_HEADER_SLAT",
      LONGITUDE = "DIS_HEADER_SLON",
      DEPTH     = "DIS_HEADER_START_DEPTH",
      BTL_LAT   = "DIS_HEADER_SLAT",
      BTL_LON   = "DIS_HEADER_SLON"
    ) %>%
    mutate(BTL_DATE = DATE, BTL_TIME = TIME) %>%
    mutate(PLATFORM = platform_name$name) %>%
    mutate(EXPOCODE = expocode) %>%
    left_join(soundings, by = c("CASTNO" = "COLLECTOR_EVENT_ID")) %>%
    select(-DIS_DATA_NUM,
           -DIS_HEADER_END_DEPTH,
           -DIS_DETAIL_DATA_TYPE_SEQ,
           -DIS_DETAIL_DETECTION_LIMIT,
           -DIS_DETAIL_COLLECTOR,
           -CREATED_BY,
           -CREATED_DATE,
           -DATA_CENTER_CODE,
           -PROCESS_FLAG,
           -BATCH_SEQ,
           -DIS_SAMPLE_KEY_VALUE)

  # translate methods ----
  # Store original method names (BioChem names extracted from column names)
  # Use exact matching: column names are "DIS_DETAIL_DATA_VALUE_<method>"
  # so we extract the method by removing the prefix, then match exactly.

  all_colnames <- names(dataw)

  # Identify data value columns using exact prefix matching
  data_prefix <- "DIS_DETAIL_DATA_VALUE_"
  qc_prefix   <- "DIS_DETAIL_DATA_QC_CODE_"

  data_colnames_all <- all_colnames[startsWith(all_colnames, data_prefix)]
  qc_colnames_all   <- all_colnames[startsWith(all_colnames, qc_prefix)]

  # Extract BioChem method names from column names by removing prefix
  # Use exact suffix stripping to avoid partial matching issues
  bc_methods_from_data <- sub(paste0("^", data_prefix), "", data_colnames_all)
  bc_methods_from_qc   <- sub(paste0("^", qc_prefix),   "", qc_colnames_all)

  # Store original methods for unit lookup later
  og_methods <- bc_methods_from_data

  message("\n--- Method Translation Lookup ---")

  # Build mapping: CCHDO param -> list of exact BioChem data col names and qc col names
  cchdo_to_data_exact <- list()  # CCHDO param -> character vector of data column names
  cchdo_to_qc_exact   <- list()  # CCHDO param -> character vector of qc column names
  discarded_methods   <- c()

  for (bc in bc_methods_from_data) {
    # Construct exact column names using string paste, NOT grep
    data_col_name <- paste0(data_prefix, bc)
    qc_col_name   <- paste0(qc_prefix,   bc)

    # Verify columns actually exist (they should, but sanity check)
    if (!data_col_name %in% all_colnames) {
      warning("Expected data column '", data_col_name, "' not found. Skipping.")
      next
    }
    if (!qc_col_name %in% all_colnames) {
      warning("Expected QC column '", qc_col_name, "' not found. Skipping.")
      next
    }

    # Look up CCHDO name
    query        <- paste0("SELECT CCHDO FROM methods WHERE BIOCHEM = '", bc, "'")
    cchdo_result <- dbGetQuery(con_lookup, query)

    if (nrow(cchdo_result) == 0) {
      message("  [DISCARDED] BioChem method '", bc, "' -> not found in lookup table")
      discarded_methods <- c(discarded_methods, data_col_name, qc_col_name)
    } else {
      param <- cchdo_result[[1]]
      message("  [MAPPED]    BioChem method '", bc, "' -> CCHDO parameter '", param, "'")
      cchdo_to_data_exact[[param]] <- c(cchdo_to_data_exact[[param]], data_col_name)
      cchdo_to_qc_exact[[param]]   <- c(cchdo_to_qc_exact[[param]],   qc_col_name)
    }
  }

  message("\n--- Applying Translations ---")

  for (param in names(cchdo_to_data_exact)) {
    src_data_cols <- cchdo_to_data_exact[[param]]
    src_qc_cols   <- cchdo_to_qc_exact[[param]]

    if (length(src_data_cols) == 1) {
      # -------------------------------------------------------
      # Simple 1-to-1 case: rename columns directly
      # -------------------------------------------------------
      message("  [RENAME]  '", src_data_cols, "' -> '", param, "'")
      message("  [RENAME]  '", src_qc_cols,   "' -> '", paste0(param, "_FLAG_W"), "'")

      dataw <- dataw %>%
        rename(!!param                    := !!src_data_cols) %>%
        rename(!!paste0(param, "_FLAG_W") := !!src_qc_cols)

    } else {
      # -------------------------------------------------------
      # Many-to-1 case: coalesce all source columns into one
      # -------------------------------------------------------
      bc_names <- sub(paste0("^", data_prefix), "", src_data_cols)
      message("  [COALESCE] BioChem methods (", paste(bc_names, collapse = ", "),
              ") -> '", param, "'")

      dataw <- dataw %>%
        mutate(
          !!param                    := coalesce(!!!syms(src_data_cols)),
          !!paste0(param, "_FLAG_W") := coalesce(!!!syms(src_qc_cols))
        ) %>%
        select(-all_of(src_data_cols)) %>%
        select(-all_of(src_qc_cols))
    }
  }

  message("\n--- Final Column State After Translation ---")
  message("  Columns present: ", paste(names(dataw), collapse = ", "))

  # remove any untranslated / discarded method columns
  dataw <- dataw %>%
    select(-any_of(discarded_methods)) %>%
    select(-contains(data_prefix)) %>%
    select(-contains(qc_prefix))

  # Data Validation and Quality Checks ----
  message("\n--- BioChem Data Quality Validation ---")
  
  # Define expected parameters for carbonate chemistry and tracers
  expected_carbonate <- c("ALKALI", "PH_TOT", "TCARBN", "PCO2")
  expected_tracers   <- c("CFC-11", "CFC-12", "CFC113", "SF6")
  
  # Check for carbonate chemistry parameters
  missing_carbonate <- setdiff(expected_carbonate, names(dataw))
  present_carbonate <- intersect(expected_carbonate, names(dataw))
  
  if (length(present_carbonate) > 0) {
    message("  [✓] Found carbonate chemistry parameters: ", paste(present_carbonate, collapse = ", "))
    
    # Check for missing QC flags on present carbonate parameters
    for (param in present_carbonate) {
      flag_col <- paste0(param, "_FLAG_W")
      if (flag_col %in% names(dataw)) {
        flag_values <- na.omit(unique(dataw[[flag_col]]))
        # Check if all flags are 0 (indicating no QC has been applied)
        if (length(flag_values) == 1 && flag_values == "0") {
          warning("  [!] All QC flags for ", param, " are 0 - no quality control has been applied!")
        }
        # Check if flags are missing entirely
        if (length(flag_values) == 0 || all(is.na(dataw[[flag_col]]))) {
          warning("  [!] QC flags for ", param, " are missing or all NA!")
        }
      } else {
        warning("  [!] Missing QC flag column for ", param)
      }
    }
  }
  
  if (length(missing_carbonate) > 0) {
    message("  [NOTE] Missing carbonate chemistry parameters: ", paste(missing_carbonate, collapse = ", "))
    message("         This may be expected if this mission did not collect carbonate chemistry data.")
  }
  
  # Check for tracer parameters
  present_tracers <- intersect(expected_tracers, names(dataw))
  missing_tracers <- setdiff(expected_tracers, names(dataw))
  
  if (length(present_tracers) > 0) {
    message("  [✓] Found tracer parameters: ", paste(present_tracers, collapse = ", "))
    
    # Check for missing QC flags on present tracer parameters
    for (param in present_tracers) {
      flag_col <- paste0(param, "_FLAG_W")
      if (flag_col %in% names(dataw)) {
        flag_values <- na.omit(unique(dataw[[flag_col]]))
        if (length(flag_values) == 1 && flag_values == "0") {
          warning("  [!] All QC flags for ", param, " are 0 - no quality control has been applied!")
        }
        if (length(flag_values) == 0 || all(is.na(dataw[[flag_col]]))) {
          warning("  [!] QC flags for ", param, " are missing or all NA!")
        }
      } else {
        warning("  [!] Missing QC flag column for ", param)
      }
    }
  }
  
  if (length(missing_tracers) > 0) {
    message("  [NOTE] Missing tracer parameters: ", paste(missing_tracers, collapse = ", "))
    message("         This may be expected if this mission did not collect tracer data.")
  }
  
  # General QC flag validation for all translated parameters
  all_flag_cols <- grep("_FLAG_W$", names(dataw), value = TRUE)
  for (flag_col in all_flag_cols) {
    param_name <- sub("_FLAG_W$", "", flag_col)
    flag_values <- na.omit(unique(dataw[[flag_col]]))
    
    # Check if all flags are 0
    if (length(flag_values) == 1 && flag_values == "0") {
      if (!param_name %in% c(present_carbonate, present_tracers)) {
        # Only show warning if we haven't already warned about this parameter
        warning("  [!] All QC flags for ", param_name, " are 0 - no quality control has been applied!")
      }
    }
    
    # Check for data presence without flags
    if (param_name %in% names(dataw)) {
      data_values <- na.omit(dataw[[param_name]])
      if (length(data_values) > 0 && (length(flag_values) == 0 || all(is.na(dataw[[flag_col]])))) {
        warning("  [!] ", param_name, " has data values but missing QC flags!")
      }
    }
  }
  
  message("  [✓] Data quality validation complete\n")

  # translate QC flags ----
  dataw <- dataw %>%
    mutate(across(contains("_FLAG_W"), ~ flag_mapping(.)))

  # add 2 flags to CTD oxygen and salinity
  dataw <- dataw %>%
    mutate(CTDSAL_FLAG_W = str_replace(CTDSAL_FLAG_W, '0', '2'),
           CTDOXY_FLAG_W = str_replace(CTDOXY_FLAG_W, '0', '2'))

  # Apply 6 flags for replicates ----
  methods_list <- grep('FLAG_W$', names(dataw), value = TRUE) %>%
    str_replace('_FLAG_W$', '')

  for (si in unique(dataw$SAMPNO)) {
    for (m in methods_list) {
      escaped_m <- escape_special_chars(m)
      datacol <- dataw %>%
        select(matches(str_glue("^{escaped_m}$"))) %>%
        pull()

      qccol <- str_glue("{m}_FLAG_W")

      if (length(na.omit(datacol[dataw$SAMPNO == si])) > 1) {
        qcvals <- unique(dataw[[qccol]][dataw$SAMPNO == si])
        if (length(na.omit(unique(qcvals))) == 1 &&
            na.omit(unique(qcvals)) %in% c('0', '1', '2')) {
          dataw <- dataw %>%
            mutate(!!qccol := if_else(SAMPNO == si & !is.na(!!sym(m)), '6', !!sym(qccol)))
        } else {
          dataw <- dataw %>%
            mutate(!!qccol := if_else(
              SAMPNO == si & !is.na(!!sym(m)),
              as.character(max(as.numeric(qcvals), na.rm = TRUE)),
              !!sym(qccol)
            ))
        }
      }
    }
  }

  # make qc cols numeric for averaging
  qccols <- grep('FLAG', names(dataw), value = TRUE)
  dataw[qccols] <- lapply(dataw[qccols], as.numeric)

  # Average replicates ----
  dataavg <- dataw %>%
    group_by(SAMPNO) %>%
    summarise(across(everything(), ~ ifelse(is.numeric(.), mean(., na.rm = TRUE), unique(.))))

  # fill NaNs with -999 / 9 ----
  for (n in 1:ncol(dataavg)) {
    if (length(grep(names(dataavg)[n], pattern = 'FLAG')) > 0) {
      dataavg[[n]] <- gsub(dataavg[[n]], pattern = 'NA',  replacement = '9')
      dataavg[[n]] <- gsub(dataavg[[n]], pattern = 'NaN', replacement = '9')
    } else {
      dataavg[[n]] <- gsub(dataavg[[n]], pattern = 'NA',  replacement = '-999')
      dataavg[[n]] <- gsub(dataavg[[n]], pattern = 'NaN', replacement = '-999')
    }
  }

  # fix date and time formatting ----
  dataavg <- dataavg %>%
    mutate(
      DATE     = format(as.Date(DATE,     format = '%m/%d/%Y'), '%Y-%m-%d'),
      TIME     = str_pad(TIME, 4, pad = "0"),
      BTL_DATE = format(as.Date(BTL_DATE, format = '%m/%d/%Y'), '%Y-%m-%d'),
      BTL_TIME = str_pad(TIME, 4, pad = "0")
    )
  if (length(na.omit(dataavg$DATE)) < nrow(dataavg)) {
    stop('NAs introduced to DATE column. Original data should be formatted %m/%d/%Y')
  }

  # Units ----
  data_conv <- perform_unit_conversions(data = dataavg)

  unit_row     <- character(ncol(data_conv))
  flag_columns <- grep("_FLAG_W$", colnames(data_conv), value = TRUE)
  data_columns <- sub("_FLAG_W$", "", flag_columns)

  for (i in seq_along(unit_row)) {
    col_name <- names(data_conv)[i]

    if (col_name == 'SOUNDING') {
      unit_row[i] <- 'METERS'
      next
    }

    if (col_name %in% data_columns) {
      # First try: look up unit filtering by original BioChem methods
      # Use DISTINCT to handle the case where multiple BioChem methods
      # map to the same CCHDO parameter but have the same unit (eg PO4_Filt_F and PO4_Tech_F -> PHSPHT)
      query <- paste0(
        "SELECT DISTINCT Unit FROM methods WHERE CCHDO = '", col_name,
        "' AND BIOCHEM IN ('", paste(og_methods, collapse = "', '"), "')"
      )
      unit <- dbGetQuery(con_lookup, query)

      # Fall back to querying without BIOCHEM filter if no result
      if (nrow(unit) == 0) {
        query <- paste0("SELECT DISTINCT Unit FROM methods WHERE CCHDO = '", col_name, "'")
        unit  <- dbGetQuery(con_lookup, query)
      }

      if (nrow(unit) == 1) {
        unit_row[i] <- unit$Unit
      } else if (nrow(unit) == 0) {
        stop(paste("Could not identify unit for", col_name, "- no matching entry in lookup table"))
      } else {
        # Multiple distinct units found - this is a genuine conflict that needs attention
        stop(paste0(
          "Multiple DISTINCT units found for '", col_name, "': ",
          paste(unit$Unit, collapse = ", "),
          "\nBioChem methods involved: ",
          paste(og_methods[og_methods %in%
                             dbGetQuery(con_lookup,
                                        paste0("SELECT BIOCHEM FROM methods WHERE CCHDO = '", col_name, "'"))$BIOCHEM],
                collapse = ", "),
          "\nPlease resolve unit conflict in lookup table."
        ))
      }
    } else {
      unit_row[i] <- ''
    }
  }

  unit_row_df        <- as.data.frame(t(unit_row), stringsAsFactors = FALSE)
  names(unit_row_df) <- names(data_conv)
  data_conv          <- rbind(unit_row_df, data_conv)
  # order columns ----
  metadata_columns <- c('EXPOCODE', 'NAME', 'PLATFORM', 'STNNBR', 'CASTNO',
                        'SAMPNO', 'DATE', 'TIME', 'SOUNDING')
  other_columns    <- setdiff(colnames(data_conv),
                              c(metadata_columns, flag_columns, data_columns))
  new_col_order    <- c(
    metadata_columns,
    other_columns,
    unlist(lapply(data_columns, function(col) c(col, paste0(col, "_FLAG_W"))))
  )
  data_conv <- data_conv %>% select(all_of(new_col_order))

  # testing ----
  if (!all(sort(as.numeric(unique(data$DIS_DETAIL_COLLECTOR_SAMP_ID))) ==
           sort(as.numeric(unique(data_conv$SAMPNO))))) {
    stop("Mismatch in unique values between data$COLLECTOR_SAMPLE_ID and data_conv$SAMPNO")
  }

  required_columns <- c("CTDPRS", "NO2+NO3", "CHLORA")
  missing_columns  <- setdiff(required_columns, names(data_conv))
  if (length(missing_columns) > 0) {
    warning("Missing columns: ", paste(missing_columns, collapse = ", "))
  }

  nutrient_carbonate_cols <- c("NO2+NO3", "PHSPHT", "SILCAT", "SALNTY",
                               "TCARBN", "PH_TOT", "ALKALI", "PCO2")
  for (col in nutrient_carbonate_cols) {
    if (col %in% names(data_conv)) {
      if (any(na.omit(as.numeric(data_conv[[paste0(col, "_FLAG_W")]])) <= 1)) {
        stop(paste("Flags for", col, "are not all above 1"))
      }
    }
  }

  for (dc in data_columns) {
    qc <- str_glue("{dc}_FLAG_W")
    if (qc %in% names(data_conv)) {
      mflags <- unique(data_conv[[qc]][data_conv[[dc]] == '-999'])
      if (length(mflags) > 1) stop("Missing data flags are not properly assigned!")
      if (length(mflags) == 1 && !'9' %in% mflags) stop('Missing data flags are not properly assigned!')
    }
  }

  if (any(is.na(data_conv))) stop("NA values found in data_conv")

  if (!all(metadata_columns %in% names(data_conv))) stop("Missing metadata columns")

  # Random subset of 25 data points, check that data values, and flags match from data to data_conv
  set.seed(123)
  sample_indices <- sample(nrow(data), 25)
  for (i in sample_indices) {
    si    <- data$DIS_DETAIL_COLLECTOR_SAMP_ID[i]
    ogdat <- data %>% filter(DIS_DETAIL_COLLECTOR_SAMP_ID == si)
    cdat  <- data_conv %>% filter(SAMPNO == si)

    # check an unconverted variable
    # pull() extracts a single vector, first() ensures scalar comparison
    # in case of duplicate rows for same sample
    og_pressure <- ogdat %>%
      filter(DATA_TYPE_METHOD == 'Pressure') %>%
      pull(DIS_DETAIL_DATA_VALUE) %>%
      first()

    conv_pressure <- cdat %>%
      pull(CTDPRS) %>%
      first()

    if (!is.na(og_pressure) && og_pressure != conv_pressure) {
      stop('Data integrity error detected: Data values have been misassigned!')
    }

    # check a flag
    nitrate <- grep(unique(data$DATA_TYPE_METHOD), pattern = 'NO2NO3', value = TRUE)

    ogflags <- ogdat %>%
      filter(DATA_TYPE_METHOD %in% nitrate) %>%
      pull(DIS_DETAIL_DATA_QC_CODE) %>%
      unique()

    cflags <- cdat %>%
      pull(`NO2+NO3_FLAG_W`) %>%
      first()

    # if there were multiple replicates, take the worst flag
    if (length(ogflags) > 1) {
      ogflags <- as.character(max(as.numeric(ogflags), na.rm = TRUE))
    }

    # skip check if no nitrate data for this sample
    if (length(ogflags) == 0 || is.na(ogflags)) next

    mapped_flag <- flag_mapping(as.character(ogflags))

    if (mapped_flag != cflags && cflags != '6') {
      if (cflags != '9') {
        stop('Data integrity error detected: Data flags have been misassigned!')
      }
    }
  }

  dbDisconnect(con_biochem)
  dbDisconnect(con_lookup)

  # Prompt for submission notes if enabled
  if (prompt_notes) {
    # Extract mission and year info
    mission_descriptor <- unique(data$MISSION_DESCRIPTOR)
    year <- as.numeric(substr(expocode, 5, 8))
    
    message("\n✓ OCADS conversion complete!")
    message("Mission: ", mission_descriptor, " (EXPOCODE: ", expocode, ")")
    
    # Prompt for notes
    tryCatch({
      prompt_submission_notes("OCADS", mission_descriptor, year)
    }, error = function(e) {
      message("Note: Could not prompt for submission notes. ",
              "You can add them later using append_mission_notes()")
    })
  }

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
        rep(21, nrow(data)), #use Peter's lab temp 21 deg C
        #ifelse(CTDTMP == -999, 15, as.numeric(CTDTMP)), # updated Dec 2025
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
