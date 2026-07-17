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
#' - Expected tracer parameters (CFC-12, SF6, DELO18)
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

convert_OCADS <- function(data, biochem.password, biochem.username, prompt_notes = TRUE, lookup_path = NULL) {
  require(tidyverse)
  require(RSQLite)
  require(DBI)
  require(ROracle)

  # Resolve lookup.sqlite path ----
  # Priority:
  #   1. Explicit path passed by caller
  #   2. here() project root
  #   3. Directory of the calling script (sys.frames)
  #   4. Current working directory

  if (is.null(lookup_path)) {
    candidates <- c(
      here("lookup.sqlite"),                          # project root via here()
      file.path(dirname(here()), "lookup.sqlite"),    # one level above here() root
      file.path(getwd(), "lookup.sqlite"),            # working directory
      file.path(getwd(), "..", "lookup.sqlite")       # one level above working directory
    )

    # Normalise paths and take the first one that actually exists
    candidates <- normalizePath(candidates, mustWork = FALSE)
    lookup_path <- candidates[file.exists(candidates)][1]

    if (is.na(lookup_path) || length(lookup_path) == 0) {
      stop(
        "Cannot find lookup.sqlite. Searched in:\n",
        paste(" -", candidates, collapse = "\n"),
        "\nEither place lookup.sqlite in your project root, or pass the ",
        "explicit path via the lookup_path argument:\n",
        "  convert_OCADS(data, password, user, lookup_path = 'C:/path/to/lookup.sqlite')"
      )
    }

    message("Using lookup database: ", lookup_path)
  }

  # connect to databases ----
  con_biochem <- open_biochem(user = biochem.username, pass = biochem.password)
  con_lookup  <- dbConnect(RSQLite::SQLite(), lookup_path)

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
  expected_tracers   <- c( "CFC-12", "SF6", "DELO18")

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


#' Generate OCADS Metadata File from OCADS Data and Mission Info
#'
#' Produces a submission-ready metadata .xlsx file conforming to the OCADS/SDIS
#' platform requirements. Static method information is hard-coded; dynamic
#' fields (dates, coordinates, cruise info, CRM batches) are derived from the
#' data file and a small mission info list supplied by the user.
#'
#' @param OCADS_fn     Character. File path to the OCADS-format data CSV.
#' @param mission_info Named list of mission-specific fields (see details).
#' @param out_dir      Character. Directory to write the metadata file.
#'                     Defaults to the same directory as OCADS_fn.
#'
#' @details
#' \code{mission_info} should contain the following named elements:
#' \describe{
#'   \item{expocode}{e.g. "74EQ20230913"}
#'   \item{cruise_id}{e.g. "74EQ23902"}
#'   \item{platform_name}{e.g. "RRS Discovery"}
#'   \item{platform_id}{e.g. "74EQ"}
#'   \item{platform_type}{e.g. "Research Vessel"}
#'   \item{platform_owner}{e.g. "National Environment Research Council"}
#'   \item{platform_country}{e.g. "United Kingdom"}
#'   \item{dic_crm_batch}{e.g. "207;208"}
#'   \item{ta_crm_batch}{e.g. "207;208"}
#'   \item{investigators}{Data frame with columns: name, institution, address,
#'         email, orcid (one row per investigator)}
#'   \item{submitter}{Named list with: name, institution, address, email, orcid}
#'   \item{author_list}{e.g. "Beazley, Lindsay; Azetsu-Scott, Kumiko; ..."}
#'   \item{geographic_names}{e.g. "SCOTIAN SHELF"}
#'   \item{funding_agency}{e.g. "Government of Canada: Fisheries and Oceans Canada"}
#'   \item{funding_project_title}{e.g. "Aquatic Climate Change Adaptation Services Program"}
#'   \item{funding_project_id}{e.g. "881-15K-96036"}
#'   \item{research_project}{e.g. "Atlantic Zone Monitoring Program"}
#' }
#'
#' @return File path to the written metadata xlsx (invisibly).
#' @export
generate_OCADS_metadata <- function(OCADS_fn,
                                    mission_info,
                                    out_dir = NULL) {

  suppressPackageStartupMessages({
    require(tidyverse)
    require(openxlsx)
  })

  # ---------------------------------------------------------------------------
  # 0. Helper — build a two-column data frame row (Field | Value)
  # ---------------------------------------------------------------------------
  row2 <- function(field, value) {
    tibble(Field = as.character(field),
           Value = as.character(value))
  }

  # ---------------------------------------------------------------------------
  # 1. Load OCADS data to derive dynamic fields
  # ---------------------------------------------------------------------------
  dat <- read_csv(OCADS_fn,
                  show_col_types = FALSE,
                  col_types = cols(.default = "c"),
                  skip = 1)   # skip units row

  # Parse dates — handle YYYYMMDD or YYYY-MM-DD
  parse_date <- function(x) {
    x <- na.omit(x)
    if (all(nchar(x) == 8 & !grepl("-", x))) {
      as.Date(x, "%Y%m%d")
    } else {
      as.Date(x)
    }
  }

  dates      <- parse_date(dat$DATE)
  start_date <- format(min(dates, na.rm = TRUE), "%Y-%m-%d")
  end_date   <- format(max(dates, na.rm = TRUE), "%Y-%m-%d")

  lat  <- as.numeric(dat$LATITUDE)
  lon  <- as.numeric(dat$LONGITUDE)
  north <- round(max(lat, na.rm = TRUE), 5)
  south <- round(min(lat, na.rm = TRUE), 5)
  east  <- round(max(lon, na.rm = TRUE), 5)
  west  <- round(min(lon, na.rm = TRUE), 5)

  # ---------------------------------------------------------------------------
  # 2. Static method blocks
  #    These do not change between missions.  Update here if methods change.
  # ---------------------------------------------------------------------------

  # --- QC flag legend (shared by all variables) ---
  qc_flag_desc <- paste0(
    "1 = Sample drawn from bottle but analysis not received; ",
    "2 = QC Performed: Acceptable Measurement; ",
    "3 = QC Performed: Questionable Measurement; ",
    "4 = QC Performed: Bad Measurement; ",
    "5 = QC Performed: Not Reported; ",
    "6 = Mean of replicate measurements; ",
    "7 = Manual chromatographic peak measurement; ",
    "8 = Irregular digital chromatographic peak integration; ",
    "9 = Not sampled"
  )

  ctd_qc_flag_desc <- paste0(
    "\"\" = No QC Performed; ",
    qc_flag_desc
  )

  # --- DIC static fields ---
  dic_static <- bind_rows(
    row2("DIC: Variable abbreviation in data files",  "TCARBN"),
    row2("DIC: Variable unit",                        "umol/kg"),
    row2("DIC: Observation type",                     "profile"),
    row2("DIC: Measured or calculated",               "measured"),
    row2("DIC: Calculation method and parameters",    ""),
    row2("DIC: Sampling instrument",                  "Niskin bottle, 10 L"),
    row2("DIC: Analyzing instrument",                 "SOMMA with UIC 5011 coulometer"),
    row2("DIC: Detailed sampling and analyzing information",
         paste0("Seawater samples were collected from standard depths in 500 mL borosilicate ",
                "glass reagent bottles. Five mL of water was removed to allow room for thermal ",
                "expansion and the sample was preserved within 30 minutes of collection with ",
                "0.1 mL of a mercuric chloride saturated solution, then sealed with Apiezon M ",
                "grease and the ground glass stoppers secured in place with rubber bands. The ",
                "samples were stored at room temperature until on-shore laboratory analysis. ",
                "The samples were analysed for DIC using a SOMMA sample handling system in ",
                "conjunction with a coulometer (UIC Inc.) to quantify total CO2 purged from ",
                "the acidified sample. Measurements of a Certified reference material seawater ",
                "(CRMs, Scripps Oceanographic Institution) were used for calibrating DIC with ",
                "each batch of 20 samples being bracketed by duplicate CRM measurements.")),
    row2("DIC: Field replicate information",          ""),
    row2("DIC: Standardization technique description",
         "Referenced to duplicate measurement of CRM prior to, and following, each daily batch of DIC samples"),
    row2("DIC: Frequency of standardization",        "To bracket each batch of 20 samples"),
    row2("DIC: CRM manufacturer",                    "Andrew Dickson, Scripps Oceanographic Institute"),
    row2("DIC: Batch number",                        mission_info$dic_crm_batch),
    row2("DIC: Poison used to kill the sample",      "Mercuric chloride saturated solution"),
    row2("DIC: Poison volume",                       "100 uL per 500 ml sample bottle = 0.02%"),
    row2("DIC: Poisoning correction description",    "None"),
    row2("DIC: Uncertainty",                         "0.0015"),
    row2("DIC: Data quality flag description",       qc_flag_desc),
    row2("DIC: Method reference (citation)",
         paste0("Dickson, A.G., Sabine, C.L. and Christian, J.R. (Eds.) 2007. ",
                "Guide to Best Practices for Ocean CO2 Measurements. PICES Special Publication 3, ",
                "191 pp., https://cdiac.ess-dive.lbl.gov/ftp/oceans/Handbook_2007/Guide_all_in_one.pdf")),
    row2("DIC: Researcher Name",        "Kumiko Azetsu-Scott"),
    row2("DIC: Researcher Institution", "Bedford Institute of Oceanography")
  )

  # --- TA static fields ---
  ta_static <- bind_rows(
    row2("TA: Variable abbreviation in data files",  "ALKALI"),
    row2("TA: Variable unit",                        "umol/kg"),
    row2("TA: Observation type",                     "profile"),
    row2("TA: Measured or calculated",               "measured"),
    row2("TA: Calculation method and parameters",
         "Concentration (umol / kg) = concentration (umoles/litre) / density (10 C,S)"),
    row2("TA: Sampling instrument",                  "Niskin bottle, 10 L"),
    row2("TA: Analyzing instrument",
         "In-house automated sampler with Metrohm Titrando dosimat and Tiamo software"),
    row2("TA: Type of titration",                    "Potentiometric multi point titration"),
    row2("TA: Cell type (open or closed)",           "Open"),
    row2("TA: Curve fitting method",                 "Gran point"),
    row2("TA: Detailed sampling and analyzing information",
         paste0("Seawater samples were collected from standard depths in 500 mL borosilicate ",
                "glass reagent bottles. Five mL of water was removed to allow room for thermal ",
                "expansion and the sample was preserved within 30 minutes of collection with ",
                "0.1 mL of a mercuric chloride saturated solution, then sealed with Apiezon M ",
                "grease and the ground glass stoppers secured in place with rubber bands. The ",
                "samples were stored at room temperature until on-shore laboratory analysis. ",
                "TA in the sample was determined using an open cell automated potentiometric ",
                "titration with a Metrohm Titrando dosing unit controlled by Metrohm Tiamo ",
                "software. The multi-point titration, using 0.1N HCl titrant containing 35 g ",
                "of sodium chloride per litre of acid, was performed in a temperature controlled ",
                "flask held at 25 degrees C with Gran endpoint determination. Measurements of a ",
                "Certified reference material seawater (CRMs, Scripps Oceanographic Institution) ",
                "were used for calibrating TA with each batch of 20 samples being bracketed by ",
                "duplicate CRM measurements.")),
    row2("TA: Field replicate information",          ""),
    row2("TA: Standardization technique description",
         "Referenced to duplicate measurements of a CRM before and after each daily batch of samples"),
    row2("TA: Frequency of standardization",        "To bracket each batch of 20 samples"),
    row2("TA: CRM manufacturer",                    "Andrew Dickson, Scripps Oceanographic Institute"),
    row2("TA: Batch Number",                        mission_info$ta_crm_batch),
    row2("TA: Poison used to kill the sample",      "Mercuric chloride saturated solution"),
    row2("TA: Poison volume",                       "100 uL per 500 ml sample bottle = 0.02%"),
    row2("TA: Poisoning correction description",    "None"),
    row2("TA: Magnitude of blank correction",       ""),
    row2("TA: Uncertainty",                         "0.0025"),
    row2("TA: Data quality flag description",       qc_flag_desc),
    row2("TA: Method reference (citation)",
         paste0("Dickson, A.G., Sabine, C.L. and Christian, J.R. (Eds.) 2007. ",
                "Guide to Best Practices for Ocean CO2 Measurements. PICES Special Publication 3, ",
                "191 pp., https://cdiac.ess-dive.lbl.gov/ftp/oceans/Handbook_2007/Guide_all_in_one.pdf")),
    row2("TA: Researcher Name",        "Kumiko Azetsu-Scott"),
    row2("TA: Researcher Institution", "Bedford Institute of Oceanography")
  )

  # --- pCO2 discrete static fields ---
  pco2_static <- bind_rows(
    row2("pCO2D: Variable abbreviation in data files", "PCO2"),
    row2("pCO2D: Variable unit",                       "uatm"),
    row2("pCO2D: Observation type",                    "profile"),
    row2("pCO2D: Measured or calculated",              "measured"),
    row2("pCO2D: Calculation method and parameters",   ""),
    row2("pCO2D: Sampling instrument",                 "Niskin bottle, 10 L"),
    row2("pCO2D: Analyzing instrument",
         "SRI 8610C gas chromatograph with flame ionization detector and methanizer"),
    row2("pCO2D: Detailed sampling and analyzing information",
         paste0("Seawater samples were collected in 160 mL volume crimp seal serum bottles, ",
                "allowing the bottle to overflow by 3 volumes. The sample was immediately ",
                "stabilised by the addition of 50 uL of saturated mercuric chloride solution, ",
                "crimp sealed with a butyl rubber septum then stored in a refrigerator. Before ",
                "analysis, a 11 mL headspace of 400 ppm CO2 in zero air was introduced into the ",
                "bottle which was then thermally equilibrated at 22 degrees C for one hour. The ",
                "bottle was then shaken vigorously for 8 minutes and the headspace displaced by a ",
                "brine solution, flushing the sample loop of an SRI gas chromatograph equipped ",
                "with a methaniser and flame ionisation detector. CO2 peaks were calibrated by ",
                "injections of primary standard gas mixtures (Air Liquide) having CO2 mixing ",
                "ratios of 397.3, 797.2, 1201 and 2017 ppm.")),
    row2("pCO2D: Storage method",                      "Vials stored in refrigerator"),
    row2("pCO2D: Seawater volume (mL)",                "150 mL"),
    row2("pCO2D: Headspace volume (mL)",               "11 mL"),
    row2("pCO2D: Temperature of measurement",          "22 degrees Celsius"),
    row2("pCO2D: Field replicate information",         ""),
    row2("pCO2D: Manufacturer of the gas detector",   "SRI instruments"),
    row2("pCO2D: Model of the gas detector",           "8610 GC-FID with methanizer"),
    row2("pCO2D: Resolution of the gas detector",      "0.1 uatm"),
    row2("pCO2D: Uncertainty of the gas detector",     "1 uatm"),
    row2("pCO2D: Standardization technique description",
         "Calibration plot made up from injections of primary gas standards, 400 ppm, 800 ppm, 1200 ppm"),
    row2("pCO2D: Frequency of standardization",
         "Twice daily, before and after each batch of samples"),
    row2("pCO2D: Temperature of standardization",     "25 degrees Celsius"),
    row2("pCO2D: Manufacturer of standard gas",       "Air Liquide"),
    row2("pCO2D: Concentrations of standard gas",     "397.3 ppm, 797.2 ppm, 1201 ppm"),
    row2("pCO2D: Uncertainties of standard gas",      "0.01"),
    row2("pCO2D: Water vapor correction method",
         "Dickson et al., 2007, Guide to best practices for ocean CO2 measurements, SOP 4, section 8.3"),
    row2("pCO2D: Temperature correction method",      "None"),
    row2("pCO2D: at what temperature was pCO2 reported", "22 degrees Celsius"),
    row2("pCO2D: Uncertainty",                        "0.0025"),
    row2("pCO2D: Data quality flag description",      qc_flag_desc),
    row2("pCO2D: Method reference (citation)",
         paste0("Dickson, A.G., Sabine, C.L. and Christian, J.R. (Eds.) 2007. ",
                "Guide to Best Practices for Ocean CO2 Measurements. PICES Special Publication 3, ",
                "191 pp., https://cdiac.ess-dive.lbl.gov/ftp/oceans/Handbook_2007/Guide_all_in_one.pdf")),
    row2("pCO2D: Researcher Name",        "Kumiko Azetsu-Scott"),
    row2("pCO2D: Researcher Institution", "Bedford Institute of Oceanography")
  )

  # --- CTD / sensor variable static blocks ---
  # Helper to build a CTD sensor block quickly
  ctd_var_block <- function(abbrev, full_name, unit, detail_name,
                            researcher = "Lindsay Beazley",
                            institution = "Bedford Institute of Oceanography",
                            method_ref = "") {
    bind_rows(
      row2(paste0(abbrev, ": Variable abbreviation in data files"), abbrev),
      row2(paste0(abbrev, ": Full variable name"),                  full_name),
      row2(paste0(abbrev, ": Variable unit"),                       unit),
      row2(paste0(abbrev, ": Observation type"),                    "profile"),
      row2(paste0(abbrev, ": Sampling instrument"),                 "CTD"),
      row2(paste0(abbrev, ": Analyzing instrument"),                "not applicable"),
      row2(paste0(abbrev, ": Detailed sampling and analyzing information"),
           paste0("Name - ", detail_name, "; Preservative - not applicable; Storage - not applicable")),
      row2(paste0(abbrev, ": Field replicate information"),         ""),
      row2(paste0(abbrev, ": Uncertainty"),                         ""),
      row2(paste0(abbrev, ": Data quality flag description"),       ctd_qc_flag_desc),
      row2(paste0(abbrev, ": Method reference (citation)"),        method_ref),
      row2(paste0(abbrev, ": Researcher Name"),                    researcher),
      row2(paste0(abbrev, ": Researcher Institution"),             institution)
    )
  }

  # Helper for bottle-sampled nutrient / chemistry variables
  bottle_var_block <- function(abbrev, full_name, unit, detail_name,
                               method_ref,
                               researcher, institution,
                               preserve = "frozen (-20)",
                               storage  = "frozen (-20)",
                               replicates = "Average of replicates",
                               filter_type = "Unfiltered",
                               convert_note = "") {
    detail <- paste0("Name - ", detail_name,
                     "; Preservative - ", preserve,
                     "; Storage - ", storage,
                     if (nchar(convert_note) > 0) paste0("; ", convert_note) else "")
    bind_rows(
      row2(paste0(abbrev, ": Variable abbreviation in data files"), abbrev),
      row2(paste0(abbrev, ": Full variable name"),                  full_name),
      row2(paste0(abbrev, ": Variable unit"),                       unit),
      row2(paste0(abbrev, ": Observation type"),                    "profile"),
      row2(paste0(abbrev, ": Sampling instrument"),                 "Niskin bottle, 10 L"),
      row2(paste0(abbrev, ": Analyzing instrument"),                filter_type),
      row2(paste0(abbrev, ": Detailed sampling and analyzing information"), detail),
      row2(paste0(abbrev, ": Field replicate information"),         replicates),
      row2(paste0(abbrev, ": Uncertainty"),                         ""),
      row2(paste0(abbrev, ": Data quality flag description"),       qc_flag_desc),
      row2(paste0(abbrev, ": Method reference (citation)"),        method_ref),
      row2(paste0(abbrev, ": Researcher Name"),                    researcher),
      row2(paste0(abbrev, ": Researcher Institution"),             institution)
    )
  }

  # Helper for HPLC pigment variables
  hplc_block <- function(abbrev, full_name) {
    hplc_ref <- paste0(
      "High Performance Liquid Chromatography (HPLC) assay of acetone extraction (GFF filtered). ",
      "Head, E.J., Horne, E.P.W. (1993) Pigment transformation and vertical flux in an area of ",
      "convergence in the North Atlantic. Deep-Sea Res. II. 40:329-346"
    )
    bind_rows(
      row2(paste0(abbrev, ": Variable abbreviation in data files"), abbrev),
      row2(paste0(abbrev, ": Full variable name"),                  full_name),
      row2(paste0(abbrev, ": Variable unit"),                       "mg/m3"),
      row2(paste0(abbrev, ": Observation type"),                    "profile"),
      row2(paste0(abbrev, ": Sampling instrument"),                 "Niskin bottle, 10 L"),
      row2(paste0(abbrev, ": Analyzing instrument"),                "GFF_filtered"),
      row2(paste0(abbrev, ": Detailed sampling and analyzing information"),
           paste0("Name - ", full_name,
                  "; Preservative - frozen (-196); Storage - frozen and/or desiccated")),
      row2(paste0(abbrev, ": Field replicate information"),         ""),
      row2(paste0(abbrev, ": Uncertainty"),                         ""),
      row2(paste0(abbrev, ": Data quality flag description"),       qc_flag_desc),
      row2(paste0(abbrev, ": Method reference (citation)"),        hplc_ref),
      row2(paste0(abbrev, ": Researcher Name"),                    "Emmanuel Devred"),
      row2(paste0(abbrev, ": Researcher Institution"),             "Bedford Institute of Oceanography")
    )
  }

  nutrient_ref <- paste0(
    "Strain, P.M. and P.M. Clement. 1996. Nutrient and dissolved Oxygen concentrations in the ",
    "LeTange Inlet, New Brunswick, in the Summer of 1994. Can. Data Rep. Fish. Aquat. Sci. 1004"
  )
  nutrient_convert <- paste0(
    "Converted from ug/L using 15 degrees C, CTD salinity, and CTD pressure ",
    "for volume to mass conversion"
  )
  poc_ref <- paste0(
    "Ehrhardt, M (1983) Determination of particulate organic carbon and nitrogen, p. 268-275. ",
    "In K Grasshoff, M Ehrhardt, K Kremling (eds), Methods of seawater analysis, 2nd revised ",
    "and extended edition. Verlag Chemie GmbH, Weinheim. 419 pp"
  )
  o2_ref <- paste0(
    "Levy, E.M, C.C. Cunningham, C.D. Conrad and J.D. Moffat. 1977. A titration apparatus for ",
    "the determination of dissolved Oxygen in seawater. J. Fish. Res. Board. Can. 34:2218-2220."
  )
  chl_ref <- paste0(
    "Holm-Hansen, O, CJ Lorenzen, RW Holmes, JD Strickland (1965) Fluorometric determination ",
    "of chlorophyll. J Cons Cons Int Explor Mer, 30:3-15."
  )

  # ---------------------------------------------------------------------------
  # 3. Build investigator blocks dynamically from mission_info$investigators
  # ---------------------------------------------------------------------------
  build_investigator_rows <- function(investigators) {
    pmap_dfr(
      list(
        investigators$name,
        investigators$institution,
        investigators$address,
        investigators$email,
        investigators$orcid,
        seq_len(nrow(investigators))
      ),
      function(nm, inst, addr, email, orcid, idx) {
        bind_rows(
          row2(paste0("Investigator-", idx, " name"),        nm),
          row2(paste0("Investigator-", idx, " institution"), inst),
          row2(paste0("Investigator-", idx, " address"),     addr),
          row2(paste0("Investigator-", idx, " phone"),       ""),
          row2(paste0("Investigator-", idx, " email"),       email),
          row2(paste0("Investigator-", idx, " researcher ID"), orcid),
          row2(paste0("Investigator-", idx, " ID type (ORCID, Researcher ID, etc.)"), "ORCID")
        )
      }
    )
  }

  # ---------------------------------------------------------------------------
  # 4. Assemble the full metadata table in OCADS field order
  # ---------------------------------------------------------------------------
  metadata <- bind_rows(

    # -- Submission info --
    row2("Submission Date",            format(Sys.Date(), "%m/%d/%Y")),
    row2("Data submitter name",        mission_info$submitter$name),
    row2("Data submitter institution", mission_info$submitter$institution),
    row2("Data submitter address",     mission_info$submitter$address),
    row2("Data submitter phone",       ""),
    row2("Data submitter email",       mission_info$submitter$email),
    row2("Data submitter researcher ID", mission_info$submitter$orcid),
    row2("Data submitter ID type (ORCID, Researcher ID, etc.)", "ORCID"),

    # -- Dataset identifiers --
    row2("EXPOCODE",  mission_info$expocode),
    row2("Cruise ID", mission_info$cruise_id),
    row2("Section",   ""),

    # -- Title / Abstract --
    row2("Title",
         paste0("Atlantic Zone Monitoring Program: ",
                mission_info$platform_name, " (", mission_info$expocode, ")")),
    row2("Abstract",
         paste0("Oceanographic sampling of physical, biological and chemical parameters is ",
                "performed biweekly at selected fixed stations and two to three times annually ",
                "along selected fixed sections, as part of the Atlantic Zone Monitoring Program. ",
                "The sampling consists at minimum of vertical profile of the entire water column, ",
                "water bottle sampling at selected depths of nutrients, salinity and oxygen, ",
                "vertical net tows for zooplankton and Secchi depth measurement. ",
                "See: http://science-catalogue.canada.ca/record=b3951030~S6 for more details.")),
    row2("Purpose",
         paste0("The Atlantic Zone Monitoring Program (AZMP) was implemented in 1998 with the ",
                "aim of increasing Fisheries and Oceans Canada's (DFO) capacity to understand, ",
                "describe, and forecast the state of the marine ecosystem and to quantify the ",
                "changes in the ocean physical, chemical and biological properties. A critical ",
                "element of the AZMP involves an observation program aimed at assessing the ",
                "variability in nutrients, phytoplankton and zooplankton.")),

    # -- Temporal coverage --
    row2("Start date", start_date),
    row2("End date",   end_date),

    # -- Spatial coverage --
    row2("Westbd longitude",  west),
    row2("Eastbd longitude",  east),
    row2("Northbd latitude",  north),
    row2("Southbd latitude",  south),
    row2("Geographic names",  mission_info$geographic_names),

    # -- Platform --
    row2("Platform-1 name",    mission_info$platform_name),
    row2("Platform-1 ID",      mission_info$platform_id),
    row2("Platform-1 type",    mission_info$platform_type),
    row2("Platform-1 owner",   mission_info$platform_owner),
    row2("Platform-1 country", mission_info$platform_country),

    # Blank platform slots 2 and 3 (required by template)
    row2("Platform-2 name",    ""), row2("Platform-2 ID",      ""),
    row2("Platform-2 type",    ""), row2("Platform-2 owner",   ""),
    row2("Platform-2 country", ""),
    row2("Platform-3 name",    ""), row2("Platform-3 ID",      ""),
    row2("Platform-3 type",    ""), row2("Platform-3 owner",   ""),
    row2("Platform-3 country", ""),

    # -- Funding --
    row2("Funding agency name",        mission_info$funding_agency),
    row2("Funding project title",      mission_info$funding_project_title),
    row2("Funding project ID (Grant no.)", mission_info$funding_project_id),
    row2("Research projects",          mission_info$research_project),

    # -- Author list --
    row2("Author list for citation",   mission_info$author_list),

    # -- References --
    row2("References",
         "https://www.dfo-mpo.gc.ca/science/data-donnees/azmp-pmza/index-eng.html#data"),
    row2("Supplemental information",
         paste0("Data extracted from BioChem, the Fisheries and Oceans Canada database for ",
                "biological and chemical data (Devine, L., M.K. Kennedy, I. St-Pierre, ",
                "C. Lafleur, M. Ouellet, and data. Bond. 2014. BioChem: the Fisheries and Oceans ",
                "Canada database for biological and chemical data. Can. Tech. Rep. Fish. Aquat. ",
                "Sci. 3073: iv + 40 pp., http://science-catalogue.canada.ca/record=b4008162~S6)")),

    # -- Investigators (dynamic) --
    build_investigator_rows(mission_info$investigators),

    # -- Accession --
    row2("Accession no. of related data sets", ""),

    # ---- Carbon system variables ----
    dic_static,
    ta_static,
    pco2_static,

    # pH (sensor only — no lab pH for this program)
    row2("pH: Variable abbreviation in data files",  ""),
    row2("pH: pH scale",                             ""),
    row2("pH: Observation type",                     ""),
    row2("pH: Measured or calculated",               ""),
    row2("pH: Calculation method and parameters",    ""),
    row2("pH: Sampling instrument",                  ""),
    row2("pH: Analyzing instrument",                 ""),
    row2("pH: Temperature of measurement",           ""),
    row2("pH: Detailed sampling and analyzing information", ""),
    row2("pH: Field replicate information",          ""),
    row2("pH: Standardization technique description", ""),
    row2("pH: Frequency of standardization",         ""),
    row2("pH: pH values of the standards",           ""),
    row2("pH: Temperature of standardization",       ""),
    row2("pH: Temperature correction method",        ""),
    row2("pH: at what temperature was pH reported",  ""),
    row2("pH: Uncertainty",                          ""),
    row2("pH: Data quality flag description",        ""),
    row2("pH: Method reference (citation)",          ""),
    row2("pH: Researcher Name",                      ""),
    row2("pH: Researcher Institution",               ""),

    # pCO2 autonomous (not used)
    row2("pCO2A: Variable abbreviation in data files", ""),
    row2("pCO2A: Variable unit",                       ""),
    row2("pCO2A: Observation type",                    ""),
    row2("pCO2A: Measured or calculated",              ""),
    row2("pCO2A: Calculation method and parameters",   ""),
    row2("pCO2A: Sampling instrument",                 ""),
    row2("pCO2A: Location of seawater intake",         ""),
    row2("pCO2A: Depth of seawater intake",            ""),
    row2("pCO2A: Analyzing instrument",                ""),
    row2("pCO2A: Detailed sampling and analyzing information", ""),
    row2("pCO2A: Equilbrator type",                    ""),
    row2("pCO2A: Equilibrator volume (L)",             ""),
    row2("pCO2A: Vented or not",                       ""),
    row2("pCO2A: Water flow rate (L/min)",             ""),
    row2("pCO2A: Headspace gas flow rate (L/min)",     ""),
    row2("pCO2A: How was temperature inside the equilibrator measured", ""),
    row2("pCO2A: How was pressure inside the equilibrator measured",    ""),
    row2("pCO2A: Drying method for CO2 gas",           ""),
    row2("pCO2A: Manufacturer of the gas detector",    ""),
    row2("pCO2A: Model of the gas detector",           ""),
    row2("pCO2A: Resolution of the gas detector",      ""),
    row2("pCO2A: Uncertainty of the gas detector",     ""),
    row2("pCO2A: Standardization technique description", ""),
    row2("pCO2A: Frequency of standardization",        ""),
    row2("pCO2A: Manufacturer of standard gas",        ""),
    row2("pCO2A: Concentrations of standard gas",      ""),
    row2("pCO2A: Uncertainties of standard gas",       ""),
    row2("pCO2A: Water vapor correction method",       ""),
    row2("pCO2A: Temperature correction method",       ""),
    row2("pCO2A: at what temperature was pCO2 reported", ""),
    row2("pCO2A: Uncertainty",                         ""),
    row2("pCO2A: Data quality flag description",       ""),
    row2("pCO2A: Method reference (citation)",         ""),
    row2("pCO2A: Researcher Name",                     ""),
    row2("pCO2A: Researcher Institution",              ""),

    # ---- CTD sensor variables ----
    ctd_var_block("CTDCDOM",  "CDOM",         "mg/m3",    "CDOM",
                  method_ref = "CDOM sensor, part of the Sea-Bird CTD package"),
    ctd_var_block("CTDFLUOR", "CHL-SENSOR insitu", "mg/m3", "CHL-SENSOR insitu"),
    ctd_var_block("CTDOXY",   "O2",           "umol/kg",  "O2",
                  method_ref = "Calibrated sensor as part of the CTD package"),
    ctd_var_block("CTDPRS",   "Pressure",     "dbar",     "Pressure"),
    ctd_var_block("CTDSAL",   "Salinity",     "none",     "Salinity"),
    ctd_var_block("CTDTMP",   "Temperature",  "degreesC", "Temperature",
                  method_ref = "Temperature measured at depth using calibrated sensor as part of CTD package"),
    ctd_var_block("CTDPH",    "pH",           "none",     "pH",
                  method_ref = "pH sensor, part of the CTD package, not calibrated with lab measures"),
    ctd_var_block("PAR",      "PAR",          "uEm-2s-1", "PAR",
                  method_ref = "Photosynthetically active radiation sensor attached to CTD package"),
    ctd_var_block("CTDBETA650_124", "TURB_B", "m-1sr-1",  "TURB_B"),

    # ---- Bottle variables ----
    bottle_var_block("CHLORA",  "Chlorophyll A", "mg/m3", "Chlorophyll A",
                     chl_ref, "Emmanuel Devred", "Bedford Institute of Oceanography",
                     preserve = "frozen (-20)", storage = "frozen (-20)",
                     filter_type = "Pre-filtered"),
    bottle_var_block("PPHYTN",  "Phaeophytin",  "mg/m3", "Phaeophytin",
                     chl_ref, "Emmanuel Devred", "Bedford Institute of Oceanography",
                     preserve = "frozen (-20)", storage = "frozen (-20)",
                     filter_type = "Pre-filtered"),
    bottle_var_block("SALNTY",  "Salinity",     "none",  "Salinity",
                     "Autosal salinometer, calibrated against standard seawater.",
                     "Lindsay Beazley", "Bedford Institute of Oceanography",
                     preserve = "not applicable", storage = "not applicable",
                     replicates = "", filter_type = "not applicable"),
    bottle_var_block("OXYGEN",  "O2",           "umol/kg", "O2",
                     o2_ref, "Lindsay Beazley", "Bedford Institute of Oceanography",
                     preserve = "not assigned", storage = "not applicable",
                     convert_note = paste0("Converted from mL/L to umol/kg using conversion ",
                                           "factor 44.66 and CTD temperature, salinity, and pressure")),
    bottle_var_block("NH3",     "Ammonia",      "umol/kg", "Ammonia",
                     nutrient_ref, "Marc Ringuette", "Bedford Institute of Oceanography",
                     preserve = "frozen", storage = "frozen",
                     convert_note = nutrient_convert),
    bottle_var_block("NITRIT",  "Nitrite",      "umol/kg", "Nitrite",
                     nutrient_ref, "Marc Ringuette", "Bedford Institute of Oceanography",
                     convert_note = nutrient_convert),
    bottle_var_block("NO2+NO3", "Nitrate",      "umol/kg", "Nitrate",
                     nutrient_ref, "Marc Ringuette", "Bedford Institute of Oceanography",
                     convert_note = nutrient_convert),
    bottle_var_block("PHSPHT",  "Phosphate",    "umol/kg", "Phosphate",
                     nutrient_ref, "Marc Ringuette", "Bedford Institute of Oceanography",
                     convert_note = nutrient_convert),
    bottle_var_block("SILCAT",  "Silicate",     "umol/kg", "Silicate",
                     nutrient_ref, "Marc Ringuette", "Bedford Institute of Oceanography",
                     convert_note = nutrient_convert),
    bottle_var_block("POC",     "POC",          "ug/kg",   "POC",
                     poc_ref,  "Marc Ringuette", "Bedford Institute of Oceanography",
                     preserve = "not assigned", storage = "not assigned",
                     convert_note = nutrient_convert),
    bottle_var_block("PON",     "PON",          "ug/kg",   "PON",
                     poc_ref,  "Marc Ringuette", "Bedford Institute of Oceanography",
                     preserve = "not assigned", storage = "not assigned",
                     convert_note = nutrient_convert),

    # ---- HPLC pigments ----
    hplc_block("ALPHA-CAR",     "HPLC_ACAROT"),
    hplc_block("ALLO",          "HPLC_ALLOX"),
    hplc_block("HPLC_ASTAX",    "HPLC_ASTAX"),
    hplc_block("BETA-CAR",      "HPLC_BCAROT"),
    hplc_block("BUT-FUCO",      "HPLC_BUT19"),
    hplc_block("HPLC_BUTLIKE",  "HPLC_BUTLIKE"),
    hplc_block("TOT_CHL_A",     "HPLC_CHLA"),
    hplc_block("TOT_CHL_B",     "HPLC_CHLB"),
    hplc_block("CHL_C1C2",      "HPLC_CHLC12"),
    hplc_block("CHL_C3",        "HPLC_CHLC3"),
    hplc_block("CHLIDE_A",      "HPLC_CHLIDEA"),
    hplc_block("DIADINO",       "HPLC_DIADINOX"),
    hplc_block("DIATO",         "HPLC_DIATOX"),
    hplc_block("HFUCO",         "HPLC_FUCOX"),
    hplc_block("HEX-FUCO",      "HPLC_HEX19"),
    hplc_block("HPLC_HEXLIKE",  "HPLC_HEXLIKE"),
    hplc_block("HPLC_HEXLIKE2", "HPLC_HEXLIKE2"),
    hplc_block("PHYTIN_A",      "HPLC_PHAEO"),
    hplc_block("PERID",         "HPLC_PERID"),
    hplc_block("PRAS",          "HPLC_PRASINOX"),
    hplc_block("HPLC_PYROPHAE", "HPLC_PYROPHAE"),
    hplc_block("VIOLA",         "HPLC_VIOLAX"),
    hplc_block("ZEA",           "HPLC_ZEA")

  ) # end bind_rows

  # ---------------------------------------------------------------------------
  # 5. Write to xlsx — two columns: Field | Value
  #    SDIS expects this exact two-column format
  # ---------------------------------------------------------------------------
  if (is.null(out_dir)) {
    out_dir <- dirname(OCADS_fn)
  }
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  out_path <- file.path(
    out_dir,
    paste0(mission_info$expocode, "_metadata.xlsx")
  )

  wb <- createWorkbook()
  addWorksheet(wb, "Metadata")

  # Write headers + data
  writeData(wb, "Metadata",
            x        = metadata,
            startRow = 1,
            colNames = TRUE,
            rowNames = FALSE)

  # Style: bold header
  addStyle(wb, "Metadata",
           style      = createStyle(textDecoration = "bold", wrapText = FALSE),
           rows       = 1,
           cols       = 1:2,
           gridExpand = TRUE)

  # Style: Field column — bold, fixed width
  setColWidths(wb, "Metadata", cols = 1, widths = 55)
  setColWidths(wb, "Metadata", cols = 2, widths = 120)

  # Style: wrap text in Value column for long method descriptions
  addStyle(wb, "Metadata",
           style      = createStyle(wrapText = TRUE, valign = "top"),
           rows       = 2:(nrow(metadata) + 1),
           cols       = 2,
           gridExpand = TRUE)

  # Style: Field column — no wrap, top-aligned
  addStyle(wb, "Metadata",
           style      = createStyle(wrapText = FALSE, valign = "top"),
           rows       = 2:(nrow(metadata) + 1),
           cols       = 1,
           gridExpand = TRUE)

  # Freeze the header row
  freezePane(wb, "Metadata", firstRow = TRUE)

  saveWorkbook(wb, file = out_path, overwrite = TRUE)
  message("OCADS metadata file written: ", out_path)

  invisible(out_path)
}
