#' Extract BCD Data Directly from BioChem
#'
#' This function replaces the manual workflow of running SQL queries in Oracle SQL Developer
#' and exporting to CSV. It connects directly to BioChem, extracts data for a specified mission,
#' and returns it in BCD format ready for conversion to OCADS/CCHDO formats.
#'
#' @param mission_descriptor chr, the mission descriptor (e.g., "18QL23573", "CAR2023573")
#' @param biochem.username chr, BioChem username (optional if already in environment)
#' @param biochem.password chr, BioChem password (optional if already in environment)
#' @param validate logical, whether to perform preliminary data validation (default: TRUE)
#'
#' @details
#' This function executes the BCD query against the biochem.bcdiscrete_mv materialized view,
#' extracting both individual samples (AVERAGED_DATA = 'N') and replicate measurements
#' (AVERAGED_DATA = 'Y' with individual replicates from bcdiscretereplicates table).
#'
#' The returned dataframe includes all required columns for convert_OCADS():
#' - MISSION_DESCRIPTOR
#' - EVENT_COLLECTOR_EVENT_ID
#' - EVENT_COLLECTOR_STN_NAME
#' - DIS_HEADER_START_DEPTH, DIS_HEADER_END_DEPTH
#' - DIS_HEADER_SLAT, DIS_HEADER_SLON
#' - DIS_HEADER_SDATE (formatted as %m/%d/%Y)
#' - DIS_HEADER_STIME
#' - DATA_TYPE_METHOD
#' - DIS_DETAIL_DATA_VALUE
#' - DIS_DETAIL_DATA_QC_CODE
#' - DIS_DETAIL_COLLECTOR_SAMP_ID
#' - Additional metadata columns (DIS_DATA_NUM, CREATED_BY, etc.)
#'
#' @return A dataframe in BCD format ready for convert_OCADS()
#' @export
#'
#' @examples
#' \dontrun{
#' # Basic usage - credentials will be prompted if not provided
#' bcd_data <- extract_from_biochem("CAR2023573")
#'
#' # With explicit credentials
#' source("biochem_creds.R")
#' bcd_data <- extract_from_biochem("18QL23573", biochem.user, biochem.password)
#'
#' # Then convert directly to OCADS
#' ocads_data <- convert_OCADS(bcd_data, biochem.password, biochem.user)
#' }
extract_from_biochem <- function(mission_descriptor,
                                  biochem.username = NA,
                                  biochem.password = NA,
                                  validate = TRUE) {
  require(tidyverse)
  require(ROracle)
  require(DBI)

  message("\n=== Extracting BCD Data from BioChem ===")
  message("Mission: ", mission_descriptor)

  # Connect to BioChem ----
  message("\n[1/5] Connecting to BioChem...")
  con_biochem <- open_biochem(user = biochem.username, pass = biochem.password)
  message("  ✓ Connected successfully")

  # Build and execute query ----
  message("\n[2/5] Querying BioChem database...")

  query <- paste0("
    SELECT 
        DESCRIPTOR as MISSION_DESCRIPTOR,
        COLLECTOR_EVENT_ID as EVENT_COLLECTOR_EVENT_ID,
        COLLECTOR_STATION_NAME as EVENT_COLLECTOR_STN_NAME,
        HEADER_START_DEPTH as DIS_HEADER_START_DEPTH,
        HEADER_END_DEPTH as DIS_HEADER_END_DEPTH,
        HEADER_START_LAT as DIS_HEADER_SLAT,
        HEADER_START_LON as DIS_HEADER_SLON,
        HEADER_START as DIS_HEADER_SDATE,
        HEADER_START_TIME as DIS_HEADER_STIME,
        DATA_TYPE_SEQ as DIS_DETAIL_DATA_TYPE_SEQ,
        METHOD as DATA_TYPE_METHOD,
        DATA_VALUE as DIS_DETAIL_DATA_VALUE,
        DATA_QC_CODE as DIS_DETAIL_DATA_QC_CODE,
        DETECTION_LIMIT as DIS_DETAIL_DETECTION_LIMIT,
        COLLECTOR as DIS_DETAIL_COLLECTOR,
        COLLECTOR_SAMPLE_ID as DIS_DETAIL_COLLECTOR_SAMP_ID
    FROM biochem.bcdiscrete_mv
    WHERE AVERAGED_DATA = 'N'
    AND DESCRIPTOR = '", mission_descriptor, "'
    UNION ALL
    SELECT
        dd.DESCRIPTOR as MISSION_DESCRIPTOR,
        dd.COLLECTOR_EVENT_ID as EVENT_COLLECTOR_EVENT_ID,
        dd.COLLECTOR_STATION_NAME as EVENT_COLLECTOR_STN_NAME,
        dd.HEADER_START_DEPTH as DIS_HEADER_START_DEPTH,
        dd.HEADER_END_DEPTH as DIS_HEADER_END_DEPTH,
        dd.HEADER_START_LAT as DIS_HEADER_SLAT,
        dd.HEADER_START_LON as DIS_HEADER_SLON,
        dd.HEADER_START as DIS_HEADER_SDATE,
        dd.HEADER_START_TIME as DIS_HEADER_STIME,
        dd.DATA_TYPE_SEQ as DIS_DETAIL_DATA_TYPE_SEQ,
        dd.METHOD as DATA_TYPE_METHOD,
        br.DATA_VALUE as DIS_DETAIL_DATA_VALUE,
        br.DATA_QC_CODE as DIS_DETAIL_DATA_QC_CODE,
        br.DETECTION_LIMIT as DIS_DETAIL_DETECTION_LIMIT,
        dd.COLLECTOR as DIS_DETAIL_COLLECTOR,
        dd.COLLECTOR_SAMPLE_ID as DIS_DETAIL_COLLECTOR_SAMP_ID
    FROM biochem.bcdiscrete_mv dd, biochem.bcdiscretereplicates br
    WHERE dd.AVERAGED_DATA = 'Y'
    AND dd.discrete_detail_seq = br.discrete_detail_seq
    AND dd.DESCRIPTOR = '", mission_descriptor, "'
  ")

  bcd_data <- dbGetQuery(con_biochem, query)

  if (nrow(bcd_data) == 0) {
    dbDisconnect(con_biochem)
    stop("No data found for mission descriptor '", mission_descriptor,
         "'. Check that:\n",
         "  1. Mission descriptor is correct\n",
         "  2. Data has been uploaded to BioChem\n",
         "  3. You have access permissions to this data")
  }

  message("  ✓ Retrieved ", nrow(bcd_data), " rows")
  message("  ✓ Found ", length(unique(bcd_data$DATA_TYPE_METHOD)), " unique parameters")
  message("  ✓ Found ", length(unique(bcd_data$DIS_DETAIL_COLLECTOR_SAMP_ID)), " unique samples")

  # Format and standardize data ----
  message("\n[3/5] Formatting BCD data...")

  bcd_data <- bcd_data %>%
    mutate(
      # Ensure proper data types
      MISSION_DESCRIPTOR = as.character(MISSION_DESCRIPTOR),
      EVENT_COLLECTOR_EVENT_ID = as.character(EVENT_COLLECTOR_EVENT_ID),
      EVENT_COLLECTOR_STN_NAME = as.character(EVENT_COLLECTOR_STN_NAME),
      DIS_DETAIL_COLLECTOR_SAMP_ID = as.character(DIS_DETAIL_COLLECTOR_SAMP_ID),
      DIS_DETAIL_DATA_VALUE = as.numeric(DIS_DETAIL_DATA_VALUE),
      DIS_DETAIL_DATA_QC_CODE = as.character(DIS_DETAIL_DATA_QC_CODE),

      # Format date to expected format (%m/%d/%Y)
      DIS_HEADER_SDATE = format(as.Date(DIS_HEADER_SDATE), '%m/%d/%Y'),

      # Pad time to 4 digits
      DIS_HEADER_STIME = str_pad(as.character(DIS_HEADER_STIME), 4, pad = "0")
    )

  # Add required metadata columns if missing ----
  if (!"DIS_DATA_NUM" %in% names(bcd_data)) {
    bcd_data$DIS_DATA_NUM <- seq(1:nrow(bcd_data))
  }

  if (!"CREATED_BY" %in% names(bcd_data)) {
    bcd_data$CREATED_BY <- Sys.getenv("USERNAME")
  }

  if (!"CREATED_DATE" %in% names(bcd_data)) {
    bcd_data$CREATED_DATE <- Sys.Date()
  }

  if (!"DATA_CENTER_CODE" %in% names(bcd_data)) {
    bcd_data$DATA_CENTER_CODE <- '20'  # BIO data center code
  }

  if (!"PROCESS_FLAG" %in% names(bcd_data)) {
    bcd_data$PROCESS_FLAG <- 0
  }

  if (!"BATCH_SEQ" %in% names(bcd_data)) {
    bcd_data$BATCH_SEQ <- 1
  }

  if (!"DIS_SAMPLE_KEY_VALUE" %in% names(bcd_data)) {
    bcd_data$DIS_SAMPLE_KEY_VALUE <- paste0(
      bcd_data$MISSION_DESCRIPTOR, "_",
      bcd_data$EVENT_COLLECTOR_EVENT_ID, "_",
      bcd_data$DIS_DETAIL_COLLECTOR_SAMP_ID
    )
  }

  message("  ✓ Date formatted to %m/%d/%Y")
  message("  ✓ Added standard BioChem metadata columns")

  # Preliminary validation ----
  if (validate) {
    message("\n[4/5] Performing preliminary validation...")

    # Check for essential parameters
    params_found <- unique(bcd_data$DATA_TYPE_METHOD)

    # Core parameters that should almost always be present
    core_params <- c("Pressure", "Temperature", "Salinity")
    missing_core <- setdiff(core_params, params_found)
    if (length(missing_core) > 0) {
      warning("  [!] Missing core parameters: ", paste(missing_core, collapse = ", "))
    } else {
      message("  ✓ Core parameters present (Pressure, Temperature, Salinity)")
    }

    # Check for carbonate chemistry
    carb_params <- c("Alkalinity", "pH", "Total_CO2", "pCO2")
    carb_found <- intersect(carb_params, params_found)
    if (length(carb_found) > 0) {
      message("  ✓ Found carbonate chemistry parameters: ", paste(carb_found, collapse = ", "))
    } else {
      message("  [NOTE] No carbonate chemistry parameters found")
    }

    # Check for tracers
    tracer_params <- c("CFC-12", "SF6", "DELO18")
    tracer_found <- intersect(tracer_params, params_found)
    if (length(tracer_found) > 0) {
      message("  ✓ Found tracer parameters: ", paste(tracer_found, collapse = ", "))
    } else {
      message("  [NOTE] No tracer parameters found")
    }

    # Check for missing QC codes
    qc_summary <- bcd_data %>%
      group_by(DATA_TYPE_METHOD) %>%
      summarise(
        n_samples = n(),
        n_missing_qc = sum(is.na(DIS_DETAIL_DATA_QC_CODE)),
        n_zero_qc = sum(DIS_DETAIL_DATA_QC_CODE == "0", na.rm = TRUE),
        .groups = "drop"
      )

    params_missing_qc <- qc_summary %>%
      filter(n_missing_qc > 0) %>%
      pull(DATA_TYPE_METHOD)

    if (length(params_missing_qc) > 0) {
      warning("  [!] Parameters with missing QC codes: ",
              paste(params_missing_qc, collapse = ", "))
    }

    params_all_zero_qc <- qc_summary %>%
      filter(n_zero_qc == n_samples) %>%
      pull(DATA_TYPE_METHOD)

    if (length(params_all_zero_qc) > 0) {
      warning("  [!] Parameters with all QC codes = 0 (no QC applied): ",
              paste(params_all_zero_qc, collapse = ", "),
              "\n      These should be quality controlled before submission!")
    }

    # Check for coordinate issues
    coord_check <- bcd_data %>%
      select(DIS_HEADER_SLAT, DIS_HEADER_SLON) %>%
      distinct()

    if (any(is.na(coord_check$DIS_HEADER_SLAT)) || any(is.na(coord_check$DIS_HEADER_SLON))) {
      warning("  [!] Missing latitude or longitude values detected")
    }

    if (any(coord_check$DIS_HEADER_SLAT > 90 | coord_check$DIS_HEADER_SLAT < -90, na.rm = TRUE)) {
      warning("  [!] Latitude values out of range (-90 to 90)")
    }

    if (any(coord_check$DIS_HEADER_SLON > 180 | coord_check$DIS_HEADER_SLON < -180, na.rm = TRUE)) {
      warning("  [!] Longitude values out of range (-180 to 180)")
    }

    message("  ✓ Preliminary validation complete")
  }

  # Cleanup ----
  message("\n[5/5] Finalizing...")
  dbDisconnect(con_biochem)
  message("  ✓ Disconnected from BioChem")

  message("\n=== Extraction Complete ===")
  message("Rows extracted: ", nrow(bcd_data))
  message("Parameters: ", length(unique(bcd_data$DATA_TYPE_METHOD)))
  message("Samples: ", length(unique(bcd_data$DIS_DETAIL_COLLECTOR_SAMP_ID)))
  message("Stations: ", length(unique(bcd_data$EVENT_COLLECTOR_STN_NAME)))
  message("\nData is ready for convert_OCADS()")

  return(bcd_data)
}


#' List Available Missions in BioChem
#'
#' Helper function to query BioChem for available mission descriptors.
#' Useful when you're not sure of the exact mission descriptor format.
#'
#' @param biochem.username chr, BioChem username (optional)
#' @param biochem.password chr, BioChem password (optional)
#' @param year numeric, filter missions by year (optional)
#' @param ship_code chr, filter missions by ship code (e.g., "18QL") (optional)
#'
#' @return A dataframe with mission descriptors and basic info
#' @export
#'
#' @examples
#' \dontrun{
#' # List all missions from 2023
#' missions_2023 <- list_biochem_missions(year = 2023)
#'
#' # List all missions for a specific ship
#' missions_hudson <- list_biochem_missions(ship_code = "18HU")
#' }
list_biochem_missions <- function(biochem.username = NA,
                                   biochem.password = NA,
                                   year = NULL,
                                   ship_code = NULL) {
  require(ROracle)
  require(DBI)
  require(tidyverse)

  # Connect to BioChem
  con_biochem <- open_biochem(user = biochem.username, pass = biochem.password)

  # Build query
  query <- "
    SELECT DISTINCT
        m.DESCRIPTOR,
        m.START_DATE,
        m.END_DATE,
        COUNT(DISTINCT dd.DISCRETE_SEQ) as N_SAMPLES,
        COUNT(DISTINCT e.EVENT_SEQ) as N_EVENTS
    FROM biochem.bcmissions m
    LEFT JOIN biochem.bcevents e ON e.MISSION_SEQ = m.MISSION_SEQ
    LEFT JOIN biochem.bcdiscretehedrs dd ON dd.EVENT_SEQ = e.EVENT_SEQ
    WHERE 1=1
  "

  # Add filters if provided
  if (!is.null(year)) {
    query <- paste0(query, " AND EXTRACT(YEAR FROM m.START_DATE) = ", year)
  }

  if (!is.null(ship_code)) {
    query <- paste0(query, " AND m.DESCRIPTOR LIKE '", ship_code, "%'")
  }

  query <- paste0(query, "
    GROUP BY m.DESCRIPTOR, m.START_DATE, m.END_DATE
    ORDER BY m.START_DATE DESC
  ")

  missions <- dbGetQuery(con_biochem, query)
  dbDisconnect(con_biochem)

  if (nrow(missions) == 0) {
    message("No missions found matching the specified criteria")
    return(tibble())
  }

  missions <- missions %>%
    mutate(
      START_DATE = as.Date(START_DATE),
      END_DATE = as.Date(END_DATE),
      YEAR = format(START_DATE, "%Y")
    )

  message("Found ", nrow(missions), " missions")

  return(missions)
}
