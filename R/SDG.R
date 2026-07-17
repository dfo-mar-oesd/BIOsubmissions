#' Convert data from OCADS to SDG 14.3.1 format
#'
#' @param OCADS_fn  Character. File path to OCADS-format data CSV.
#' @param expocode  Character. Expedition code for output naming. If NULL
#'                  the EXPOCODE column value is used.
#' @param out_dir   Character. Parent directory. File written to
#'                  <out_dir>/SDG/<expocode>_data_SDG.xlsx
#'
#' @return SDG-formatted data frame (invisibly).
#' @export
convert_SDG <- function(OCADS_fn,
                        expocode = NULL,
                        out_dir  = getwd()) {

  suppressPackageStartupMessages({
    require(tidyverse)
    require(openxlsx)
    require(oce)
  })

  # ---------------------------------------------------------------------------
  # 1. Read OCADS file
  #    Structure: Row 1 = column names, Row 2 = units, Row 3+ = data
  # ---------------------------------------------------------------------------
  col_names <- names(
    read_csv(OCADS_fn,
             show_col_types = FALSE,
             col_types      = cols(.default = "c"),
             n_max          = 0)
  )

  # Detect units row by checking if row 2 contains unit strings
  row2_vals <- suppressMessages(
    read_csv(OCADS_fn,
             show_col_types = FALSE,
             col_types      = cols(.default = "c"),
             col_names      = FALSE,
             skip           = 1L,
             n_max          = 1L)
  ) %>% unlist() %>% unname()

  row2_is_units <- sum(
    grepl("meters|umol|decimal|pss|dbar|uatm|mg/m|its-90|ug/|s/m|total @|degreesc",
          row2_vals, ignore.case = TRUE),
    na.rm = TRUE
  ) >= 2L

  ocads_data <- suppressMessages(
    read_csv(OCADS_fn,
             show_col_types = FALSE,
             col_types      = cols(.default = "c"),
             col_names      = col_names,
             skip           = if (row2_is_units) 2L else 1L)
  )

  # ---------------------------------------------------------------------------
  # 2. Direct column name mapping  OCADS → SDG
  #    Based on the known structure of the 74EQ OCADS export.
  #    Each entry: sdg_name = ocads_name (or NA if not a direct column map)
  # ---------------------------------------------------------------------------
  col_map <- c(
    "MOORING_NAME"   = "EXPOCODE",
    "LATITUDE"       = "BTL_LAT",
    "LONGITUDE"      = "BTL_LON",
    "DATE_UTC"       = "BTL_DATE",
    "TIME_UTC"       = "BTL_TIME",
    "DEPTH_STATION"  = "SOUNDING",
    "DEPTH_SAMPLING" = "DEPTH",
    "CTDTMP"         = "CTDTMP",
    "CTDTMP FLAG"    = "CTDTMP_FLAG_W",
    "CTDSAL"         = "CTDSAL",
    "CTDSAL FLAG"    = "CTDSAL_FLAG_W",
    "SALNTY"         = "SALNTY",
    "SALNTY FLAG"    = "SALNTY_FLAG_W",
    "ALKALI"         = "ALKALI",
    "ALKALI FLAG"    = "ALKALI_FLAG_W",
    "PH_TOT"         = "CTDPH",
    "PH_TOT FLAG"    = "CTDPH_FLAG_W",
    "TCARBN"         = "TCARBN",
    "TCARBN FLAG"    = "TCARBN_FLAG_W",
    "OXYGEN"         = "OXYGEN",
    "OXYGEN FLAG"    = "OXYGEN_FLAG_W",
    "NITRAT"         = "NO2+NO3",
    "NITRAT FLAG"    = "NO2+NO3_FLAG_W",
    "NITRIT"         = "NITRIT",
    "NITRIT FLAG"    = "NITRIT_FLAG_W",
    "PHSPHT"         = "PHSPHT",
    "PHSPHT FLAG"    = "PHSPHT_FLAG_W",
    "SILCAT"         = "SILCAT",
    "SILCAT FLAG"    = "SILCAT_FLAG_W",
    "PCO2"           = "PCO2",
    "PCO2 FLAG"      = "PCO2_FLAG_W"
  )

  # Units for SDG output (NA = blank cell in units row)
  col_units <- c(
    "MOORING_NAME"   = NA,
    "LATITUDE"       = "decimal degrees",
    "LONGITUDE"      = "decimal degrees",
    "DATE_UTC"       = NA,
    "TIME_UTC"       = NA,
    "DEPTH_STATION"  = "meters",
    "DEPTH_SAMPLING" = "meters",
    "CTDTMP"         = "decimal C",
    "CTDTMP FLAG"    = NA,
    "CTDSAL"         = NA,
    "CTDSAL FLAG"    = NA,
    "SALNTY"         = NA,
    "SALNTY FLAG"    = NA,
    "ALKALI"         = "umol/kg",
    "ALKALI FLAG"    = NA,
    "PH_TOT"         = "total @ 25 C",
    "PH_TOT FLAG"    = NA,
    "TCARBN"         = "umol/kg",
    "TCARBN FLAG"    = NA,
    "OXYGEN"         = "umol/kg",
    "OXYGEN FLAG"    = NA,
    "NITRAT"         = "umol/kg",
    "NITRAT FLAG"    = NA,
    "NITRIT"         = "umol/kg",
    "NITRIT FLAG"    = NA,
    "PHSPHT"         = "umol/kg",
    "PHSPHT FLAG"    = NA,
    "SILCAT"         = "umol/kg",
    "SILCAT FLAG"    = NA,
    "PCO2"           = "uatm",
    "PCO2 FLAG"      = NA
  )

  # Which SDG columns should be numeric
  numeric_sdg_cols <- c(
    "LATITUDE", "LONGITUDE", "DEPTH_STATION", "DEPTH_SAMPLING",
    "CTDTMP", "CTDSAL", "SALNTY", "ALKALI", "PH_TOT",
    "TCARBN", "OXYGEN", "NITRAT", "NITRIT", "PHSPHT", "SILCAT", "PCO2"
  )

  # ---------------------------------------------------------------------------
  # 3. Build SDG data frame — mapped columns first
  # ---------------------------------------------------------------------------
  sdg_dat <- tibble(.rows = nrow(ocads_data))

  for (sdg_nm in names(col_map)) {
    ocads_nm <- col_map[[sdg_nm]]
    sdg_dat[[sdg_nm]] <- if (ocads_nm %in% names(ocads_data)) {
      ocads_data[[ocads_nm]]
    } else {
      NA_character_
    }
  }

  # ---------------------------------------------------------------------------
  # 4. Validate critical columns — warn if empty after mapping
  # ---------------------------------------------------------------------------
  critical <- c("MOORING_NAME", "LATITUDE", "LONGITUDE",
                "DATE_UTC",     "TIME_UTC")

  for (col_nm in critical) {
    vals <- sdg_dat[[col_nm]]
    if (all(is.na(vals) | vals == "")) {
      ocads_src <- col_map[[col_nm]]
      warning(
        "SDG column '", col_nm, "' is entirely empty after mapping.\n",
        "  Expected OCADS source column: '", ocads_src, "'\n",
        "  Columns available in file:    ",
        paste(names(ocads_data), collapse = ", ")
      )
    }
  }

  # ---------------------------------------------------------------------------
  # 5. Carry through remaining OCADS columns not consumed by the map
  #    Skip pure metadata columns that are not needed in the SDG data file.
  # ---------------------------------------------------------------------------
  skip_cols <- c("NAME", "PLATFORM", "STNNBR", "CASTNO", "SAMPNO",
                 "DATE", "TIME", "BTL_DATE", "BTL_TIME", "SOUNDING",
                 unname(col_map))   # already mapped

  remaining <- setdiff(names(ocads_data), skip_cols)

  for (datcol in remaining) {
    is_flag <- grepl("_FLAG_W$", datcol, ignore.case = TRUE)
    sdg_nm  <- if (is_flag) {
      gsub("_FLAG_W$", " FLAG", datcol, ignore.case = TRUE)
    } else {
      datcol
    }
    if (sdg_nm %in% names(sdg_dat)) next   # already present
    sdg_dat[[sdg_nm]] <- ocads_data[[datcol]]
  }

  # ---------------------------------------------------------------------------
  # 6. Format DATE_UTC → YYYY-MM-DD (with error reporting)
  # ---------------------------------------------------------------------------
  d <- sdg_dat$DATE_UTC

  # Ensure d is character for consistent processing
  d <- if (is.numeric(d)) as.character(as.integer(d)) else as.character(d)

  # Replace "NA" strings with actual NA
  d[d == "NA" | d == "" | is.na(d)] <- NA_character_

  # Process dates with detailed error reporting
  formatted_dates <- rep(NA_character_, length(d))
  errors_date <- list()

  for (i in seq_along(d)) {
    if (is.na(d[i])) {
      next
    }

    tryCatch({
      # Try M/D/YYYY format
      if (grepl("/", d[i])) {
        formatted_dates[i] <- format(as.Date(d[i], "%m/%d/%Y"), "%Y-%m-%d")
      }
      # Try 8-digit YYYYMMDD format
      else if (nchar(d[i]) == 8 && !grepl("-", d[i])) {
        formatted_dates[i] <- format(as.Date(d[i], "%Y%m%d"), "%Y-%m-%d")
      }
      # Try 6-digit YYMMDD format
      else if (nchar(d[i]) == 6 && !grepl("-", d[i])) {
        # Assume 20YY for years
        year <- paste0("20", substr(d[i], 1, 2))
        month <- substr(d[i], 3, 4)
        day <- substr(d[i], 5, 6)
        formatted_dates[i] <- format(as.Date(paste(year, month, day, sep = "-")), "%Y-%m-%d")
      }
      # Already formatted YYYY-MM-DD
      else if (grepl("^\\d{4}-\\d{2}-\\d{2}$", d[i])) {
        formatted_dates[i] <- d[i]
      }
      # Try generic YYYY-M-D (variable padding)
      else if (grepl("^\\d{4}-\\d{1,2}-\\d{1,2}$", d[i])) {
        formatted_dates[i] <- format(as.Date(d[i]), "%Y-%m-%d")
      }
      # Try ISO8601 with time component
      else if (grepl("T", d[i])) {
        formatted_dates[i] <- format(as.Date(substr(d[i], 1, 10)), "%Y-%m-%d")
      }
      else {
        errors_date[[length(errors_date) + 1]] <- list(
          index = i,
          value = d[i],
          reason = "Unrecognized date format"
        )
      }
    }, error = function(e) {
      errors_date[[length(errors_date) + 1]] <<- list(
        index = i,
        value = d[i],
        reason = as.character(e$message)
      )
    })
  }

  # Report date errors if any
  if (length(errors_date) > 0) {
    warning(
      "\n=== DATE_UTC FORMATTING ERRORS ===\n",
      "Found ", length(errors_date), " problematic date value(s):\n",
      paste(
        sapply(errors_date, function(err) {
          sprintf("  Row %d: '%s' - %s", err$index, err$value, err$reason)
        }),
        collapse = "\n"
      ),
      "\nThese values have been set to NA."
    )
  }

  sdg_dat$DATE_UTC <- formatted_dates

  # ---------------------------------------------------------------------------
  # 7. Format TIME_UTC → HH:MM:SS (with error reporting)
  # ---------------------------------------------------------------------------
  t <- sdg_dat$TIME_UTC

  # Ensure t is character
  t <- as.character(t)

  # Replace "NA" strings with actual NA
  t[t == "NA" | t == "" | is.na(t)] <- NA_character_

  # Process times with detailed error reporting
  formatted_times <- rep(NA_character_, length(t))
  errors_time <- list()

  for (i in seq_along(t)) {
    if (is.na(t[i])) {
      next
    }

    tryCatch({
      # Already HH:MM:SS
      if (grepl("^\\d{2}:\\d{2}:\\d{2}$", t[i])) {
        formatted_times[i] <- t[i]
      }
      # HH:MM format (add :00)
      else if (grepl("^\\d{2}:\\d{2}$", t[i])) {
        formatted_times[i] <- paste0(t[i], ":00")
      }
      # H:MM format (pad hour)
      else if (grepl("^\\d{1}:\\d{2}$", t[i])) {
        formatted_times[i] <- paste0("0", t[i], ":00")
      }
      # HHMM format (4 digits, no separator)
      else if (grepl("^\\d{4}$", t[i]) && nchar(t[i]) == 4) {
        hh <- substr(t[i], 1, 2)
        mm <- substr(t[i], 3, 4)
        formatted_times[i] <- paste0(hh, ":", mm, ":00")
      }
      # HMM format (3 digits, no separator)
      else if (grepl("^\\d{3}$", t[i]) && nchar(t[i]) == 3) {
        hh <- paste0("0", substr(t[i], 1, 1))
        mm <- substr(t[i], 2, 3)
        formatted_times[i] <- paste0(hh, ":", mm, ":00")
      }
      # HHMMSS format (6 digits, no separator)
      else if (grepl("^\\d{6}$", t[i]) && nchar(t[i]) == 6) {
        hh <- substr(t[i], 1, 2)
        mm <- substr(t[i], 3, 4)
        ss <- substr(t[i], 5, 6)
        formatted_times[i] <- paste0(hh, ":", mm, ":", ss)
      }
      # Try strptime as last resort for various formats
      else {
        # Try common time formats
        for (fmt in c("%H:%M:%S", "%H:%M", "%I:%M:%S %p", "%I:%M %p")) {
          parsed <- strptime(t[i], fmt)
          if (!is.na(parsed)) {
            formatted_times[i] <- format(parsed, "%H:%M:%S")
            break
          }
        }

        # If still NA, log error
        if (is.na(formatted_times[i])) {
          errors_time[[length(errors_time) + 1]] <- list(
            index = i,
            value = t[i],
            reason = "Unrecognized time format"
          )
        }
      }
    }, error = function(e) {
      errors_time[[length(errors_time) + 1]] <<- list(
        index = i,
        value = t[i],
        reason = as.character(e$message)
      )
    })
  }

  # Report time errors if any
  if (length(errors_time) > 0) {
    warning(
      "\n=== TIME_UTC FORMATTING ERRORS ===\n",
      "Found ", length(errors_time), " problematic time value(s):\n",
      paste(
        sapply(errors_time, function(err) {
          sprintf("  Row %d: '%s' - %s", err$index, err$value, err$reason)
        }),
        collapse = "\n"
      ),
      "\nThese values have been set to NA."
    )
  }

  sdg_dat$TIME_UTC <- formatted_times
  # ---------------------------------------------------------------------------
  # 8. Derive DEPTH_SAMPLING from CTDPRS if DEPTH column missing / all NA
  # ---------------------------------------------------------------------------
  depth_all_na <- all(is.na(suppressWarnings(
    as.numeric(sdg_dat$DEPTH_SAMPLING)
  )))

  if (depth_all_na && "CTDPRS" %in% names(ocads_data)) {
    sdg_dat$DEPTH_SAMPLING <- suppressWarnings(
      as.character(round(
        oce::swDepth(
          as.numeric(ocads_data[["CTDPRS"]]),
          as.numeric(sdg_dat$LATITUDE)
        ), 2L))
    )
  }

  # ---------------------------------------------------------------------------
  # 9. Coerce numeric columns
  # ---------------------------------------------------------------------------

  # Numeric = anything in numeric_sdg_cols PLUS remaining non-flag columns
  # that ended up in sdg_dat and are not known text columns
  text_sdg_cols <- c("MOORING_NAME", "DATE_UTC", "TIME_UTC",
                     grep(" FLAG$", names(sdg_dat), value = TRUE))

  all_numeric_cols <- unique(c(
    intersect(numeric_sdg_cols, names(sdg_dat)),
    setdiff(names(sdg_dat), c(text_sdg_cols, "MOORING_NAME",
                              "DATE_UTC", "TIME_UTC"))
  ))
  # Remove any that are actually flag or text columns
  all_numeric_cols <- all_numeric_cols[
    !grepl(" FLAG$|_FLAG|MOORING|DATE|TIME|NAME|PLATFORM|STATION|CAST",
           all_numeric_cols, ignore.case = TRUE)
  ]

  for (col in all_numeric_cols) {
    sdg_dat[[col]] <- suppressWarnings(as.numeric(sdg_dat[[col]]))
  }

  # ---------------------------------------------------------------------------
  # 10. Build units row — matches col_units for mapped cols, NA for the rest
  # ---------------------------------------------------------------------------
  units_row <- tibble(
    !!!setNames(
      lapply(names(sdg_dat), function(nm) {
        u <- col_units[nm]
        if (length(u) == 0 || is.na(u)) NA_character_
        else as.character(u)
      }),
      names(sdg_dat)
    )
  )

  # ---------------------------------------------------------------------------
  # 11. Resolve EXPOCODE for file naming
  # ---------------------------------------------------------------------------
  if (is.null(expocode)) {
    expocode <- suppressWarnings(na.omit(ocads_data[["EXPOCODE"]])[1])
    if (length(expocode) == 0 || is.na(expocode)) {
      expocode <- tools::file_path_sans_ext(basename(OCADS_fn))
    }
  }

  # ---------------------------------------------------------------------------
  # 12. Write xlsx → <out_dir>/SDG/<expocode>_data_SDG.xlsx
  # ---------------------------------------------------------------------------
  sdg_dir  <- file.path(out_dir, "SDG")
  dir.create(sdg_dir, showWarnings = FALSE, recursive = TRUE)
  out_path <- file.path(sdg_dir, paste0(expocode, "_data_SDG.xlsx"))

  wb <- createWorkbook()
  addWorksheet(wb, "Data")

  writeData(wb, "Data", x = units_row,
            startRow = 1L, colNames = TRUE,  rowNames = FALSE)
  writeData(wb, "Data", x = units_row,
            startRow = 2L, colNames = FALSE, rowNames = FALSE)
  writeData(wb, "Data", x = sdg_dat,
            startRow = 3L, colNames = FALSE, rowNames = FALSE)

  data_rows <- seq(3L, 2L + nrow(sdg_dat))
  n_cols    <- ncol(sdg_dat)

  # Text format for date and time
  for (col_nm in c("DATE_UTC", "TIME_UTC")) {
    idx <- which(names(sdg_dat) == col_nm)
    if (length(idx) > 0L) {
      addStyle(wb, "Data",
               style      = createStyle(numFmt = "@"),
               rows       = data_rows,
               cols       = idx,
               gridExpand = TRUE)
    }
  }

  # Number format for numeric columns
  num_idx <- which(names(sdg_dat) %in% all_numeric_cols)
  if (length(num_idx) > 0L) {
    addStyle(wb, "Data",
             style      = createStyle(numFmt = "0.######"),
             rows       = data_rows,
             cols       = num_idx,
             gridExpand = TRUE)
  }

  # Bold header row
  addStyle(wb, "Data",
           style      = createStyle(textDecoration = "bold"),
           rows       = 1L,
           cols       = seq_len(n_cols),
           gridExpand = TRUE)

  saveWorkbook(wb, file = out_path, overwrite = TRUE)

  message("SDG data file written: ", out_path)

  invisible(sdg_dat)
}

#' Generate SDG 14.3.1 Metadata File
#'
#' Produces a submission-ready metadata .xlsx file matching the SDG 14.3.1
#' platform exactly as if filled in by hand. Accepts the same mission_info
#' list object used by generate_OCADS_metadata().
#'
#' Template structure (from 1612019_SDG14_3_1_Metadata_submission_template):
#'   Column 1: Number
#'   Column 2: Metadata element name
#'   Column 3: Your input       <- this is what we populate
#'   Column 4: Help reference number
#'
#' @param OCADS_fn     Character. File path to the OCADS-format data CSV.
#'                     Used to derive dates, coordinates, and variable list.
#' @param mission_info Named list — identical object used for generate_OCADS_metadata().
#' @param out_dir      Character. Output directory. Defaults to directory of OCADS_fn.
#'
#' @return File path to the written metadata xlsx (invisibly).
#' @export
generate_SDG_metadata <- function(OCADS_fn,
                                  mission_info,
                                  out_dir = NULL) {

  suppressPackageStartupMessages({
    require(tidyverse)
    require(openxlsx)
  })

  # ---------------------------------------------------------------------------
  # 0. Helpers
  # ---------------------------------------------------------------------------

  # Build a single metadata row matching the SDG template columns exactly.
  # Number is kept numeric so Excel stores it as general/number format,
  # which is required by the parser.
  sdg_row <- function(number, element_name, your_input, help_ref = "") {
    tibble(
      Number                  = as.numeric(number),
      `Metadata element name` = as.character(element_name),
      `Your input`            = as.character(if (is.na(your_input)) "" else your_input),
      `Help reference number` = as.character(help_ref)
    )
  }

  # ---------------------------------------------------------------------------
  # CONTROLLED VOCABULARY NORMALISERS
  #
  # All dropdown fields in the SDG template must match the allowed options
  # exactly — including case, spelling, and special characters (e.g. µ not u).
  #
  # platform_category options:
  #   Fixed Ocean Time Series | Mooring | Coastal Monitoring Site |
  #   Repeat Hydrography (vessel) | Ship-based time series (vessel) |
  #   Argo float | Glider | Voluntary Observing Ship
  #
  # depth unit:          m | cm
  # DIC/TA unit:         mol kg-1 | mmol kg-1 | µmol kg-1
  # TA measured/calc:    Measured | Calculated
  # pH scale:            total scale | seawater scale | NBS scale | free scale
  # pCO2/fCO2 unit:      µatm
  # temperature unit:    Celsius | Fahrenheit
  # salinity unit:       PSU | PPT
  # ---------------------------------------------------------------------------

  # Country name lookup — maps common shorthand to exact CV strings
  country_cv <- c(
    "United Kingdom"                   = "United Kingdom of Great Britain & Northern Ireland",
    "UK"                               = "United Kingdom of Great Britain & Northern Ireland",
    "Great Britain"                    = "United Kingdom of Great Britain & Northern Ireland",
    "United States"                    = "United States of America",
    "USA"                              = "United States of America",
    "US"                               = "United States of America",
    "South Korea"                      = "Republic of Korea",
    "Korea"                            = "Republic of Korea",
    "North Korea"                      = "Democratic People's Republic of Korea",
    "Russia"                           = "Russian Federation",
    "Iran"                             = "Iran (Islamic Republic of)",
    "Venezuela"                        = "Venezuela (Bolivarian Republic of)",
    "Viet Nam"                         = "Viet Nam",
    "Vietnam"                          = "Viet Nam",
    "Democratic Republic of the Congo" = "Congo (Democratic Republic)",
    "DRC"                              = "Congo (Democratic Republic)",
    "Republic of the Congo"            = "Congo",
    "Tanzania"                         = "United Republic of Tanzania",
    "Trinidad and Tobago"              = "Trinidad & Tobago",
    "Trinidad And Tobago"              = "Trinidad & Tobago",
    "Ivory Coast"                      = "\u00c9te d'Ivoire",
    "Cote d'Ivoire"                    = "C\u00f4te d'Ivoire",
    "Syria"                            = "Syrian Arab Republic",
    "Timor Leste"                      = "Timor-Leste"
  )

  normalise_country <- function(x) {
    if (is.null(x) || is.na(x) || x == "") return("")
    mapped <- country_cv[x]
    if (!is.na(mapped)) mapped else x
  }

  # Platform category lookup — maps common alternatives to exact CV strings
  platform_cat_cv <- c(
    "Research Vessel"            = "Repeat Hydrography (vessel)",
    "research vessel"            = "Repeat Hydrography (vessel)",
    "RV"                         = "Repeat Hydrography (vessel)",
    "Ship"                       = "Repeat Hydrography (vessel)",
    "ship"                       = "Repeat Hydrography (vessel)",
    "Vessel"                     = "Repeat Hydrography (vessel)",
    "vessel"                     = "Repeat Hydrography (vessel)",
    "Repeat Hydrography"         = "Repeat Hydrography (vessel)",
    "repeat hydrography"         = "Repeat Hydrography (vessel)",
    "Repeat Hydrography (vessel)"= "Repeat Hydrography (vessel)",
    "Time Series Vessel"         = "Ship-based time series (vessel)",
    "time series vessel"         = "Ship-based time series (vessel)",
    "Ship-based time series"     = "Ship-based time series (vessel)",
    "Ship-based time series (vessel)" = "Ship-based time series (vessel)",
    "Mooring"                    = "Mooring",
    "mooring"                    = "Mooring",
    "Fixed Time Series"          = "Fixed Ocean Time Series",
    "fixed time series"          = "Fixed Ocean Time Series",
    "Fixed Ocean Time Series"    = "Fixed Ocean Time Series",
    "Coastal"                    = "Coastal Monitoring Site",
    "coastal"                    = "Coastal Monitoring Site",
    "Coastal Monitoring Site"    = "Coastal Monitoring Site",
    "Argo"                       = "Argo float",
    "argo"                       = "Argo float",
    "Argo float"                 = "Argo float",
    "Glider"                     = "Glider",
    "glider"                     = "Glider",
    "VOS"                        = "Voluntary Observing Ship",
    "Voluntary Observing Ship"   = "Voluntary Observing Ship"
  )

  normalise_platform_cat <- function(x) {
    if (is.null(x) || is.na(x) || x == "") return("")
    mapped <- platform_cat_cv[x]
    if (!is.na(mapped)) mapped else x
  }

  # ---------------------------------------------------------------------------
  # Build a complete Var block (14 rows) for one additional variable.
  # var_num     : integer, the Var index (1, 2, 3, ...)
  # row_start   : integer, the row Number to begin at
  # abbrev      : column abbreviation in the data file
  # full_name   : human-readable variable name
  # unit        : variable unit string
  # flag_abbrev : abbreviation of the flag column
  # obs_type    : observation type string
  # collection  : collection method string
  # flag_desc   : full flag scheme description string
  # ---------------------------------------------------------------------------
  make_var_block <- function(var_num, row_start, abbrev, full_name, unit,
                             flag_abbrev = "", obs_type = "profile",
                             collection  = "Niskin bottle, 10 L",
                             flag_desc   = "") {
    prefix <- paste0("Var", var_num, ": ")
    rs     <- row_start

    bind_rows(
      sdg_row(rs + 0,  paste0(prefix, "Variable abbreviation in data files"), abbrev,      paste0("2", var_num + 6, ".1")),
      sdg_row(rs + 1,  paste0(prefix, "Full variable name"),                  full_name,   paste0("2", var_num + 6, ".2")),
      sdg_row(rs + 2,  paste0(prefix, "Observation type"),                    obs_type,    paste0("2", var_num + 6, ".3")),
      sdg_row(rs + 3,  paste0(prefix, "Variable unit"),                       unit,        paste0("2", var_num + 6, ".4")),
      sdg_row(rs + 4,  paste0(prefix, "Collection method (e.g. bottle sampling)"), collection, paste0("2", var_num + 6, ".5")),
      sdg_row(rs + 5,  paste0(prefix, "Analyzing instrument"),                "",          paste0("2", var_num + 6, ".6")),
      sdg_row(rs + 6,  paste0(prefix, "Analyzing information with citation (SOP etc)"), "", paste0("2", var_num + 6, ".7")),
      sdg_row(rs + 7,  paste0(prefix, "Quality control"),                     "",          paste0("2", var_num + 6, ".8")),
      sdg_row(rs + 8,  paste0(prefix, "Abbreviation of data quality flag scheme"), flag_abbrev, paste0("2", var_num + 6, ".9")),
      sdg_row(rs + 9,  paste0(prefix, "Data quality flag scheme"),            flag_desc,   paste0("2", var_num + 6, ".1")),
      sdg_row(rs + 10, paste0(prefix, "Uncertainty"),                         "",          paste0("2", var_num + 6, ".11")),
      sdg_row(rs + 11, paste0(prefix, "Field replicate information"),         "",          paste0("2", var_num + 6, ".12")),
      sdg_row(rs + 12, paste0(prefix, "Method reference (citation)"),         "",          paste0("2", var_num + 6, ".13")),
      sdg_row(rs + 13, paste0(prefix, "Changes to Method or SOP"),            "",          paste0("2", var_num + 6, ".14"))
    )
  }

  # ---------------------------------------------------------------------------
  # 1. Derive dynamic fields from OCADS data file
  # ---------------------------------------------------------------------------

  raw_lines <- readLines(OCADS_fn, n = 3, warn = FALSE)

  line2 <- raw_lines[2]
  has_units_row <- grepl(
    "meters|umol|decimal|PSU|degreesC|none|dbar|uatm|mg/m|pss|kg|ppt|\\bC\\b",
    line2, ignore.case = TRUE
  ) && !grepl("^\\d{4}", trimws(strsplit(line2, ",")[[1]][1]))

  col_names_raw <- strsplit(raw_lines[1], ",")[[1]]
  col_names_raw <- trimws(gsub('"', '', col_names_raw))

  dat <- read_csv(
    OCADS_fn,
    col_names      = col_names_raw,
    skip           = if (has_units_row) 2 else 1,
    show_col_types = FALSE,
    col_types      = cols(.default = "c")
  )

  resolve_col <- function(candidates, data_names) {
    data_upper <- toupper(data_names)
    cand_upper <- toupper(candidates)
    idx <- which(data_upper %in% cand_upper)
    if (length(idx) == 0) NA_character_ else data_names[idx[1]]
  }

  lat_col  <- resolve_col(c("BTL_LAT",  "LATITUDE",  "LAT"),  names(dat))
  lon_col  <- resolve_col(c("BTL_LON",  "LONGITUDE", "LON"),  names(dat))
  date_col <- resolve_col(c("DATE",     "DATE_UTC"),           names(dat))

  if (is.na(lat_col))  stop("Cannot find latitude column in OCADS file.")
  if (is.na(lon_col))  stop("Cannot find longitude column in OCADS file.")
  if (is.na(date_col)) stop("Cannot find date column in OCADS file.")

  lat  <- as.numeric(dat[[lat_col]])
  lon  <- as.numeric(dat[[lon_col]])

  # ---------------------------------------------------------------------------
  # Parse dates from OCADS file for metadata (robust version)
  # ---------------------------------------------------------------------------
  raw_dates <- dat[[date_col]]

  # Ensure character format
  raw_dates <- if (is.numeric(raw_dates)) {
    as.character(as.integer(raw_dates))
  } else {
    as.character(raw_dates)
  }

  # Parse dates with error handling
  parsed_dates <- rep(as.Date(NA), length(raw_dates))
  parse_errors <- list()

  for (i in seq_along(raw_dates)) {
    if (is.na(raw_dates[i]) || raw_dates[i] == "NA" || raw_dates[i] == "") {
      next
    }

    tryCatch({
      # Try 8-digit YYYYMMDD
      if (grepl("^\\d{8}$", raw_dates[i])) {
        parsed_dates[i] <- as.Date(raw_dates[i], "%Y%m%d")
      }
      # Try M/D/YYYY or MM/DD/YYYY
      else if (grepl("/", raw_dates[i])) {
        parsed_dates[i] <- as.Date(raw_dates[i], "%m/%d/%Y")
      }
      # Try YYYY-MM-DD
      else if (grepl("^\\d{4}-\\d{2}-\\d{2}$", raw_dates[i])) {
        parsed_dates[i] <- as.Date(raw_dates[i])
      }
      # Try YYYY-M-D (variable padding)
      else if (grepl("^\\d{4}-\\d{1,2}-\\d{1,2}$", raw_dates[i])) {
        parsed_dates[i] <- as.Date(raw_dates[i])
      }
      # Try 6-digit YYMMDD
      else if (grepl("^\\d{6}$", raw_dates[i])) {
        year <- paste0("20", substr(raw_dates[i], 1, 2))
        month <- substr(raw_dates[i], 3, 4)
        day <- substr(raw_dates[i], 5, 6)
        parsed_dates[i] <- as.Date(paste(year, month, day, sep = "-"))
      }
      else {
        parse_errors[[length(parse_errors) + 1]] <- list(
          row = i,
          value = raw_dates[i],
          reason = "Unrecognized format"
        )
      }
    }, error = function(e) {
      parse_errors[[length(parse_errors) + 1]] <<- list(
        row = i,
        value = raw_dates[i],
        reason = as.character(e$message)
      )
    })
  }

  # Report parsing errors
  if (length(parse_errors) > 0) {
    warning(
      "\n=== DATE PARSING ERRORS (for metadata date range) ===\n",
      "Found ", length(parse_errors), " unparseable date(s):\n",
      paste(
        sapply(parse_errors, function(err) {
          sprintf("  Row %d: '%s' - %s", err$row, err$value, err$reason)
        }),
        collapse = "\n"
      ),
      "\nThese rows will be excluded from date range calculation."
    )
  }

  # Calculate start and end dates (excluding NAs)
  valid_dates <- parsed_dates[!is.na(parsed_dates)]

  if (length(valid_dates) == 0) {
    stop("No valid dates found in column '", date_col, "'. Cannot determine temporal coverage.")
  }

  start_date <- format(min(valid_dates, na.rm = TRUE), "%Y-%m-%d")
  end_date   <- format(max(valid_dates, na.rm = TRUE), "%Y-%m-%d")

  north      <- round(max(lat, na.rm = TRUE), 5)
  south      <- round(min(lat, na.rm = TRUE), 5)
  east       <- round(max(lon, na.rm = TRUE), 5)
  west       <- round(min(lon, na.rm = TRUE), 5)

  var_present <- function(...) any(c(...) %in% names(dat))

  has_dic  <- var_present("TCARBN")
  has_ta   <- var_present("ALKALI")
  has_ph   <- var_present("PH_TOT", "PH")
  has_pco2 <- var_present("PCO2")
  has_temp <- var_present("CTDTMP", "TEMPERATURE")
  has_sal  <- var_present("CTDSAL", "SALNTY", "SALINITY")

  # ---------------------------------------------------------------------------
  # 1b. Build additional-variable table from all remaining columns
  # ---------------------------------------------------------------------------

  fixed_cols <- toupper(c(
    "TCARBN", "TCARBN_FLAG_W", "TCARBN_FLAG",
    "ALKALI",  "ALKALI_FLAG_W",  "ALKALI_FLAG",
    "PH_TOT",  "PH_TOT_FLAG_W",  "PH_TOT_FLAG", "PH",
    "PCO2",    "PCO2_FLAG_W",    "PCO2_FLAG",
    "CTDTMP",  "CTDTMP_FLAG_W",  "CTDTMP_FLAG",  "TEMPERATURE",
    "CTDSAL",  "CTDSAL_FLAG_W",  "CTDSAL_FLAG",
    "SALNTY",  "SALNTY_FLAG_W",  "SALNTY_FLAG",  "SALINITY",
    "DEPTH_SAMPLING", "DEPTH", "PRESSURE",
    "BTL_LAT", "BTL_LON", "LATITUDE", "LONGITUDE", "LAT", "LON",
    "DATE",    "DATE_UTC", "TIME", "TIME_UTC",
    "STATION", "CAST", "BOTTLE", "NISKIN", "EXPOCODE", "CRUISE"
  ))

  # Known variable metadata lookup
  sdg_var_lookup <- tribble(
    ~ocads,      ~full_name,                          ~unit,              ~collection,
    "CTDOXY",    "Dissolved Oxygen (sensor)",         "ml/l",             "CTD",
    "OXYGEN",    "Dissolved Oxygen (bottle)",         "ml/l",             "Niskin bottle, 10 L",
    "CTDPRS",    "Pressure (sensor)",                 "dbar",             "CTD",
    "CTDSAL",    "Salinity (sensor)",                 "PSS-78",           "CTD",
    "NITRAT",    "Nitrate",                           "\u00b5mol kg-1",   "Niskin bottle, 10 L",
    "NO2+NO3",   "Nitrate + Nitrite",                 "\u00b5mol kg-1",   "Niskin bottle, 10 L",
    "NITRIT",    "Nitrite",                           "\u00b5mol kg-1",   "Niskin bottle, 10 L",
    "PHSPHT",    "Phosphate",                         "\u00b5mol kg-1",   "Niskin bottle, 10 L",
    "SILCAT",    "Silicate",                          "\u00b5mol kg-1",   "Niskin bottle, 10 L",
    "NH3",       "Ammonium",                          "\u00b5mol kg-1",   "Niskin bottle, 10 L",
    "CH4",       "Methane",                           "nmol/kg",          "Niskin bottle, 10 L",
    "DELO18",    "Oxygen-18 isotope ratio (d18O)",    "/mille",           "Niskin bottle, 10 L",
    "CTDPH",     "pH (sensor)",                       "",                 "CTD",
    "FLUOR",     "Fluorescence",                      "mg/m3",            "CTD",
    "CTDFLU",    "Fluorescence (sensor)",             "mg/m3",            "CTD",
    "TRANSM",    "Beam Transmission",                 "%",                "CTD",
    "CHLPIG",    "Chlorophyll a + Phaeopigments",     "ug/l",             "Niskin bottle, 10 L",
    "CHLA",      "Chlorophyll a",                     "ug/l",             "Niskin bottle, 10 L"
  )

  all_cols      <- names(dat)
  flag_cols_idx <- grepl("_FLAG", toupper(all_cols))
  data_cols     <- all_cols[!flag_cols_idx]
  data_cols     <- data_cols[!toupper(data_cols) %in% fixed_cols]

  extra_vars <- tibble(ocads = data_cols) %>%
    mutate(
      ocads_upper = toupper(ocads),
      flag_abbrev = map_chr(ocads, function(col) {
        candidates <- c(
          paste0(toupper(col), "_FLAG_W"),
          paste0(toupper(col), "_FLAG")
        )
        matched <- all_cols[toupper(all_cols) %in% candidates]
        if (length(matched) == 0) "" else matched[1]
      }),
      full_name = map_chr(ocads_upper, function(v) {
        idx <- match(v, toupper(sdg_var_lookup$ocads))
        if (!is.na(idx)) sdg_var_lookup$full_name[idx] else v
      }),
      unit = map_chr(ocads_upper, function(v) {
        idx <- match(v, toupper(sdg_var_lookup$ocads))
        if (!is.na(idx)) sdg_var_lookup$unit[idx] else ""
      }),
      collection = map_chr(ocads_upper, function(v) {
        idx <- match(v, toupper(sdg_var_lookup$ocads))
        if (!is.na(idx)) sdg_var_lookup$collection[idx] else "Niskin bottle, 10 L"
      })
    ) %>%
    select(ocads, full_name, unit, collection, flag_abbrev)

  # ---------------------------------------------------------------------------
  # 2. Static method text blocks
  # ---------------------------------------------------------------------------

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

  dic_method <- paste0(
    "Seawater samples were collected from standard depths in 500 mL borosilicate glass ",
    "reagent bottles. Five mL of water was removed to allow room for thermal expansion ",
    "and the sample was preserved within 30 minutes of collection with 0.1 mL of a ",
    "mercuric chloride saturated solution, then sealed with Apiezon M grease and the ",
    "ground glass stoppers secured in place with rubber bands. The samples were stored at ",
    "room temperature until on-shore laboratory analysis. The samples were analysed for DIC ",
    "using a SOMMA sample handling system in conjunction with a coulometer (UIC Inc.) to ",
    "quantify total CO2 purged from the acidified sample. Measurements of a Certified ",
    "reference material seawater (CRMs, Scripps Oceanographic Institution) were used for ",
    "calibrating DIC with each batch of 20 samples being bracketed by duplicate CRM ",
    "measurements."
  )

  ta_method <- paste0(
    "Seawater samples were collected from standard depths in 500 mL borosilicate glass ",
    "reagent bottles. Five mL of water was removed to allow room for thermal expansion ",
    "and the sample was preserved within 30 minutes of collection with 0.1 mL of a ",
    "mercuric chloride saturated solution, then sealed with Apiezon M grease and the ",
    "ground glass stoppers secured in place with rubber bands. The samples were stored at ",
    "room temperature until on-shore laboratory analysis. TA in the sample was determined ",
    "using an open cell automated potentiometric titration with a Metrohm Titrando dosing ",
    "unit controlled by Metrohm Tiamo software. The multi-point titration, using 0.1N HCl ",
    "titrant containing 35 g of sodium chloride per litre of acid, was performed in a ",
    "temperature controlled flask held at 25 degrees C with Gran endpoint determination. ",
    "Measurements of a Certified reference material seawater (CRMs, Scripps Oceanographic ",
    "Institution) were used for calibrating TA with each batch of 20 samples being bracketed ",
    "by duplicate CRM measurements."
  )

  pco2_method <- paste0(
    "Seawater samples were collected in 160 mL volume crimp seal serum bottles, allowing ",
    "the bottle to overflow by 3 volumes. The sample was immediately stabilised by the ",
    "addition of 50 uL of saturated mercuric chloride solution, crimp sealed with a butyl ",
    "rubber septum then stored in a refrigerator. Before analysis, a 11 mL headspace of ",
    "400 ppm CO2 in zero air was introduced into the bottle which was then thermally ",
    "equilibrated at 22 degrees C for one hour. The bottle was then shaken vigorously for ",
    "8 minutes and the headspace displaced by a brine solution, flushing the sample loop ",
    "of an SRI gas chromatograph equipped with a methaniser and flame ionisation detector. ",
    "CO2 peaks were calibrated by injections of primary standard gas mixtures (Air Liquide) ",
    "having CO2 mixing ratios of 397.3, 797.2, 1201 and 2017 ppm."
  )

  co2_ref <- paste0(
    "Dickson, A.G., Sabine, C.L. and Christian, J.R. (Eds.) 2007. Guide to Best Practices ",
    "for Ocean CO2 Measurements. PICES Special Publication 3, 191 pp., ",
    "https://cdiac.ess-dive.lbl.gov/ftp/oceans/Handbook_2007/Guide_all_in_one.pdf"
  )

  # ---------------------------------------------------------------------------
  # 3. Assemble fixed metadata rows 1-226
  # ---------------------------------------------------------------------------

  inv <- mission_info$investigators
  get_inv <- function(idx, field) {
    if (idx > nrow(inv)) return("")
    val <- inv[[field]][idx]
    if (is.na(val)) "" else as.character(val)
  }

  sub <- mission_info$submitter

  fixed_rows <- bind_rows(

    # ---- Submission / accession ----
    sdg_row(1,  "Submission Date",
            format(Sys.Date(), "%m/%d/%Y"),           "1"),
    sdg_row(2,  "Accession no. of related data sets on the 14.3.1 data platform or any other data base",
            "",                                        "2"),
    sdg_row(3,  "URL of metadata set",                "",  "3"),
    sdg_row(4,  "URL of associated data set",         "",  "4"),
    sdg_row(5,  "DOI of dataset (if applicable)",     "",  "5"),

    # ---- Investigator 1 ----
    sdg_row(6,  "Investigator-1 name",                get_inv(1, "name"),        "6.1"),
    sdg_row(7,  "Investigator-1 institution",         get_inv(1, "institution"), "6.2"),
    sdg_row(8,  "Investigator-1 institution ID (OceanExpert)", "",               "6.3."),
    sdg_row(9,  "Investigator-1 address",             get_inv(1, "address"),     "6.4"),
    sdg_row(10, "Investigator-1 phone",               "",                        "6.5"),
    sdg_row(11, "Investigator-1 email",               get_inv(1, "email"),       "6.6"),
    sdg_row(12, "Investigator-1 researcher ID",       get_inv(1, "orcid"),       "6.7"),
    sdg_row(13, "Investigator-1 ID type  (OceanExpert, ORCID, ResearcherID, etc.)",
            "ORCID",                                                              "6.8"),

    # ---- Investigator 2 ----
    sdg_row(14, "Investigator-2 name",                get_inv(2, "name"),        "6.1"),
    sdg_row(15, "Investigator-2 institution",         get_inv(2, "institution"), "6.2"),
    sdg_row(16, "Investigator-2 institution ID (OceanExpert)", "",               "6.3."),
    sdg_row(17, "Investigator-2 address",             get_inv(2, "address"),     "6.4"),
    sdg_row(18, "Investigator-2 phone",               "",                        "6.5"),
    sdg_row(19, "Investigator-2 email",               get_inv(2, "email"),       "6.6"),
    sdg_row(20, "Investigator-2 researcher ID",       get_inv(2, "orcid"),       "6.7"),
    sdg_row(21, "Investigator-2 ID type  (OceanExpert, ORCID, ResearcherID, etc.)",
            "ORCID",                                                              "6.8"),

    # ---- Investigator 3 ----
    sdg_row(22, "Investigator-3 name",                get_inv(3, "name"),        "6.1"),
    sdg_row(23, "Investigator-3 institution",         get_inv(3, "institution"), "6.2"),
    sdg_row(24, "Investigator-2 institution ID (OceanExpert)", "",               "6.3."),
    sdg_row(25, "Investigator-3 address",             get_inv(3, "address"),     "6.4"),
    sdg_row(26, "Investigator-3 phone",               "",                        "6.5"),
    sdg_row(27, "Investigator-3 email",               get_inv(3, "email"),       "6.6"),
    sdg_row(28, "Investigator-3 researcher ID",       get_inv(3, "orcid"),       "6.7"),
    sdg_row(29, "Investigator-3 ID type  (OceanExpert, ORCID, ResearcherID, etc.)",
            if (nrow(inv) >= 3 && !is.na(inv$orcid[3]) && inv$orcid[3] != "") "ORCID" else "",
            "6.8"),

    # ---- Data submitter ----
    sdg_row(30, "Data submitter name",        sub$name,        "7.1"),
    sdg_row(31, "Data submitter institution", sub$institution, "7.2"),
    sdg_row(32, "Data submitter - institution ID (OceanExpert)", "", "7.3"),
    sdg_row(33, "Data submitter address",     sub$address,     "7.4"),
    sdg_row(34, "Data submitter phone",       "",              "7.5"),
    sdg_row(35, "Data submitter email",       sub$email,       "7.6"),
    sdg_row(36, "Data submitter researcher ID", sub$orcid,     "7.7"),
    sdg_row(37, "Data submitter ID type  (OceanExpert, ORCID, ResearcherID, etc.)",
            "ORCID",                                           "7.8"),

    # ---- Dataset description ----
    sdg_row(38, "Name of sampling site or title of related research project",
            mission_info$research_project,                     "8"),
    sdg_row(39, "Short description including purpose of observation",
            paste0("Oceanographic sampling of physical, biological and chemical parameters ",
                   "is performed biweekly at selected fixed stations and two to three times ",
                   "annually along selected fixed sections, as part of the Atlantic Zone ",
                   "Monitoring Program. The sampling consists at minimum of vertical profile ",
                   "of the entire water column, water bottle sampling at selected depths of ",
                   "nutrients, salinity and oxygen, vertical net tows for zooplankton and ",
                   "Secchi depth measurement."),
            "9"),
    sdg_row(40, "Method(s) applied",
            "Discrete water column sampling (CTD/Niskin rosette)",
            "10"),

    # ---- Temporal coverage ----
    sdg_row(41, "First day of measurement included in data file (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)",
            start_date, "11.1"),
    sdg_row(42, "Last day of measurement included in data file (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)",
            end_date,   "11.2"),

    # ---- Spatial coverage ----
    sdg_row(43, "Site specific measurement longitude",             "",    "12.1"),
    sdg_row(44, "Site specific measurement latitude",              "",    "12.2"),
    sdg_row(45, "Transect measurement longitude easternmost", east,       "12.3"),
    sdg_row(46, "Transect measurement longitude westernmost", west,       "12.4"),
    sdg_row(47, "Transect measurement latitude northernmost", north,      "12.5"),
    sdg_row(48, "Transect measurement latitude southernmost", south,      "12.6"),

    # ---- Funding ----
    sdg_row(49, "Funding agency name",         mission_info$funding_agency,        "13.1"),
    sdg_row(50, "Funding project title",       mission_info$funding_project_title, "13.2"),
    sdg_row(51, "Funding project ID (Grant number)",
            mission_info$funding_project_id,                                       "13.3"),

    # ---- Platform ----
    # platform_category and platform_country use controlled vocabularies —
    # values are normalised through lookup tables before writing.
    sdg_row(52, "Platform name",
            mission_info$platform_name,                                       "14.1"),
    sdg_row(53, "Platform category",
            normalise_platform_cat(mission_info$platform_type),               "14.2"),
    sdg_row(54, "Platform ID",
            mission_info$platform_id,                                         "14.3"),
    sdg_row(55, "Platform ID type",   "ICES",                                 "14.4"),
    sdg_row(56, "Platform-1 owner",   mission_info$platform_owner,            "14.5"),
    sdg_row(57, "Platform-1 country",
            normalise_country(mission_info$platform_country),                 "14.6"),

    # ---- Cruise identifiers ----
    sdg_row(58, "EXPOCODE",       mission_info$expocode,  "15.1"),
    sdg_row(59, "Cruise ID",      mission_info$cruise_id, "15.2"),
    sdg_row(60, "Cruise ID type", "ICES",                 "15.3"),

    # ---- Citation / references ----
    sdg_row(61, "Author list for citation", mission_info$author_list, "16"),
    sdg_row(62, "References",
            "https://www.dfo-mpo.gc.ca/science/data-donnees/azmp-pmza/index-eng.html#data",
            "17"),
    sdg_row(63, "Supplemental information",
            paste0("Data extracted from BioChem, the Fisheries and Oceans Canada database ",
                   "for biological and chemical data (Devine, L., M.K. Kennedy, I. St-Pierre, ",
                   "C. Lafleur, M. Ouellet, and data. Bond. 2014. BioChem: the Fisheries and ",
                   "Oceans Canada database for biological and chemical data. Can. Tech. Rep. ",
                   "Fish. Aquat. Sci. 3073: iv + 40 pp., ",
                   "http://science-catalogue.canada.ca/record=b4008162~S6)"),
            "18"),

    # ---- Depth ----
    # depth unit CV: "m" or "cm" only
    sdg_row(64, "Depth: Variable abbreviation in data files", "DEPTH_SAMPLING", "19.1"),
    sdg_row(65, "Depth: Variable unit",                       "m",              "19.2"),

    # ---- DIC ----
    # unit CV: mol kg-1 | mmol kg-1 | µmol kg-1
    sdg_row(66, "DIC: Variable abbreviation in data files",
            if (has_dic) "TCARBN" else "",                          "20.1"),
    sdg_row(67, "DIC: Observation type",
            if (has_dic) "profile" else "",                         "20.2"),
    sdg_row(68, "DIC: Variable unit",
            if (has_dic) "\u00b5mol kg-1" else "",                  "20.3"),
    sdg_row(69, "DIC: Collection method (e.g. bottle sampling)",
            if (has_dic) "Niskin bottle, 10 L" else "",             "20.4"),
    sdg_row(70, "DIC: Analyzing instrument",
            if (has_dic) "SOMMA with UIC 5011 coulometer" else "",  "20.5"),
    sdg_row(71, "DIC: Analyzing information with citation",
            if (has_dic) dic_method else "",                        "20.6"),
    sdg_row(72, "DIC: Quality control",
            if (has_dic) "Duplicate CRM measurements bracketing each batch of 20 samples" else "",
            "20.7"),
    sdg_row(73, "DIC: Abbreviation of data quality flag scheme",
            if (has_dic) "TCARBN_FLAG_W" else "",                   "20.8"),
    sdg_row(74, "DIC: Data quality scheme (name of scheme)",
            if (has_dic) qc_flag_desc else "",                      "20.9"),
    sdg_row(75, "DIC: Uncertainty",
            if (has_dic) "0.0015" else "",                          "20.1"),
    sdg_row(76, "DIC: Field replicate information",                  "", "20.11"),
    sdg_row(77, "DIC: Calibration method",
            if (has_dic) "Referenced to duplicate measurement of CRM prior to and following each daily batch" else "",
            "20.12"),
    sdg_row(78, "DIC: Frequency of calibration",
            if (has_dic) "To bracket each batch of 20 samples" else "", "20.13"),
    sdg_row(79, "DIC: CRM manufacturer",
            if (has_dic) "Andrew Dickson, Scripps Oceanographic Institute" else "", "20.14"),
    sdg_row(80, "DIC: Batch number(s)",
            if (has_dic) mission_info$dic_crm_batch else "",        "20.15"),
    sdg_row(81, "DIC: Poison used to kill the sample",
            if (has_dic) "Mercuric chloride saturated solution" else "", "20.16"),
    sdg_row(82, "DIC: Poison volume",
            if (has_dic) "100 uL per 500 ml sample bottle = 0.02%" else "", "20.17"),
    sdg_row(83, "DIC: Poisoning correction description",             "", "20.18"),
    sdg_row(84, "DIC: Method reference (citation)",
            if (has_dic) co2_ref else "",                           "20.19"),
    sdg_row(85, "DIC: Changes to Method or SOP",                    "", "20.2"),

    # ---- TA ----
    # unit CV: mol kg-1 | mmol kg-1 | µmol kg-1
    # measured/calculated CV: Measured | Calculated  (capital first letter)
    sdg_row(86,  "TA: Variable abbreviation in data files",
            if (has_ta) "ALKALI" else "",                           "21.1"),
    sdg_row(87,  "TA: Observation type",
            if (has_ta) "profile" else "",                          "21.2"),
    sdg_row(88,  "TA: Variable unit",
            if (has_ta) "\u00b5mol kg-1" else "",                   "21.3"),
    sdg_row(89,  "TA: Collection method (e.g. bottle sampling)",
            if (has_ta) "Niskin bottle, 10 L" else "",              "21.4"),
    sdg_row(90,  "TA: Measured or calculated",
            if (has_ta) "Measured" else "",                         "21.5"),
    sdg_row(91,  "TA: Calculation method and parameters",           "", "21.6"),
    sdg_row(92,  "TA: Analyzing instrument",
            if (has_ta) "In-house automated sampler with Metrohm Titrando dosimat and Tiamo software" else "",
            "21.7"),
    sdg_row(93,  "TA: Analyzing information with citation (SOP etc)",
            if (has_ta) ta_method else "",                          "21.8"),
    sdg_row(94,  "TA: Quality control",
            if (has_ta) "Duplicate CRM measurements bracketing each batch of 20 samples" else "",
            "21.9"),
    sdg_row(95,  "TA: Abbreviation of data quality flag scheme",
            if (has_ta) "ALKALI_FLAG_W" else "",                    "21.1"),
    sdg_row(96,  "TA: Data quality flag scheme",
            if (has_ta) qc_flag_desc else "",                       "21.11"),
    sdg_row(97,  "TA: Uncertainty",
            if (has_ta) "0.0025" else "",                           "21.12"),
    sdg_row(98,  "TA: Type of titration",
            if (has_ta) "Potentiometric multi point titration" else "", "21.13"),
    sdg_row(99,  "TA: Cell type (open or closed)",
            if (has_ta) "Open" else "",                             "21.14"),
    sdg_row(100, "TA: Curve fitting method",
            if (has_ta) "Gran point" else "",                       "21.15"),
    sdg_row(101, "TA: Field replicate information",                  "", "21.16"),
    sdg_row(102, "TA: Calibration method",
            if (has_ta) "Referenced to duplicate measurements of a CRM before and after each daily batch" else "",
            "21.17"),
    sdg_row(103, "TA: Frequency of calibration",
            if (has_ta) "To bracket each batch of 20 samples" else "", "21.18"),
    sdg_row(104, "TA: CRM manufacturer",
            if (has_ta) "Andrew Dickson, Scripps Oceanographic Institute" else "", "21.19"),
    sdg_row(105, "TA: Batch Number(s)",
            if (has_ta) mission_info$ta_crm_batch else "",          "21.2"),
    sdg_row(106, "TA: Poison used to kill the sample",
            if (has_ta) "Mercuric chloride saturated solution" else "", "21.21"),
    sdg_row(107, "TA: Poison volume",
            if (has_ta) "100 uL per 500 ml sample bottle = 0.02%" else "", "21.22"),
    sdg_row(108, "TA: Poisoning correction description",             "", "21.23"),
    sdg_row(109, "TA: Magnitude of blank correction",               "", "21.24"),
    sdg_row(110, "TA: Method reference (citation)",
            if (has_ta) co2_ref else "",                            "21.25"),
    sdg_row(111, "TA: Changes to Method or SOP",                    "", "21.26"),

    # ---- pH ----
    # pH scale CV: total scale | seawater scale | NBS scale | free scale
    sdg_row(112, "pH: Variable abbreviation in data files",
            if (has_ph) "PH_TOT" else "",                           "22.1"),
    sdg_row(113, "pH: Observation type",
            if (has_ph) "profile" else "",                          "22.2"),
    sdg_row(114, "pH: Collection method (e.g. bottle sampling)",
            if (has_ph) "Niskin bottle, 10 L" else "",              "22.3"),
    sdg_row(115, "pH: Analyzing instrument",                        "", "22.4"),
    sdg_row(116, "pH: Analyzing information with citation (SOP etc)", "", "22.5"),
    sdg_row(117, "pH: pH scale",
            if (has_ph) "total scale" else "",                      "22.6"),
    sdg_row(118, "pH: Quality control",                             "", "22.7"),
    sdg_row(119, "pH: Abbreviation of data quality flag scheme",
            if (has_ph) "PH_TOT_FLAG_W" else "",                    "22.8"),
    sdg_row(120, "pH: Data quality flag scheme",
            if (has_ph) qc_flag_desc else "",                       "22.9"),
    sdg_row(121, "pH: Uncertainty",                                 "", "22.1"),
    sdg_row(122, "pH: Temperature of measurement",                  "", "22.11"),
    sdg_row(123, "pH: Field replicate information",                 "", "22.12"),
    sdg_row(124, "pH: Calibration method",                         "", "22.13"),
    sdg_row(125, "pH: Frequency of calibration",                   "", "22.14"),
    sdg_row(126, "pH:Type of dye and manufacturer information",    "", "22.15"),
    sdg_row(127, "pH: pH values of the standards",                 "", "22.16"),
    sdg_row(128, "pH: Temperature of calibration",                 "", "22.17"),
    sdg_row(129, "pH: Temperature correction method",              "", "22.18"),
    sdg_row(130, "pH: At what temperature was pH reported",        "", "22.19"),
    sdg_row(131, "pH: Method reference (citation)",                "", "22.2"),
    sdg_row(132, "pH: Changes to Method or SOP",                   "", "22.21"),

    # ---- pCO2 ----
    # unit CV: µatm  (\u00b5atm)
    sdg_row(133, "pCO2: Variable abbreviation in data files",
            if (has_pco2) "PCO2" else "",                           "23.1"),
    sdg_row(134, "pCO2: Observation type",
            if (has_pco2) "profile" else "",                        "23.2"),
    sdg_row(135, "pCO2: Variable unit",
            if (has_pco2) "\u00b5atm" else "",                      "23.3"),
    sdg_row(136, "pCO2: Collection method (e.g. bottle sampling)",
            if (has_pco2) "Niskin bottle, 10 L" else "",            "23.4"),
    sdg_row(137, "pCO2: Location of seawater intake",              "", "23.5"),
    sdg_row(138, "pCO2: Depth of seawater intake",                 "", "23.6"),
    sdg_row(139, "pCO2: Analyzing instrument",
            if (has_pco2) "SRI 8610C gas chromatograph with flame ionization detector and methanizer" else "",
            "23.7"),
    sdg_row(140, "pCO2: Analyzing information with citation (SOP etc)",
            if (has_pco2) pco2_method else "",                      "23.8"),
    sdg_row(141, "pCO2: Quality control",                          "", "23.9"),
    sdg_row(142, "pCO2: Abbreviation of data quality flag scheme",
            if (has_pco2) "PCO2_FLAG_W" else "",                    "23.1"),
    sdg_row(143, "pCO2: Data quality flag scheme",
            if (has_pco2) qc_flag_desc else "",                     "23.11"),
    sdg_row(144, "pCO2: Uncertainty",
            if (has_pco2) "0.0025" else "",                         "23.12"),
    sdg_row(145, "pCO2: Equilbrator type",                         "", "23.13"),
    sdg_row(146, "pCO2: Equilibrator volume (L)",                  "", "23.14"),
    sdg_row(147, "pCO2: Equilibrator vented or not",               "", "23.15"),
    sdg_row(148, "pCO2: Equilibrator water flow rate (L min-1)",   "", "23.16"),
    sdg_row(149, "pCO2: Equilibrator headspace gas flow rate (L min-1)", "", "23.17"),
    sdg_row(150, "pCO2: How was temperature inside the equilibrator measured", "", "23.18"),
    sdg_row(151, "pCO2: How was pressure inside the equilibrator measured",    "", "23.19"),
    sdg_row(152, "pCO2: Drying method for CO2 gas",                "", "23.2"),
    sdg_row(153, "pCO2: Manufacturer of the gas detector",
            if (has_pco2) "SRI instruments" else "",                "23.21"),
    sdg_row(154, "pCO2: Model of the gas detector",
            if (has_pco2) "8610 GC-FID with methanizer" else "",    "23.22"),
    sdg_row(155, "pCO2: Resolution of the gas detector",
            if (has_pco2) "0.1 uatm" else "",                       "23.23"),
    sdg_row(156, "pCO2: Uncertainty of the gas detector",
            if (has_pco2) "1 uatm" else "",                         "23.24"),
    sdg_row(157, "pCO2: Calibration method",
            if (has_pco2) "Calibration plot from injections of primary gas standards, 400 ppm, 800 ppm, 1200 ppm" else "",
            "23.25"),
    sdg_row(158, "pCO2: Frequency of calibration",
            if (has_pco2) "Twice daily, before and after each batch of samples" else "", "23.26"),
    sdg_row(159, "pCO2: Manufacturer of standard gas",
            if (has_pco2) "Air Liquide" else "",                    "23.27"),
    sdg_row(160, "pCO2: Concentrations of standard gas",
            if (has_pco2) "397.3 ppm, 797.2 ppm, 1201 ppm" else "", "23.28"),
    sdg_row(161, "pCO2: Uncertainties of standard gas",
            if (has_pco2) "0.01" else "",                           "23.29"),
    sdg_row(162, "pCO2: Water vapor correction method",
            if (has_pco2) "Dickson et al., 2007, Guide to best practices for ocean CO2 measurements, SOP 4, section 8.3" else "",
            "23.3"),
    sdg_row(163, "pCO2: Temperature correction method",
            if (has_pco2) "None" else "",                           "23.31"),
    sdg_row(164, "pCO2: At what temperature was pCO2 reported",
            if (has_pco2) "22 degrees Celsius" else "",             "23.32"),
    sdg_row(165, "pCO2: Method reference (citation)",
            if (has_pco2) co2_ref else "",                          "23.33"),
    sdg_row(166, "pCO2: Changes to Method or SOP",                 "", "23.34"),

    # ---- fCO2 (blank — not measured) ----
    sdg_row(167, "fCO2: Variable abbreviation in data files",       "", "24.1"),
    sdg_row(168, "fCO2: Observation type",                          "", "24.2"),
    sdg_row(169, "fCO2: Variable unit",                             "", "24.3"),
    sdg_row(170, "fCO2: Collection method (e.g. bottle sampling)",  "", "24.4"),
    sdg_row(171, "fCO2: Location of seawater intake",               "", "24.5"),
    sdg_row(172, "fCO2: Depth of seawater intake",                  "", "24.6"),
    sdg_row(173, "fCO2: Analyzing instrument",                      "", "24.7"),
    sdg_row(174, "fCO2: Analyzing information with citation (SOP etc)", "", "24.8"),
    sdg_row(175, "fCO2: Quality control",                           "", "24.9"),
    sdg_row(176, "fCO2: Abbreviation of data quality flag scheme",  "", "24.1"),
    sdg_row(177, "fCO2: Data quality flag scheme",                  "", "24.11"),
    sdg_row(178, "fCO2: Uncertainty",                               "", "24.12"),
    sdg_row(179, "fCO2: Equilbrator type",                          "", "24.13"),
    sdg_row(180, "fCO2: Equilibrator volume (L)",                   "", "24.14"),
    sdg_row(181, "fCO2: Equilibrator vented or not",                "", "24.15"),
    sdg_row(182, "fCO2: Equilibrator water flow rate (L min-1)",    "", "24.16"),
    sdg_row(183, "fCO2: Equilibrator headspace gas flow rate (L min-1)", "", "24.17"),
    sdg_row(184, "fCO2: How was temperature inside the equilibrator measured", "", "24.18"),
    sdg_row(185, "fCO2: How was pressure inside the equilibrator measured",    "", "24.19"),
    sdg_row(186, "fCO2: Drying method for CO2 gas",                 "", "24.2"),
    sdg_row(187, "fCO2: Manufacturer of the gas detector",          "", "24.21"),
    sdg_row(188, "fCO2: Model of the gas detector",                 "", "24.22"),
    sdg_row(189, "fCO2: Resolution of the gas detector",            "", "24.23"),
    sdg_row(190, "fCO2: Uncertainty of the gas detector",           "", "24.24"),
    sdg_row(191, "fCO2: Calibration method",                        "", "23.25"),
    sdg_row(192, "fCO2: Frequency of calibration",                  "", "23.26"),
    sdg_row(193, "fCO2: Manufacturer of standard gas",              "", "23.27"),
    sdg_row(194, "fCO2: Concentrations of standard gas",            "", "23.28"),
    sdg_row(195, "fCO2: Uncertainties of standard gas",             "", "23.29"),
    sdg_row(196, "fCO2: Water vapor correction method",             "", "23.3"),
    sdg_row(197, "fCO2: Temperature correction method",             "", "23.31"),
    sdg_row(198, "fCO2: At what temperature was fCO2 reported",     "", "23.32"),
    sdg_row(199, "fCO2: Method reference (citation)",               "", "23.33"),
    sdg_row(200, "fCO2: Changes to Method or SOP",                  "", "23.34"),

    # ---- Temperature ----
    # unit CV: Celsius | Fahrenheit
    sdg_row(201, "Temperature: Variable abbreviation in data files",
            if (has_temp) "CTDTMP" else "",                         "25.1"),
    sdg_row(202, "Temperature: Observation type",
            if (has_temp) "profile" else "",                        "25.2"),
    sdg_row(203, "Temperature: Variable unit",
            if (has_temp) "Celsius" else "",                        "25.3"),
    sdg_row(204, "Temperature: Collection method (e.g. bottle sampling)",
            if (has_temp) "CTD" else "",                            "25.4"),
    sdg_row(205, "Temperature: Analyzing instrument",
            if (has_temp) "Sea-Bird CTD" else "",                   "25.5"),
    sdg_row(206, "Temperature: Analyzing information with citation (SOP etc)",
            if (has_temp) "Temperature measured at depth using calibrated sensor as part of CTD package" else "",
            "25.6"),
    sdg_row(207, "Temperature: Quality control",                    "", "25.7"),
    sdg_row(208, "Temperature: Abbreviation of data quality flag scheme",
            if (has_temp) "CTDTMP_FLAG_W" else "",                  "25.8"),
    sdg_row(209, "Temperature: Data quality flag scheme",
            if (has_temp) qc_flag_desc else "",                     "25.9"),
    sdg_row(210, "Temperature: Uncertainty",                        "", "25.1"),
    sdg_row(211, "Temperature: Field replicate information",        "", "25.11"),
    sdg_row(212, "Temperature: Method reference (citation)",        "", "25.12"),
    sdg_row(213, "Temperature: Changes to Method or SOP",           "", "25.13"),

    # ---- Salinity ----
    # unit CV: PSU | PPT
    sdg_row(214, "Salinity: Variable abbreviation in data files",
            if (has_sal) "CTDSAL" else "",                          "26.1"),
    sdg_row(215, "Salinity: Observation type",
            if (has_sal) "profile" else "",                         "26.2"),
    sdg_row(216, "Salinity: Variable unit",
            if (has_sal) "PSU" else "",                             "26.3"),
    sdg_row(217, "Salinity: Collection method (e.g. bottle sampling)",
            if (has_sal) "CTD" else "",                             "26.4"),
    sdg_row(218, "Salinity: Analyzing instrument",
            if (has_sal) "Sea-Bird CTD" else "",                    "26.5"),
    sdg_row(219, "Salinity: Analyzing information with citation (SOP etc)", "", "26.6"),
    sdg_row(220, "Salinity: Quality control",                       "", "26.7"),
    sdg_row(221, "Salinity: Abbreviation of data quality flag scheme",
            if (has_sal) "CTDSAL_FLAG_W" else "",                   "26.8"),
    sdg_row(222, "Salinity: Data quality flag scheme",
            if (has_sal) qc_flag_desc else "",                      "26.9"),
    sdg_row(223, "Salinity: Uncertainty",                           "", "26.1"),
    sdg_row(224, "Salinity: Field replicate information",           "", "26.11"),
    sdg_row(225, "Salinity: Method reference (citation)",           "", "26.12"),
    sdg_row(226, "Salinity: Changes to Method or SOP",              "", "26.13")

  ) # end fixed_rows

  # ---------------------------------------------------------------------------
  # 4. Dynamically build Var blocks for all additional variables.
  #    Row numbering continues from 227, each block consuming 14 rows.
  # ---------------------------------------------------------------------------
  var_rows <- vector("list", nrow(extra_vars))

  for (i in seq_len(nrow(extra_vars))) {
    row_start <- 227 + (i - 1) * 14
    v         <- extra_vars[i, ]

    var_rows[[i]] <- make_var_block(
      var_num     = i,
      row_start   = row_start,
      abbrev      = v$ocads,
      full_name   = v$full_name,
      unit        = v$unit,
      flag_abbrev = v$flag_abbrev,
      obs_type    = "profile",
      collection  = v$collection,
      flag_desc   = if (v$flag_abbrev != "") qc_flag_desc else ""
    )
  }

  var_rows_df <- if (length(var_rows) > 0) bind_rows(var_rows) else tibble(
    Number                  = numeric(0),
    `Metadata element name` = character(0),
    `Your input`            = character(0),
    `Help reference number` = character(0)
  )

  rows <- bind_rows(fixed_rows, var_rows_df)

  total_rows <- nrow(rows)
  message("SDG metadata: ", total_rows, " rows (",
          nrow(extra_vars), " additional variable blocks appended after row 226)")

  if (total_rows < 240) {
    warning(
      "SDG metadata has only ", total_rows, " rows. The parser requires at least 240. ",
      "Check that extra_vars is populated correctly."
    )
  }

  # ---------------------------------------------------------------------------
  # 5. Write xlsx
  # ---------------------------------------------------------------------------
  if (is.null(out_dir)) out_dir <- dirname(OCADS_fn)
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  out_path <- file.path(
    out_dir,
    paste0(mission_info$expocode, "_SDG_metadata.xlsx")
  )

  wb <- createWorkbook()
  addWorksheet(wb, "Metadata")

  writeData(wb, "Metadata",
            x        = rows,
            startRow = 1,
            colNames = TRUE,
            rowNames = FALSE)

  n_rows_out <- nrow(rows)

  # ---- Column widths ----
  setColWidths(wb, "Metadata", cols = 1, widths = 8)
  setColWidths(wb, "Metadata", cols = 2, widths = 65)
  setColWidths(wb, "Metadata", cols = 3, widths = 120)
  setColWidths(wb, "Metadata", cols = 4, widths = 22)

  # ---- Header style ----
  header_style <- createStyle(
    textDecoration = "bold",
    fgFill         = "#DCE6F1",
    border         = "Bottom",
    borderColour   = "#4472C4",
    wrapText       = FALSE
  )
  addStyle(wb, "Metadata", header_style,
           rows = 1, cols = 1:4, gridExpand = TRUE)

  # ---- Input column style ----
  input_style <- createStyle(
    fgFill   = "#FFFFE0",
    wrapText = TRUE,
    valign   = "top"
  )
  addStyle(wb, "Metadata", input_style,
           rows       = 2:(n_rows_out + 1),
           cols       = 3,
           gridExpand = TRUE)

  # ---- Label columns style ----
  label_style <- createStyle(wrapText = FALSE, valign = "top")
  addStyle(wb, "Metadata", label_style,
           rows       = 2:(n_rows_out + 1),
           cols       = c(1, 2, 4),
           gridExpand = TRUE)

  # ---- Alternating shading ----
  for (r in seq(2, n_rows_out + 1, by = 2)) {
    addStyle(wb, "Metadata",
             style      = createStyle(fgFill   = "#F2F2F2",
                                      wrapText = FALSE,
                                      valign   = "top"),
             rows       = r,
             cols       = c(1, 2, 4),
             gridExpand = TRUE)
  }

  # ---- Freeze header ----
  freezePane(wb, "Metadata", firstRow = TRUE)

  saveWorkbook(wb, file = out_path, overwrite = TRUE)
  message("SDG metadata file written: ", out_path)

  invisible(out_path)
}
