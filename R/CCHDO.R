# functions to format OCADS output into real CCHDO WHP-exchange bottle files
#
# Format reference: https://exchange-format.readthedocs.io/ (source: https://github.com/cchdo/exchange)
# Bottle files ("BOTTLE Exchange") are comma-delimited text with:
#   1. a file-format-indicator/creation-stamp line, e.g. "BOTTLE,20260717DFOBIOEOG"
#   2. optional "#"-prefixed comment lines
#   3. one parameter-name header line
#   4. one units line (units row, blank where a parameter has no unit)
#   5. one data line per bottle closure, missing values as -999 / flag 9
#   6. a literal "END_DATA" terminator line
# This is NOT the same as an ordinary CSV with a header row - convert_CCHDO() only
# reshapes the data.frame; write_CCHDO_exchange() writes the actual file structure above,
# and is the only supported way to produce a submittable file.

#' Great-circle distance between two points (haversine, km)
#' @keywords internal
.haversine_km <- function(lat1, lon1, lat2, lon2, R = 6371) {
  p1 <- lat1 * pi / 180; p2 <- lat2 * pi / 180
  dphi <- (lat2 - lat1) * pi / 180
  dlambda <- (lon2 - lon1) * pi / 180
  a <- sin(dphi / 2)^2 + cos(p1) * cos(p2) * sin(dlambda / 2)^2
  2 * R * asin(pmin(1, sqrt(a)))
}

#' Initial bearing from point 1 to point 2, radians
#' @keywords internal
.bearing_rad <- function(lat1, lon1, lat2, lon2) {
  p1 <- lat1 * pi / 180; p2 <- lat2 * pi / 180
  dlambda <- (lon2 - lon1) * pi / 180
  y <- sin(dlambda) * cos(p2)
  x <- cos(p1) * sin(p2) - sin(p1) * cos(p2) * cos(dlambda)
  atan2(y, x)
}

#' Is a point within `corridor_km` of the great-circle segment from (lat1,lon1) to
#' (lat2,lon2), allowing `corridor_km` of along-track overshoot past either endpoint?
#' Vectorised over lat_p/lon_p. See
#' https://www.movable-type.co.uk/scripts/latlong.html#cross-track for the formulas.
#' @keywords internal
.on_line_corridor <- function(lat_p, lon_p, lat1, lon1, lat2, lon2,
                               corridor_km, R = 6371) {
  d13    <- .haversine_km(lat1, lon1, lat_p, lon_p, R = 1) # angular distance, radians
  theta13 <- .bearing_rad(lat1, lon1, lat_p, lon_p)
  theta12 <- .bearing_rad(lat1, lon1, lat2, lon2)

  cross_track <- asin(sin(d13) * sin(theta13 - theta12)) * R
  along_track <- acos(pmin(1, pmax(-1, cos(d13) / cos(cross_track / R)))) * R

  line_length <- .haversine_km(lat1, lon1, lat2, lon2, R = R)

  abs(cross_track) <= corridor_km &
    along_track >= -corridor_km &
    along_track <= line_length + corridor_km
}

#' Convert OCADS format to CCHDO/WHP-exchange bottle format
#'
#' Reshapes the output of \code{convert_OCADS()} to match the WHP-exchange bottle
#' parameter set and column order (see
#' \url{https://exchange-format.readthedocs.io/en/latest/bottle.html}). Pair with
#' \code{write_CCHDO_exchange()} to write a real \code{<EXPOCODE>_hy1.csv} file - do not
#' \code{write_csv()} the result directly, it is missing the stamp/units/END_DATA structure
#' CCHDO requires.
#'
#' @param ocads_data A dataframe in OCADS format, i.e. the output of \code{convert_OCADS()}.
#'   Row 1 must be the units row that \code{convert_OCADS()} produces. Must contain columns:
#'   EXPOCODE, STNNBR, CASTNO, SAMPNO, DATE, TIME, LATITUDE, LONGITUDE, SOUNDING.
#' @param sect_id chr, WHP section identifier assigned only to stations that fall within
#'   \code{corridor_km} of the great-circle line between \code{line_start} and
#'   \code{line_end} (default the AR07W Labrador Sea line - see \code{@details}). Stations
#'   outside the corridor are left with a blank SECT_ID rather than guessed at: AZOMP also
#'   occupies opportunistic/non-line stations (Scotian Slope/Rise, Bedford Basin, etc.) that
#'   have no formal section identifier.
#' @param line_start,line_end numeric \code{c(lat, lon)} pairs (decimal degrees) giving the
#'   two endpoints of the line to check against. Default is the AZOMP AR07W line: station 1
#'   at 53 40.76N 55 32.95W and station 17 at 57 49.81N 51 21.18W.
#' @param corridor_km numeric, how far (km) a station may sit from the line - and how far it
#'   may overshoot past either endpoint - and still count as on \code{sect_id} (default 50).
#'
#' @return A dataframe formatted for CCHDO/WHP-exchange, with row 1 still the units row.
#'
#' @details
#' Beyond OCADS -> CCHDO nomenclature differences (NH3 -> NH4, alphanumeric-only station
#' names), this reshapes two things that are easy to get wrong:
#' \itemize{
#'   \item \strong{DEPTH}: OCADS's \code{DEPTH} is the individual bottle collection depth
#'     (already consumed upstream by \code{convert_OCADS()} to derive \code{CTDPRS} when
#'     needed) - it is NOT the WHP-exchange \code{DEPTH} parameter, which is defined as
#'     "the reported depth to the bottom ... NOT the depth of bottle closures". OCADS's
#'     \code{SOUNDING} (bottom depth, pulled from BioChem) is what WHP calls \code{DEPTH},
#'     so that's what this function renames and keeps; the OCADS \code{DEPTH} column is
#'     dropped.
#'   \item \strong{SECT_ID}: not present in OCADS output at all, and NOT safe to fill in for
#'     every row - AZOMP missions sample well beyond the AR07W line (Scotian Slope/Rise,
#'     Bedford Basin, etc.), and those casts have no formal SECT_ID. Each station's
#'     LATITUDE/LONGITUDE is checked against the line corridor (great-circle cross-track and
#'     along-track distance, not just a bounding box) before it is labelled \code{sect_id};
#'     everything else is left blank for manual review.
#' }
#'
#' @export
#'
#' @examples
#' \dontrun{
#' data      <- extract_from_biochem("CAR2023573", biochem.user, biochem.password)
#' ocads_data <- convert_OCADS(data, biochem.password, biochem.user, prompt_notes = FALSE)
#' cchdo_data <- convert_CCHDO(ocads_data) # AR07W line/corridor defaults
#' write_CCHDO_exchange(cchdo_data, out_dir = "C:/submissions/CAR2023573/CCHDO")
#' }
#'
#' @seealso
#' \code{\link{convert_OCADS}} for the initial conversion from BCD to OCADS format,
#' \code{\link{write_CCHDO_exchange}} to write the reshaped data as a WHP-exchange file
#'
convert_CCHDO <- function(ocads_data,
                           sect_id     = "AR07W",
                           line_start  = c(lat = 53 + 40.76 / 60, lon = -(55 + 32.95 / 60)),
                           line_end    = c(lat = 57 + 49.81 / 60, lon = -(51 + 21.18 / 60)),
                           corridor_km = 50) {
  require(tidyverse)

  # Input validation ----
  if (!is.data.frame(ocads_data)) {
    stop("ocads_data must be a data frame")
  }

  required_cols <- c("EXPOCODE", "STNNBR", "CASTNO", "SAMPNO",
                      "DATE", "TIME", "LATITUDE", "LONGITUDE", "SOUNDING")
  missing_cols <- setdiff(required_cols, colnames(ocads_data))
  if (length(missing_cols) > 0) {
    stop(paste("Missing required columns:", paste(missing_cols, collapse = ", "),
               "\nEnsure input data is in OCADS format (output from convert_OCADS()), ",
               "with row 1 as the units row."))
  }

  if (nrow(ocads_data) < 2) {
    stop("ocads_data must include the units row (row 1) plus at least one data row")
  }

  cchdo_data <- ocads_data
  n <- nrow(cchdo_data)

  # Drop OCADS-only columns that aren't WHP-exchange parameters, and swap in the
  # bottom-depth SOUNDING column for OCADS's per-bottle DEPTH (see @details above) ----
  cchdo_data <- cchdo_data %>%
    select(-any_of(c("NAME", "PLATFORM", "BTL_LAT", "BTL_LON",
                      "BTL_DATE", "BTL_TIME", "DEPTH"))) %>%
    rename(DEPTH = SOUNDING)

  # Add SECT_ID, but only for stations that actually fall within corridor_km of the
  # line - AZOMP samples plenty of stations that aren't on it (see @details) ----
  lat_num <- suppressWarnings(as.numeric(cchdo_data$LATITUDE[-1]))
  lon_num <- suppressWarnings(as.numeric(cchdo_data$LONGITUDE[-1]))
  if (any(is.na(lat_num) | is.na(lon_num))) {
    stop("LATITUDE/LONGITUDE must be numeric to check station position against the line")
  }

  on_line <- .on_line_corridor(
    lat_p = lat_num, lon_p = lon_num,
    lat1 = line_start[["lat"]], lon1 = line_start[["lon"]],
    lat2 = line_end[["lat"]],   lon2 = line_end[["lon"]],
    corridor_km = corridor_km
  )

  cchdo_data <- cchdo_data %>%
    mutate(SECT_ID = c("", ifelse(on_line, sect_id, ""))) %>%
    relocate(SECT_ID, .after = EXPOCODE)

  off_line_stations <- unique(cchdo_data$STNNBR[-1][!on_line])
  message(
    "  [SECT_ID] ", sum(on_line), " of ", length(on_line), " bottles fell within ",
    corridor_km, " km of the ", sect_id, " line and were labelled accordingly."
  )
  if (length(off_line_stations) > 0) {
    message(
      "  [SECT_ID] ", length(off_line_stations), " station(s) left blank (outside the ",
      "corridor, not assumed to be ", sect_id, "): ",
      paste(off_line_stations, collapse = ", "),
      "\n            Assign these manually if they belong to a different, known section."
    )
  }

  # Rename NH3 to NH4 if present (CCHDO/WHP nomenclature) ----
  if ("NH3" %in% colnames(cchdo_data)) {
    cchdo_data <- cchdo_data %>%
      rename(NH4 = NH3)

    if ("NH3_FLAG_W" %in% colnames(cchdo_data)) {
      cchdo_data <- cchdo_data %>%
        rename(NH4_FLAG_W = NH3_FLAG_W)
    }
  }

  # DATE: OCADS uses YYYY-MM-DD, WHP-exchange uses YYYYMMDD with no delimiter.
  # Row 1 (units) is blank and untouched by gsub. ----
  cchdo_data$DATE[-1] <- gsub("-", "", cchdo_data$DATE[-1])
  if (any(is.na(cchdo_data$DATE[-1])) || any(nchar(cchdo_data$DATE[-1]) != 8)) {
    warning("Some dates may not have converted correctly. Check DATE format in input data.")
  }

  # Station names: alphanumeric only ----
  cchdo_data$STNNBR[-1] <- gsub("[^[:alnum:]]", "", cchdo_data$STNNBR[-1])
  if (any(is.na(cchdo_data$STNNBR[-1]) | cchdo_data$STNNBR[-1] == "")) {
    warning("Empty or NA values detected in STNNBR after cleaning. Review station names.")
  }

  # Column order: WHP-exchange convention puts identifying/positional parameters first,
  # in this order, then whatever data/flag pairs convert_OCADS() already produced ----
  lead_cols <- intersect(
    c("EXPOCODE", "SECT_ID", "STNNBR", "CASTNO", "SAMPNO",
      "DATE", "TIME", "LATITUDE", "LONGITUDE", "DEPTH"),
    colnames(cchdo_data)
  )
  cchdo_data <- cchdo_data %>% relocate(all_of(lead_cols))

  message("CCHDO/WHP-exchange reshaping complete.")
  message("Write the file with write_CCHDO_exchange() - NOT write_csv() - to get a valid ",
          "<EXPOCODE>_hy1.csv (stamp line, units row, END_DATA terminator).")

  return(cchdo_data)
}


#' Write a WHP-exchange bottle ("_hy1.csv") file
#'
#' Serializes the output of \code{convert_CCHDO()} to a real WHP-exchange bottle file:
#' a creation-stamp line, optional comment lines, the parameter header, the units row,
#' one data line per bottle, and a terminating \code{END_DATA} line. See
#' \url{https://exchange-format.readthedocs.io/en/latest/common.html} and
#' \url{https://exchange-format.readthedocs.io/en/latest/bottle.html}.
#'
#' @param cchdo_data A dataframe from \code{convert_CCHDO()} (row 1 = units row, exactly
#'   one EXPOCODE).
#' @param out_dir chr, directory to write to. File is named \code{<EXPOCODE>_hy1.csv} per
#'   WHP-exchange convention; the directory is created if it doesn't exist.
#' @param stamp_code chr, the submitter portion of the creation stamp (division + institution
#'   + creator initials, e.g. \code{"DFOBIOEOG"}). Defaults to \code{"DFOBIO"} plus the first
#'   three letters of \code{Sys.getenv("USERNAME")}.
#' @param comments chr vector, optional provenance notes; each element is written as its own
#'   \code{#}-prefixed comment line after the stamp (e.g. mission descriptor, data source).
#'
#' @return (invisibly) the path of the file written.
#' @export
#'
#' @examples
#' \dontrun{
#' cchdo_data <- convert_CCHDO(ocads_data, sect_id = "AR07W")
#' write_CCHDO_exchange(
#'   cchdo_data,
#'   out_dir  = "C:/submissions/CAR2023573/CCHDO",
#'   comments = "Source: BioChem mission CAR2023573, extracted via extract_from_biochem()"
#' )
#' }
#'
#' @seealso \code{\link{convert_CCHDO}}
#'
write_CCHDO_exchange <- function(cchdo_data, out_dir = ".", stamp_code = NULL, comments = NULL) {
  require(tidyverse)

  if (!is.data.frame(cchdo_data)) {
    stop("cchdo_data must be a data frame (output of convert_CCHDO())")
  }
  if (nrow(cchdo_data) < 2) {
    stop("cchdo_data must include the units row (row 1) plus at least one data row")
  }
  if (!"EXPOCODE" %in% colnames(cchdo_data)) {
    stop("cchdo_data must contain an EXPOCODE column - is this the output of convert_CCHDO()?")
  }

  expocode <- unique(as.character(cchdo_data$EXPOCODE[-1]))
  if (length(expocode) != 1 || is.na(expocode) || expocode == "") {
    stop("cchdo_data must contain exactly one EXPOCODE (one mission per file)")
  }

  if (is.null(stamp_code)) {
    initials <- toupper(substr(Sys.getenv("USERNAME"), 1, 3))
    if (identical(initials, "")) initials <- "XXX"
    stamp_code <- paste0("DFOBIO", initials)
  }
  stamp_line <- paste0("BOTTLE,", format(Sys.Date(), "%Y%m%d"), stamp_code)

  comment_lines <- if (!is.null(comments)) paste0("# ", comments) else character(0)

  header_line <- paste(colnames(cchdo_data), collapse = ",")
  unit_line   <- paste(as.character(unlist(cchdo_data[1, ])), collapse = ",")
  data_lines  <- apply(cchdo_data[-1, ], 1, paste, collapse = ",")

  if (!dir.exists(out_dir)) {
    dir.create(out_dir, recursive = TRUE)
  }
  out_file <- file.path(out_dir, paste0(expocode, "_hy1.csv"))

  writeLines(
    c(stamp_line, comment_lines, header_line, unit_line, data_lines, "END_DATA"),
    con      = out_file,
    useBytes = TRUE
  )

  message("Wrote WHP-exchange bottle file: ", out_file)
  invisible(out_file)
}
