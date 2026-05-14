# function to format from OCADS to CCHDO

#' Convert OCADS format to CCHDO format
#'
#' This function takes data in OCADS format (typically output from \code{convert_OCADS()})
#' and transforms it to meet CCHDO (CLIVAR and Carbon Hydrographic Data Office) submission
#' requirements. The main differences include column renaming, date format changes,
#' and station name cleaning.
#'
#' @param ocads_data A dataframe in OCADS format, typically produced by \code{convert_OCADS()}.
#'   Must contain columns: BTL_LAT, BTL_LON, DATE, DEPTH, STNNBR
#' @param prompt_notes logical, whether to prompt for submission notes after conversion (default: FALSE).
#'   Note: Typically notes are added during convert_OCADS(), but you can enable this if submitting to CCHDO specifically.
#'
#' @return A dataframe formatted for CCHDO submission with the following changes:
#'   \itemize{
#'     \item BTL_LAT renamed to LATITUDE
#'     \item BTL_LON renamed to LONGITUDE
#'     \item DATE format changed from YYYY-MM-DD to YYYYMMDD
#'     \item DEPTH unit set to 'METERS' in first row
#'     \item NAME column removed (if present)
#'     \item NH3 renamed to NH4 (if present)
#'     \item Special characters removed from STNNBR (station names)
#'   }
#'
#' @details
#' CCHDO has specific formatting requirements that differ slightly from OCADS:
#' \itemize{
#'   \item Date format must be YYYYMMDD without delimiters
#'   \item Latitude/Longitude use different column names than bottle positions
#'   \item Station names must contain only alphanumeric characters
#'   \item The first row of the DEPTH column contains the unit specification
#' }
#'
#' The output file should be written with \code{quote = 'none'} to meet CCHDO standards:
#' \code{write_csv(cchdo_data, file = "output.csv", quote = 'none')}
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Full workflow from BCD to CCHDO
#' library(BIOsubmissions)
#' library(tidyverse)
#'
#' # Read BioChem data
#' data <- read_csv("mission_BCD.csv")
#'
#' # Convert to OCADS format
#' ocads_data <- convert_OCADS(data, biochem.password, biochem.user)
#'
#' # Convert OCADS to CCHDO format
#' cchdo_data <- convert_CCHDO(ocads_data)
#'
#' # Write output file
#' write_csv(cchdo_data, "mission_CCHDO.csv", quote = 'none')
#' }
#'
#' @seealso
#' \code{\link{convert_OCADS}} for the initial conversion from BCD to OCADS format
#'
convert_CCHDO <- function(ocads_data, prompt_notes = FALSE) {
  require(tidyverse)

  # Input validation ----
  if (!is.data.frame(ocads_data)) {
    stop("ocads_data must be a data frame")
  }

  required_cols <- c("BTL_LAT", "BTL_LON", "DATE", "DEPTH", "STNNBR")
  missing_cols <- setdiff(required_cols, colnames(ocads_data))

  if (length(missing_cols) > 0) {
    stop(paste("Missing required columns:", paste(missing_cols, collapse = ", "),
               "\nEnsure input data is in OCADS format (output from convert_OCADS)"))
  }

  # Create CCHDO format ----
  cchdo_data <- ocads_data

  # Rename BTL_LAT and BTL_LON to LATITUDE and LONGITUDE
  cchdo_data <- cchdo_data %>%
    rename(LATITUDE = BTL_LAT, LONGITUDE = BTL_LON)

  # Update DATE format from YYYY-MM-DD to YYYYMMDD
  cchdo_data$DATE <- gsub(as.character(cchdo_data$DATE),
                          pattern = '-',
                          replacement = '')

  # Verify date format conversion was successful
  if (any(is.na(cchdo_data$DATE)) || any(nchar(cchdo_data$DATE) != 8)) {
    warning("Some dates may not have converted correctly. Check DATE format in input data.")
  }

  # Add depth unit in first row
  cchdo_data$DEPTH[1] <- 'METERS'

  # Remove NAME column if present
  cchdo_data <- cchdo_data %>%
    select(-any_of("NAME"))

  # Rename NH3 to NH4 if present (CCHDO prefers NH4 nomenclature)
  if ("NH3" %in% colnames(cchdo_data)) {
    cchdo_data <- cchdo_data %>%
      rename(NH4 = NH3)

    if ("NH3_FLAG_W" %in% colnames(cchdo_data)) {
      cchdo_data <- cchdo_data %>%
        rename(NH4_FLAG_W = NH3_FLAG_W)
    }
  }

  # Strip special characters out of station names (alphanumeric only)
  cchdo_data$STNNBR <- gsub('[^[:alnum:]]', '', cchdo_data$STNNBR)

  # Verify no NAs were introduced
  if (any(is.na(cchdo_data$STNNBR))) {
    warning("NA values detected in STNNBR after cleaning. Review station names.")
  }

  message("CCHDO conversion complete. Remember to write output with quote = 'none'")
  message("Example: write_csv(cchdo_data, 'output.csv', quote = 'none')")

  # Prompt for submission notes if enabled
  if (prompt_notes) {
    # Extract mission info from EXPOCODE
    if ("EXPOCODE" %in% colnames(cchdo_data)) {
      expocode <- unique(cchdo_data$EXPOCODE)[1]
      year <- as.numeric(substr(expocode, 5, 8))
      
      message("\n✓ CCHDO formatting complete!")
      message("EXPOCODE: ", expocode)
      
      # Prompt for notes with CCHDO as platform
      tryCatch({
        prompt_submission_notes("CCHDO", expocode, year)
      }, error = function(e) {
        message("Note: Could not prompt for submission notes. ",
                "You can add them later using append_mission_notes()")
      })
    }
  }

  return(cchdo_data)
}

