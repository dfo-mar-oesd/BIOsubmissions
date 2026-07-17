#' Mission Notes Management System
#'
#' Functions to create, read, update, and manage submission notes organized
#' by platform and mission/year

#' Get path to mission notes file
#'
#' @param platform Character. Platform name (e.g., "OCADS", "SDG")
#' @param mission Character. Mission identifier (e.g., "BBMP", "JC24301")
#' @param year Numeric. Year of mission (optional, can be included in mission name)
#'
#' @return Character path to the notes file
#' @keywords internal
get_notes_path <- function(platform, mission, year = NULL) {
  base_path <- system.file("../log", package = "BIOsubmissions")

  # If package not installed, use relative path from working directory
  if (base_path == "" || !dir.exists(base_path)) {
    base_path <- "log"
  }

  # Construct filename
  if (!is.null(year)) {
    filename <- sprintf("%s_%s.md", mission, year)
  } else if (grepl("\\d{4}", mission)) {
    # Year already in mission name
    filename <- paste0(mission, ".md")
  } else {
    filename <- paste0(mission, ".md")
  }

  file.path(base_path, platform, mission, filename)
}

#' Read mission notes
#'
#' Read submission notes for a specific platform and mission
#'
#' @param platform Character. Platform name (e.g., "OCADS", "SDG")
#' @param mission Character. Mission identifier (e.g., "BBMP", "JC24301")
#' @param year Numeric. Year of mission (optional)
#'
#' @return Character vector with notes content, or NULL if file doesn't exist
#' @export
#'
#' @examples
#' \dontrun{
#' # Read BBMP 2022 notes
#' read_mission_notes("OCADS", "BBMP", 2022)
#'
#' # Read JC24301 notes
#' read_mission_notes("OCADS", "JC24301")
#' }
read_mission_notes <- function(platform, mission, year = NULL) {
  notes_file <- get_notes_path(platform, mission, year)

  if (!file.exists(notes_file)) {
    message("No notes found for ", platform, " - ", mission,
            if (!is.null(year)) paste0(" (", year, ")") else "")
    return(NULL)
  }

  readLines(notes_file, warn = FALSE)
}

#' Write mission notes
#'
#' Create or completely overwrite notes for a specific mission
#'
#' @param platform Character. Platform name (e.g., "OCADS", "SDG")
#' @param mission Character. Mission identifier (e.g., "BBMP", "JC24301")
#' @param notes Character. Notes content (can be vector of lines)
#' @param year Numeric. Year of mission (optional)
#' @param overwrite Logical. Whether to overwrite existing notes (default: FALSE)
#'
#' @return Invisibly returns TRUE if successful
#' @export
#'
#' @examples
#' \dontrun{
#' write_mission_notes("OCADS", "BBMP", "Initial submission notes", 2023)
#' }
write_mission_notes <- function(platform, mission, notes, year = NULL, overwrite = FALSE) {
  notes_file <- get_notes_path(platform, mission, year)

  # Check if file exists and overwrite is FALSE
  if (file.exists(notes_file) && !overwrite) {
    stop("Notes file already exists. Use overwrite = TRUE or use append_mission_notes() to add content.")
  }

  # Create directory if it doesn't exist
  notes_dir <- dirname(notes_file)
  if (!dir.exists(notes_dir)) {
    dir.create(notes_dir, recursive = TRUE)
  }

  # Write notes
  writeLines(notes, notes_file)
  message("Notes written to: ", notes_file)

  invisible(TRUE)
}

#' Append to mission notes
#'
#' Add new notes to existing mission notes file
#'
#' @param platform Character. Platform name (e.g., "OCADS", "SDG")
#' @param mission Character. Mission identifier (e.g., "BBMP", "JC24301")
#' @param notes Character. Notes to append (can be vector of lines)
#' @param year Numeric. Year of mission (optional)
#' @param timestamp Logical. Whether to add timestamp to entry (default: TRUE)
#' @param separator Logical. Whether to add separator line (default: TRUE)
#'
#' @return Invisibly returns TRUE if successful
#' @export
#'
#' @examples
#' \dontrun{
#' append_mission_notes("OCADS", "BBMP", "Fixed pH metadata issue", 2022)
#' }
append_mission_notes <- function(platform, mission, notes, year = NULL,
                                 timestamp = TRUE, separator = TRUE) {
  notes_file <- get_notes_path(platform, mission, year)

  # Create directory if it doesn't exist
  notes_dir <- dirname(notes_file)
  if (!dir.exists(notes_dir)) {
    dir.create(notes_dir, recursive = TRUE)
  }

  # Create file if it doesn't exist
  if (!file.exists(notes_file)) {
    # Create initial header
    header <- c(
      paste0("# ", platform, " - ", mission, if (!is.null(year)) paste0(" (", year, ")") else ""),
      "",
      paste0("**Created:** ", Sys.Date()),
      "",
      "---",
      ""
    )
    writeLines(header, notes_file)
  }

  # Prepare entry
  entry <- c()
  if (separator) entry <- c(entry, "")
  if (timestamp) entry <- c(entry, paste0("## ", Sys.Date()))
  entry <- c(entry, notes, "")

  # Append to file
  cat(paste(entry, collapse = "\n"), file = notes_file, append = TRUE)
  message("Notes appended to: ", notes_file)

  invisible(TRUE)
}

#' List all missions with notes
#'
#' Get a summary of all missions that have associated notes
#'
#' @param platform Character. Filter by platform (optional)
#'
#' @return Data frame with platform, mission, and file path
#' @export
#'
#' @examples
#' \dontrun{
#' # List all missions
#' list_mission_notes()
#'
#' # List only OCADS missions
#' list_mission_notes("OCADS")
#' }
list_mission_notes <- function(platform = NULL) {
  base_path <- system.file("../log", package = "BIOsubmissions")
  if (base_path == "" || !dir.exists(base_path)) {
    base_path <- "log"
  }

  if (!dir.exists(base_path)) {
    message("No log directory found")
    return(data.frame(platform = character(),
                     mission = character(),
                     file = character(),
                     stringsAsFactors = FALSE))
  }

  # Find all markdown files
  all_files <- list.files(base_path, pattern = "\\.md$",
                         recursive = TRUE, full.names = TRUE)

  if (length(all_files) == 0) {
    message("No notes files found")
    return(data.frame(platform = character(),
                     mission = character(),
                     file = character(),
                     stringsAsFactors = FALSE))
  }

  # Parse paths
  results <- lapply(all_files, function(f) {
    rel_path <- sub(paste0("^", base_path, "/?"), "", f)
    parts <- strsplit(rel_path, "/|\\\\")[[1]]

    data.frame(
      platform = parts[1],
      mission = if (length(parts) > 2) parts[2] else "",
      filename = basename(f),
      file = f,
      stringsAsFactors = FALSE
    )
  })

  result_df <- do.call(rbind, results)

  # Filter by platform if specified
  if (!is.null(platform)) {
    result_df <- result_df[result_df$platform == platform, ]
  }

  result_df
}

#' Interactive note prompt after submission
#'
#' Prompt user to enter notes after completing a submission
#'
#' @param platform Character. Platform name (e.g., "OCADS", "SDG")
#' @param mission Character. Mission identifier (e.g., "BBMP", "JC24301")
#' @param year Numeric. Year of mission (optional)
#'
#' @return Invisibly returns TRUE if notes were added, FALSE if skipped
#' @export
#'
#' @examples
#' \dontrun{
#' # After completing a submission
#' prompt_submission_notes("OCADS", "BBMP", 2023)
#' }
prompt_submission_notes <- function(platform, mission, year = NULL) {
  cat("\n")
  cat("=" %R% "=", 70, "\n")
  cat("SUBMISSION COMPLETE\n")
  cat("=" %R% "=", 70, "\n")
  cat("\n")

  # Check if notes exist
  existing_notes <- read_mission_notes(platform, mission, year)
  if (!is.null(existing_notes)) {
    cat("Existing notes found for this mission.\n")
    cat("\nLast 10 lines:\n")
    cat("---\n")
    cat(tail(existing_notes, 10), sep = "\n")
    cat("---\n\n")
  }

  response <- readline(prompt = "Would you like to add submission notes? (y/n): ")

  if (tolower(trimws(response)) != "y") {
    message("Skipping notes entry.")
    return(invisible(FALSE))
  }

  cat("\nEnter your notes (press Enter on empty line when done):\n")
  cat("---\n")

  notes <- c()
  repeat {
    line <- readline(prompt = "")
    if (line == "") break
    notes <- c(notes, line)
  }

  if (length(notes) > 0) {
    append_mission_notes(platform, mission, notes, year)
    cat("\nNotes saved successfully!\n")
    return(invisible(TRUE))
  } else {
    message("No notes entered.")
    return(invisible(FALSE))
  }
}

#' Search notes across all missions
#'
#' Search for specific text across all mission notes
#'
#' @param search_term Character. Text to search for (supports regex)
#' @param platform Character. Filter by platform (optional)
#' @param ignore_case Logical. Case-insensitive search (default: TRUE)
#'
#' @return Data frame with matches including context
#' @export
#'
#' @examples
#' \dontrun{
#' # Search for pH mentions
#' search_mission_notes("pH")
#'
#' # Search for accession numbers in OCADS
#' search_mission_notes("accession", platform = "OCADS")
#' }
search_mission_notes <- function(search_term, platform = NULL, ignore_case = TRUE) {
  all_notes <- list_mission_notes(platform)

  if (nrow(all_notes) == 0) {
    message("No notes to search")
    return(NULL)
  }

  results <- list()

  for (i in seq_len(nrow(all_notes))) {
    file_path <- all_notes$file[i]
    content <- readLines(file_path, warn = FALSE)

    # Find matching lines
    matches <- grep(search_term, content, ignore.case = ignore_case, value = FALSE)

    if (length(matches) > 0) {
      for (line_num in matches) {
        # Get context (2 lines before and after)
        context_start <- max(1, line_num - 2)
        context_end <- min(length(content), line_num + 2)
        context <- content[context_start:context_end]

        results[[length(results) + 1]] <- data.frame(
          platform = all_notes$platform[i],
          mission = all_notes$mission[i],
          filename = all_notes$filename[i],
          line = line_num,
          match = content[line_num],
          stringsAsFactors = FALSE
        )
      }
    }
  }

  if (length(results) == 0) {
    message("No matches found for: ", search_term)
    return(NULL)
  }

  do.call(rbind, results)
}

# Fix operator typo
"%R%" <- function(a, b) {
  paste0(rep(a, b), collapse = "")
}
