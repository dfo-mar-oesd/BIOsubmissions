# update lookups

#' Update look up tables
#'
#' @param table the lookup table to insert data into
#' @param column the column name to insert
#' @param value the value to insert
#'
#' @return print updated table
#' @export
#'
#' @examples
#' update_lookup(table = 'CRM', data = data.frame(MISSI))
update_lookup <- function(table, column, value) {
  # Input validation: Check that table exists and all values are strings
  if (!is.character(table) || !is.character(column) || !is.character(value)) {
    stop("Table name, column name, and value must all be strings.")
  }

  db_file <- system.file("inst/extdata/lookup.sqlite", package = 'BIOsubmissions')

  # Step 1: Connect to the SQLite database
  con <- dbConnect(RSQLite::SQLite(), db_file)

  # Check if the table exists
  if (!dbExistsTable(con, table)) {
    dbDisconnect(con)
    stop(paste("Table", table, "does not exist in the database."))
  }

  # Format data to be inserted
  data <- data.frame(value, stringsAsFactors = FALSE)
  colnames(data) <- column

  # Step 2: Write the data to the SQLite database
  tryCatch({
    dbWriteTable(con, table, data, append = TRUE, row.names = FALSE)
  }, error = function(e) {
    dbDisconnect(con)
    stop("Error writing to the database: ", e$message)
  })

  # Step 3: Verify the data has been inserted correctly
  result <- dbReadTable(con, table)
  print(result)

  # Step 4: Disconnect from the database
  dbDisconnect(con)
}
