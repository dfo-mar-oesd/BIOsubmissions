# create (or update) lookup tables for reference data

library(DBI)
library(RSQLite)

# platforms table
# Define the path to your CSV file and the SQLite database
csv_file <- "C:/Users/ogradye/Documents/OA_data_submission/Maritimes OCADS Submission/Dependencies/Shared_models_vessel3.csv"
db_file <- "lookup.sqlite"

# Step 1: Create and connect to the SQLite database
con <- dbConnect(RSQLite::SQLite(), db_file)

# Step 2: Read the CSV file into a data frame
data <- read.csv(csv_file)

# Step 3: Write the data frame to the SQLite database
dbWriteTable(con, "platforms", data, overwrite = FALSE, row.names = FALSE)

# Step 4: Verify the data has been inserted correctly
result <- dbReadTable(con, "platforms")
print(result)

# Step 5: Disconnect from the database
dbDisconnect(con)

# method names table
# Define the path to your CSV file and the SQLite database
csv_file <- "C:/Users/ogradye/Documents/OA_data_submission/Maritimes OCADS Submission/Dependencies/method_map.csv"
db_file <- "lookup.sqlite"

# Step 1: Create and connect to the SQLite database
con <- dbConnect(RSQLite::SQLite(), db_file)

# Step 2: Read the CSV file into a data frame
data <- read.csv(csv_file)

# Step 3: Write the data frame to the SQLite database
dbWriteTable(con, "methods", data, overwrite = FALSE, row.names = FALSE)

# Step 4: Verify the data has been inserted correctly
result <- dbReadTable(con, "methods")
print(result)

# Step 5: Disconnect from the database
dbDisconnect(con)

# remove duplicates from methods lookup table
# Step 1: Create and connect to the SQLite database
con <- dbConnect(RSQLite::SQLite(), db_file)

# Step 2: Remove duplicates from the methods table
dbExecute(con, "DELETE FROM methods WHERE rowid NOT IN (SELECT MIN(rowid) FROM methods GROUP BY BioChem)")

# Step 5: Disconnect from the database
dbDisconnect(con)

# remove HEXLIKE HPLC
# Step 1: Create and connect to the SQLite database
con <- dbConnect(RSQLite::SQLite(), db_file)

# Step 2: Remove duplicates from the methods table
dbExecute(con, "DELETE FROM methods WHERE BioChem in ('HPLC_HEXLIKE', 'HPLC_HEXLIKE2')")

# Remove CTDTMP with ITS68
dbExecute(con, "DELETE FROM methods WHERE BioChem = 'Temp_CTD_1968'")

# Step 5: Disconnect from the database
dbDisconnect(con)

# Add ammonia method

amon <- data.frame(BioChem = 'NH3_Tech_F', CCHDO = 'NH3', Unit = 'UMOL/KG')



dbWriteTable(con_lookup, 'methods', amon, append = TRUE)

# add temp 1968
TMP <- data.frame(BioChem = 'Temp_CTD_1968', CCHDO = 'CTDTMP', Unit = 'ITS-68')
dbWriteTable(con, 'methods', TMP, append = TRUE)

# add filtered nutrients
# add temp 1968
nuts <- data.frame(
  BioChem = c('NH3_Filt_F',
              'NH3_Filt_Fsh',
              'PO4_Filt_F',
              'PO4_Filt_Fsh',
              'NO2NO3_Filt_F',
              'NO2NO3_Filt_Fsh',
              'NO2_Filt_F',
              'NO2_Filt_Fsh',
              'SiO4_Filt_F',
              'SiO4_Filt_Fsh'
              ),
  CCHDO = c('NH3',
            'NH3',
            'PHSPHT',
            'PHSPHT',
            'NO2+NO3',
            'NO2+NO3',
            'NITRIT',
            'NITRIT',
            'SILCAT',
            'SILCAT'

            ),
  Unit = 'UMOL/KG')
dbWriteTable(con, 'methods', nuts, append = TRUE)
