# CCHDO Template - UPDATED TO USE PACKAGE FUNCTION
# This template demonstrates the new workflow using convert_CCHDO()
# For detailed documentation, see vignette("quickstart")

library(tidyverse)
library(BIOsubmissions)

# Step 1: Read in OCADS file (output from convert_OCADS)
OCADS_fn <- 'C:/users/ogradye/Documents/local_submissions/data/2023/CAR2023573/OCADS/18QL23573_data.csv'
OCADS <- read_csv(OCADS_fn)

# Step 2: Convert OCADS format to CCHDO format using the package function
CCHDO <- convert_CCHDO(OCADS)

# Step 3: Write output file
# IMPORTANT: Use quote = 'none' for CCHDO requirements
write_csv(CCHDO,
          'C:/users/ogradye/Documents/local_submissions/data/2023/CAR2023573/CCHDO/18QL23573_data.csv',
          quote = 'none')

# ============================================================================
# ALTERNATIVE: Full workflow from BCD to CCHDO
# ============================================================================
# Uncomment below to run the complete workflow from BioChem data:
#
# library(BIOsubmissions)
# library(tidyverse)
# 
# # Source credentials
# source("C:/users/ogradye/desktop/biochem_creds.R")
# 
# # Read BCD data
# data <- read_csv("C:/Users/ogradye/Documents/local_submissions/data/2023/CAR2023573/CAR2023573_BCD.csv",
#                  show_col_types = FALSE)
# 
# # Convert to OCADS format
# ocads_data <- convert_OCADS(data, biochem.password, biochem.user)
# 
# # Convert to CCHDO format
# cchdo_data <- convert_CCHDO(ocads_data)
# 
# # Write CCHDO output (note: quote = 'none')
# write_csv(cchdo_data,
#           'C:/users/ogradye/Documents/local_submissions/data/2023/CAR2023573/CCHDO/18QL23573_data.csv',
#           quote = 'none')

# ============================================================================
# LEGACY CODE (for reference - now handled by convert_CCHDO function)
# ============================================================================
# # rename BTL_LAT and BTL_LON to latitude and longitude
# OCADS <- OCADS %>%
#   rename(LATITUDE = BTL_LAT, LONGITUDE = BTL_LON)
# 
# # UPDATE DATE FORMAT FROM YYYY-MM-DD TO YYYYMMDD
# OCADS$DATE <- gsub(as.character(OCADS$DATE), pattern = '-', replacement = '')
# 
# # add depth unit
# OCADS$DEPTH[1] <- 'METERS'
# 
# # remove name column
# OCADS <- OCADS %>%
#   select(-NAME)
# 
# # rename NH3 to NH4
# OCADS <- OCADS %>%
#   rename(NH4 = NH3)
# OCADS <- OCADS %>%
#   rename(NH4_FLAG_W = NH3_FLAG_W)
# 
# # strip special characters out of station names
# OCADS$STNNBR <- gsub('[^[:alnum:]]', '', OCADS$STNNBR)

