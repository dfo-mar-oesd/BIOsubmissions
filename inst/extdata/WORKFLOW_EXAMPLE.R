# BIOsubmissions - Complete Workflow Example
# Using the NEW streamlined extract_from_biochem() function
# Emily O'Grady, May 2026

# This script demonstrates the complete workflow from BioChem to CCHDO/OCADS
# submissions using the new direct extraction capabilities.

library(BIOsubmissions)
library(tidyverse)

# Setup: BioChem Credentials ----
# IMPORTANT: Never commit this file to Git!
source("C:/users/ogradye/desktop/biochem_creds.R")

# Mission to process
mission <- "CAR2023573"

# Step 1: Extract Data Directly from BioChem ----
# NEW! No need for SQL Developer
message("=== Step 1: Extracting data from BioChem ===")

bcd_data <- extract_from_biochem(
  mission_descriptor = mission,
  biochem.username = biochem.user,
  biochem.password = biochem.password,
  validate = TRUE  # Run preliminary validation
)

# Review console output for preliminary validation messages:
#   [✓] = Data found
#   [NOTE] = Informational (may be expected)
#   [!] = Warning - requires attention

# If you see warnings about missing QC flags, STOP and fix in BioChem first!

# Step 2: Convert to OCADS Format ----
message("\n=== Step 2: Converting to OCADS format ===")

ocads_data <- convert_OCADS(
  data = bcd_data,
  biochem.password = biochem.password,
  biochem.username = biochem.user,
  prompt_notes = TRUE  # Prompt for submission notes
)

# Review detailed validation messages:
#   - Carbonate chemistry parameters
#   - Tracer parameters  
#   - QC flag quality
#
# If you see [!] warnings about QC flags being all zeros:
#   1. DO NOT proceed with submission
#   2. Return to BioChem and apply proper QC flags
#   3. Re-run this script

# Step 3: Save OCADS Output ----
output_dir <- here::here("data", "2023", mission, "OCADS")
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

ocads_file <- file.path(output_dir, paste0(unique(ocads_data$EXPOCODE), "_data.csv"))
write_csv(ocads_data, ocads_file)
message("\n✓ OCADS file saved: ", ocads_file)

# Step 4: Convert to CCHDO Format (if needed) ----
message("\n=== Step 3: Converting to CCHDO format ===")

cchdo_data <- convert_CCHDO(ocads_data)

# Step 5: Save CCHDO Output ----
cchdo_dir <- here::here("data", "2023", mission, "CCHDO")
dir.create(cchdo_dir, recursive = TRUE, showWarnings = FALSE)

cchdo_file <- file.path(cchdo_dir, paste0(unique(cchdo_data$EXPOCODE), "_data.csv"))
write_csv(cchdo_data, cchdo_file, quote = 'none')  # IMPORTANT: quote = 'none'
message("✓ CCHDO file saved: ", cchdo_file)

# Next Steps ----
message("\n=== Next Steps ===")
message("1. Review validation warnings - address any [!] issues")
message("2. Prepare metadata documents (see submission guide vignette)")
message("3. Submit to platforms following their procedures")
message("4. Document submission in log/ directory")
message("\nFor detailed submission instructions:")
message("  vignette('submission-guide', package = 'BIOsubmissions')")

# Optional: Quick Data Summary ----
message("\n=== Data Summary ===")
message("Mission: ", mission)
message("EXPOCODE: ", unique(ocads_data$EXPOCODE))
message("Stations: ", length(unique(ocads_data$STNNBR)))
message("Samples: ", nrow(ocads_data) - 1)  # -1 for unit row
message("Parameters: ", 
        sum(!grepl("FLAG|EXPOCODE|NAME|PLATFORM|STNNBR|CASTNO|SAMPNO|DATE|TIME|SOUNDING|LATITUDE|LONGITUDE|BTL_", 
                   names(ocads_data))))

# List all parameters included
params <- names(ocads_data)[!grepl("FLAG|EXPOCODE|NAME|PLATFORM|STNNBR|CASTNO|SAMPNO|DATE|TIME|SOUNDING|LATITUDE|LONGITUDE|BTL_", 
                                    names(ocads_data))]
message("\nParameters included:")
for (p in params) {
  message("  - ", p)
}

message("\n✓ Workflow complete!")
