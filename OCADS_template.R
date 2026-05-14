# Template for submission of data to OCADS, SDG and CCHDO
# Emily O'Grady
# 2025

# Summary ----

# This script can be used as a template to generate submission products for
# international platforms (OCADS, SDG, CCHDO), from BioChem data

# This workflow builds on previous methods developed by Reid Steele and uses the
# package BIOsubmissions to generate consistent data products

# For detailed documentation, see:
#   vignette("quickstart")           - Complete workflow guide
#   vignette("submission-guide")     - Platform-specific submission instructions
#   ?convert_OCADS                   - Function documentation
#   ?convert_CCHDO                   - Function documentation

# This is the processing for mission: [UPDATE MISSION NAME]

# Installation ----
# devtools::install_github('eogrady21/BIOsubmissions')

library(BIOsubmissions)
library(tidyverse)
library(here)

# Set-Up ----

# BioChem credentials (NEVER commit this file to Git!)
source("C:/users/ogradye/desktop/biochem_creds.R")

# Method 1: Extract data directly from BioChem (RECOMMENDED - NEW!)
# This eliminates the need for SQL Developer
data <- extract_from_biochem(
  mission_descriptor = "HUD2016003",
  biochem.username = biochem.user,
  biochem.password = biochem.password
)

# Method 2: Load from CSV (if you already exported from SQL Developer)
# data <- read_csv("C:/Users/ogradye/Documents/local_submissions/data/2016/HUD2016003/HUD2016003_BCD_d.csv",
#                  show_col_types = FALSE)

# Data Preparation (if needed) ----
# If using extract_from_biochem(), skip this section - data is already formatted!
# 
# Uncomment and adapt the following ONLY if loading from CSV exported via SQL Developer

# # Fix date format (if extracted via SQL Developer)
# data$DIS_HEADER_SDATE <- format(
#   as.Date(data$DIS_HEADER_SDATE, format = '%d-%b-%y'),
#   format = '%m/%d/%Y'
# )
# 
# # Add required columns if missing
# if (!"DIS_DATA_NUM" %in% names(data)) {
#   data$DIS_DATA_NUM <- seq(1:nrow(data))
# }
# if (!"CREATED_BY" %in% names(data)) {
#   data$CREATED_BY <- Sys.getenv("USERNAME")
# }
# if (!"CREATED_DATE" %in% names(data)) {
#   data$CREATED_DATE <- Sys.Date()
# }
# data$DATA_CENTER_CODE <- '20'
# data$PROCESS_FLAG <- 0
# data$BATCH_SEQ <- 1
# 
# # Consolidate multiple data types for same parameter (e.g., fresh vs frozen nutrients)
# data <- data %>%
#   mutate(DATA_TYPE_METHOD = case_when(
#     DATA_TYPE_METHOD %in% c('NH3_Filt_Fsh', 'NH3_Filt_F') ~ 'NH3_0',
#     DATA_TYPE_METHOD %in% c('NO2_Filt_Fsh', 'NO2_Filt_F') ~ 'NO2_0',
#     DATA_TYPE_METHOD %in% c('NO2NO3_Filt_Fsh', 'NO2NO3_Filt_F') ~ 'NO2NO3_0',
#     DATA_TYPE_METHOD %in% c('PO4_Filt_Fsh', 'PO4_Filt_F') ~ 'PO4_0',
#     DATA_TYPE_METHOD %in% c('SiO4_Filt_Fsh', 'SiO4_Filt_F') ~ 'SiO4_0',
#     TRUE ~ DATA_TYPE_METHOD
#   ))

# OCADS Conversion ----

# Convert data to OCADS format
# This function:
#  - Retrieves sounding data from BioChem
#  - Extracts ALL data values and QC flags directly from BioChem
#  - Validates data completeness (carbonate chemistry, tracers)
#  - Checks for missing or suspicious QC flags
#  - Translates BioChem method names to CCHDO/OCADS parameters
#  - Converts units (oxygen, nutrients, chlorophyll)
#  - Applies WOCE quality flags (BioChem 0-9 → WOCE 2,3,4,6,9)
#  - Averages replicates
#  - Validates data integrity with spot checks

# IMPORTANT: Ensure all carbonate chemistry and tracer data has been uploaded
# to BioChem with proper QC flags BEFORE running this conversion!

ocads_data <- convert_OCADS(data, biochem.password, biochem.user)

# Save OCADS output
write_csv(ocads_data,
          file = here("data", "2016", "HUD2016003", "18HU20160410_data.csv"))

# Review conversion messages and warnings carefully:
#  - [NOTE] messages indicate expected missing parameters (OK if not collected)
#  - [!] warnings indicate data quality issues that MUST be addressed
#  - Pay special attention to QC flag warnings (all zeros = no QC applied)
#  
# If you see QC flag warnings:
#  1. Return to BioChem and apply proper quality control flags
#  2. Re-extract BCD data
#  3. Re-run this conversion

# CCHDO Conversion (if needed) ----

# Convert OCADS output to CCHDO format
cchdo_data <- convert_CCHDO(ocads_data)

# Save CCHDO output (IMPORTANT: use quote = 'none')
write_csv(cchdo_data,
          file = here("data", "2016", "HUD2016003", "CCHDO", "18HU20160410_data.csv"),
          quote = 'none')

# Next Steps ----

# 1. Review output files for quality
# 2. Prepare metadata documentation (see vignette("submission-guide"))
# 3. Submit to appropriate platform(s)
#    - OCADS: https://oceans.imas.utas.edu.au/OCADS/
#    - CCHDO: https://cchdo.ucsd.edu/submit
#    - SDG: Via OCADS or national data center
# 4. Track submission and respond to reviewer feedback

# For detailed submission instructions, see:
# vignette("submission-guide", package = "BIOsubmissions")

