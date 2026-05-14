# Template for submission of data to OCADS, SDG and CCHDO
# Emily O'Grady
# 2025

# Summary ----

# This script can be used as a template to generate submission products for
# international platforms (OCADS, SDG, CCHDO), from BioChem data

# This workflow builds on previous methods developed by Reid Steele and uses the
# package BIOsubmissions to generate consistent data products

# This is the processing for mission: 
# devtools::install_github('eogrady21/BIOsubmissions')
library(BIOsubmissions)
library(tidyverse)
library(here)

# Set-Up ----

# BioChem credentials
source("C:/users/ogradye/desktop/biochem_creds.R")
  
# BCD data
data <- read_csv("C:/Users/ogradye/Documents/local_submissions/data/2025/LAT2025146/LAT2025146_BCD.csv",
                 show_col_types = FALSE)

# BCD was extracted through sql dev so needs some manual fixes to match expected 
# structure
data$DIS_HEADER_SDATE <- format(
  as.Date(
    data$DIS_HEADER_SDATE, format = '%d-%b-%y'),
  format = '%m/%d/%Y')
data$DIS_DATA_NUM <- seq(1:nrow(data))
data$CREATED_BY <- "Emily OGrady"
data$CREATED_DATE <- Sys.Date()
data$DATA_CENTER_CODE <- '20'
data$PROCESS_FLAG <- 0
data$BATCH_SEQ <- 1

# We need to combine nutrient data types into a single type for OCADS
# having both fresh and frozen data types creates a non unique naming error in the convert_OCADS function
# Instead should pre-process data to include a single, more generic data type for nutrients
# ie. NH3_Filt_Fsh and NH3_Filt_F get combined to NH3_0, while sample ID ranges are noted to be preserved in metadata
data <- data %>%
  mutate(DATA_TYPE_METHOD = case_when(
    DATA_TYPE_METHOD %in% c('NH3_Filt_Fsh', 'NH3_Filt_F') ~ 'NH3_0',
    DATA_TYPE_METHOD %in% c('NO2_Filt_Fsh', 'NO2_Filt_F') ~ 'NO2_0',
    DATA_TYPE_METHOD %in% c('NO2NO3_Filt_Fsh', 'NO2NO3_Filt_F') ~ 'NO2NO3_0',
    DATA_TYPE_METHOD %in% c('PO4_Filt_Fsh', 'PO4_Filt_F') ~ 'PO4_0',
    DATA_TYPE_METHOD %in% c('SiO4_Filt_Fsh', 'SiO4_Filt_F') ~ 'SiO4_0',
    TRUE ~ DATA_TYPE_METHOD
  ))

# OCADS ----

# convert data
ocads_data <- convert_OCADS(data, biochem.password, biochem.user)

write_csv(ocads_data,
          file = here("data", "2025", "LAT2025146", "LAT2025146_data.csv") )
