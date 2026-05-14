# BIOsubmissions

## Overview

`BIOsubmissions` is an R package for processing oceanographic data from BioChem and preparing it for submission to international data platforms. It automates data format conversion, unit conversions, quality flag translation, and data validation to ensure submissions meet platform requirements and catch common errors.

### Supported Platforms

- **OCADS** - Ocean Carbon Data System
- **CCHDO** - CLIVAR and Carbon Hydrographic Data Office  
- **SDG** - UN Sustainable Development Goals

### Key Features

✅ **Automated Format Conversion**: BCD → OCADS → CCHDO  
✅ **Unit Conversions**: Oxygen (mL/L → µmol/kg), Nutrients (µmol/L → µmol/kg)  
✅ **Quality Flag Translation**: BioChem (0-9) → WOCE (2,3,4,6,9)  
✅ **Data Validation**: Catches lat/lon errors, range issues, precision loss  
✅ **Replicate Handling**: Averages replicates and applies appropriate flags  
✅ **Method Translation**: BioChem → CCHDO/OCADS standard parameter names

### What Problems Does This Solve?

Historical submissions to OCADS and CCHDO have been rejected due to:
- Latitude/longitude out of range or missing negative signs
- Missing decimal places in coordinates
- Incorrect date formats
- Wrong units or unit conversions
- Improper quality flags

This package prevents these issues through automated validation and standardized processing.

## Installation

```r
# Install devtools if not already installed
if (!require("devtools")) install.packages("devtools")

# Install BIOsubmissions from GitHub
devtools::install_github("eogrady21/BIOsubmissions")
```

## Dependencies

The package requires:
```r
install.packages(c("tidyverse", "DBI", "RSQLite", "ROracle", "oce"))
```

You'll also need:
- BioChem database access (username and password)
- Oracle client for BioChem connection

## Quick Start

### 1. Set Up BioChem Credentials

```r
# Create: C:/users/YOUR_USERNAME/desktop/biochem_creds.R
biochem.user <- "your_username"
biochem.password <- "your_password"
```

**⚠️ Important**: Add `biochem_creds.R` to `.gitignore` to avoid committing credentials!

### 2. Extract Data from BioChem

Use the SQL query in `inst/BCD_QUERY.sql` to extract data for your mission from BioChem. Save the result as a CSV file.

### 3. Basic Workflow

```r
library(BIOsubmissions)
library(tidyverse)

# Source credentials
source("C:/users/ogradye/desktop/biochem_creds.R")

# Read BCD data
data <- read_csv("LAT2025146_BCD.csv", show_col_types = FALSE)

# Convert to OCADS format
ocads_data <- convert_OCADS(data, biochem.password, biochem.user)

# Save OCADS output
write_csv(ocads_data, "LAT2025146_OCADS.csv")

# Convert to CCHDO format
cchdo_data <- convert_CCHDO(ocads_data)

# Save CCHDO output (note: quote = 'none')
write_csv(cchdo_data, "LAT2025146_CCHDO.csv", quote = 'none')
```

## Main Functions

### `convert_OCADS()`

Converts BioChem BCD format to OCADS format.

**What it does:**
- Connects to BioChem to retrieve sounding (bottom depth) data
- Translates BioChem method names using lookup tables
- Converts units for oxygen, nutrients, and chlorophyll
- Applies WOCE quality flags
- Averages replicate measurements
- Validates data integrity

**Usage:**
```r
ocads_data <- convert_OCADS(
  data = bcd_dataframe,
  biochem.password = "your_password",
  biochem.username = "your_username"
)
```

**Required BCD columns:**
- `MISSION_DESCRIPTOR` (e.g., "18QL23573")
- `DATA_TYPE_METHOD` (BioChem parameter names)
- `DIS_DETAIL_DATA_VALUE`
- `DIS_DETAIL_DATA_QC_CODE` (0-9)
- `DIS_DETAIL_COLLECTOR_SAMP_ID`
- `EVENT_COLLECTOR_STN_NAME`
- `EVENT_COLLECTOR_EVENT_ID`
- `DIS_HEADER_SDATE` (format: %m/%d/%Y)
- `DIS_HEADER_STIME` (HHMM)
- `DIS_HEADER_SLAT` (decimal degrees)
- `DIS_HEADER_SLON` (decimal degrees)
- `DIS_HEADER_START_DEPTH` (meters)

### `convert_CCHDO()`

Converts OCADS format to CCHDO exchange format.

**What it does:**
- Renames columns (BTL_LAT → LATITUDE, BTL_LON → LONGITUDE)
- Changes date format (YYYY-MM-DD → YYYYMMDD)
- Removes special characters from station names
- Renames NH3 to NH4 (CCHDO preference)
- Sets depth unit in first row

**Usage:**
```r
cchdo_data <- convert_CCHDO(ocads_data)

# IMPORTANT: Write with quote = 'none' for CCHDO
write_csv(cchdo_data, "output.csv", quote = 'none')
```

## Data Preparation Tips

### Common BCD Issues from SQL Developer

If you extracted data via SQL Developer, you may need preprocessing:

```r
# Fix date format (SQL Developer exports as DD-MON-YY)
data$DIS_HEADER_SDATE <- format(
  as.Date(data$DIS_HEADER_SDATE, format = '%d-%b-%y'),
  format = '%m/%d/%Y'
)

# Add required columns if missing
data$DIS_DATA_NUM <- seq(1:nrow(data))
data$CREATED_BY <- Sys.getenv("USERNAME")
data$CREATED_DATE <- Sys.Date()
data$DATA_CENTER_CODE <- '20'
data$PROCESS_FLAG <- 0
data$BATCH_SEQ <- 1
```

### Handling Multiple Data Types

Combine fresh and frozen samples to avoid naming conflicts:

```r
data <- data %>%
  mutate(DATA_TYPE_METHOD = case_when(
    DATA_TYPE_METHOD %in% c('NH3_Filt_Fsh', 'NH3_Filt_F') ~ 'NH3_0',
    DATA_TYPE_METHOD %in% c('NO2_Filt_Fsh', 'NO2_Filt_F') ~ 'NO2_0',
    DATA_TYPE_METHOD %in% c('NO2NO3_Filt_Fsh', 'NO2NO3_Filt_F') ~ 'NO2NO3_0',
    DATA_TYPE_METHOD %in% c('PO4_Filt_Fsh', 'PO4_Filt_F') ~ 'PO4_0',
    DATA_TYPE_METHOD %in% c('SiO4_Filt_Fsh', 'SiO4_Filt_F') ~ 'SiO4_0',
    TRUE ~ DATA_TYPE_METHOD
  ))
```

## Understanding the Output

### File Structure

Output files have units in the first row:

```
EXPOCODE,STNNBR,DEPTH,CTDSAL,NO2+NO3,NO2+NO3_FLAG_W
18QL20230504,AR7W01,METERS,PSU,µmol/kg,
18QL20230504,AR7W01,5.2,35.1,12.3,2
```

### WOCE Quality Flags

- **2**: Acceptable (good quality)
- **3**: Questionable 
- **4**: Bad (do not use)
- **6**: Replicate measurement (averaged)
- **9**: Missing data

### Missing Data Convention

- Data values: `-999`
- Quality flags: `9`

## Documentation

For detailed guidance, see the package vignettes:

```r
# Quick start guide for new users
vignette("quickstart", package = "BIOsubmissions")

# Platform-specific submission instructions
vignette("submission-guide", package = "BIOsubmissions")
```

Or access function documentation:
```r
?convert_OCADS
?convert_CCHDO
```

**For new employees**: See `HANDOFF_DOCUMENTATION.md` for complete package overview and maintenance procedures.

## Troubleshooting

### "Platform name not found in lookup table"

Ship code not recognized. Update the platforms lookup table in `lookup.sqlite`.

### "EXPOCODE not properly generated"

Date format incorrect. Ensure dates are in `%m/%d/%Y` format.

### "No sounding data found in BioChem"

Cannot connect to BioChem or mission not found. Check credentials and mission descriptor.

### "Multiple DISTINCT units found"

Unit conflict in lookup table. Contact administrator to resolve.

## File Structure

```
BIOsubmissions/
├── R/
│   ├── OCADS.R          # Main conversion function
│   ├── CCHDO.R          # CCHDO-specific formatting
│   ├── SDG.R            # SDG formatting (in development)
│   └── update_lookup.R  # Lookup table management
├── inst/
│   ├── BCD_QUERY.sql    # Standard BioChem extraction query
│   └── extdata/         # Example files
├── vignettes/
│   ├── quickstart.Rmd   # Getting started guide
│   └── submission-guide.Rmd  # Platform submission instructions
├── lookup.sqlite        # Reference tables (ships, methods, units)
├── HANDOFF_DOCUMENTATION.md  # Complete handoff guide for new maintainers
├── CCHDO_template.R     # Updated template using convert_CCHDO()
└── OCADS_template.R     # Updated template with documentation
```

## Updating Lookup Tables

To add new ships or methods:

```r
# View current tables
con <- dbConnect(RSQLite::SQLite(), 'lookup.sqlite')
dbReadTable(con, "platforms")
dbReadTable(con, "methods")
dbDisconnect(con)

# See LookupTables.R for update procedures
```

## Contributing

For new methods, ships, or bug fixes:
1. Update appropriate lookup tables
2. Test with historical data
3. Document changes
4. Contact package maintainer

## Getting Help

- **Package issues**: Contact current data manager
- **BioChem access**: biochem@dfo-mpo.gc.ca
- **Submission questions**: See `vignette("submission-guide")`
- **Handoff/Training**: See `HANDOFF_DOCUMENTATION.md`

## Citation

If you use this package for data submissions, please cite:

```
BIOsubmissions: Processing oceanographic data for international submissions
BIO Data Management Team, Fisheries and Oceans Canada
https://github.com/eogrady21/BIOsubmissions
```

## License

Internal use within DFO. Contact maintainer for external use permissions.

---

**Maintainer**: Emily O'Grady (eogrady21)  
**Last Updated**: May 2026
