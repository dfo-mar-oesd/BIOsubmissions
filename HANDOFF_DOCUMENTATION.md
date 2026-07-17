# BIOsubmissions Package - Handoff Documentation

**Date**: May 2026  
**Current Maintainer**: Emily O'Grady  
**Package Purpose**: Automate oceanographic data preparation for international platform submissions

---

## Executive Summary

This package processes oceanographic data from BioChem and formats it for submission to:
- **OCADS** (Ocean Carbon Data System)
- **CCHDO** (CLIVAR and Carbon Hydrographic Data Office)
- **SDG** (UN Sustainable Development Goals)

**Key improvement**: The package now includes integrated CCHDO conversion (`convert_CCHDO()`) eliminating the need for manual template scripts. This reduces errors and streamlines the workflow.

---

## Package Structure

```
BIOsubmissions/
├── R/
│   ├── OCADS.R          # Main conversion: BCD → OCADS
│   ├── CCHDO.R          # NEW: OCADS → CCHDO conversion
│   ├── SDG.R            # SDG formatting (in development)
│   └── update_lookup.R  # Lookup table management
├── vignettes/
│   ├── quickstart.Rmd   # NEW: Complete workflow guide for new users
│   └── submission-guide.Rmd  # NEW: Platform-specific submission instructions
├── inst/
│   ├── BCD_QUERY.sql    # Standard BioChem extraction query
│   └── extdata/         # Example files for LAT2025146
├── man/                 # Auto-generated documentation (do not edit directly)
├── lookup.sqlite        # Reference data (ships, methods, units)
├── README.Rmd           # UPDATED: Complete package documentation
├── CCHDO_template.R     # UPDATED: Now uses convert_CCHDO() function
├── OCADS_template.R     # UPDATED: Enhanced with comments and vignette references
└── DESCRIPTION          # UPDATED: Package metadata and dependencies
```

---

## What's New (May 2026 Update)

### 1. Integrated CCHDO Conversion Function
- **Location**: `R/CCHDO.R`
- **Functions**: `convert_CCHDO(ocads_data, sect_id)` + `write_CCHDO_exchange(cchdo_data, out_dir)`
- **Purpose**: Replaces manual template scripting and produces a real WHP-exchange
  `<EXPOCODE>_hy1.csv` file (stamp line, units row, `END_DATA` terminator) - see
  https://exchange-format.readthedocs.io/. Do NOT `write_csv()` the result of
  `convert_CCHDO()` directly; it is only the reshaped data, not the file structure CCHDO
  requires.
- **SECT_ID is not applied blindly**: `convert_CCHDO()` checks each station's
  LATITUDE/LONGITUDE against the AR07W line (defaults: station 1 at 53 40.76N 55 32.95W to
  station 17 at 57 49.81N 51 21.18W, 50 km corridor) and only labels stations that fall
  within it. AZOMP also samples off-line stations (Scotian Slope/Rise, Bedford Basin, etc.)
  that have no formal section - those are left with a blank SECT_ID. Check the `[SECT_ID]`
  console messages after each run and fill in blanks manually if they belong to a different,
  known section.
- **Usage**:
  ```r
  ocads_data <- convert_OCADS(bcd_data, user, pass)
  cchdo_data <- convert_CCHDO(ocads_data)  # AR07W line/corridor defaults
  write_CCHDO_exchange(cchdo_data, out_dir = "output/dir")
  ```

### 2. Comprehensive Documentation
- **Quickstart Vignette**: Step-by-step workflow for processing new missions
- **Submission Guide**: Platform-specific requirements and procedures
- **Updated README**: Complete package overview and examples
- **Function Documentation**: Full roxygen2 documentation for all functions

### 3. Enhanced Templates
- Templates now reference the new functions
- Include alternative workflows
- Better comments and documentation links

---

## Core Workflow

### Data Source Philosophy

**All data and QC flags are extracted directly from BioChem**, ensuring:
- Complete traceability to original analyses
- No manual data entry errors
- Consistent quality control application
- Automated validation of data completeness

The package includes automated checks for:
- Missing carbonate chemistry parameters (ALKALI, PH_TOT, TCARBN, PCO2)
- Missing tracer data (CFC-12, SF6, DELO18)
- Suspicious QC flags (all zeros, missing flags)
- Data integrity and unit conversions

### Standard Processing Steps

1. **Extract data from BioChem**
   - **NEW - Recommended:** Use `extract_from_biochem()` function in R
   - **Alternative:** Use SQL query `inst/BCD_QUERY.sql` via SQL Developer → export CSV
   - Ensure all carbonate chemistry and tracer data have been uploaded to BioChem with QC flags

2. **Prepare data (if needed - only for CSV imports)**
   - If using `extract_from_biochem()`, skip this step
   - Fix date formats from SQL Developer
   - Add missing columns
   - Consolidate method types

3. **Convert to OCADS**
   ```r
   # New streamlined workflow:
   data <- extract_from_biochem("CAR2023573", biochem.user, biochem.password)
   ocads_data <- convert_OCADS(data, biochem.password, biochem.user)
   # Review validation warnings about missing parameters or QC flags
   ```

4. **Convert to CCHDO (if needed)**
   ```r
   cchdo_data <- convert_CCHDO(ocads_data, sect_id = "AR07W")
   write_CCHDO_exchange(cchdo_data, out_dir = "output/dir")
   ```

5. **Prepare metadata and submit**
   - Metadata should reference BioChem as data source
   - CRM batch numbers and QC metrics can be queried from BioChem
   - See submission guide vignette

### Quick Reference Commands

```r
# Installation
devtools::install_github("eogrady21/BIOsubmissions")

# View documentation
vignette("quickstart")
vignette("submission-guide")
?extract_from_biochem
?convert_OCADS
?convert_CCHDO

# Standard workflow (NEW - STREAMLINED!)
library(BIOsubmissions)
source("biochem_creds.R")
data <- extract_from_biochem("CAR2023573", biochem.user, biochem.password)
ocads <- convert_OCADS(data, biochem.password, biochem.user)
cchdo <- convert_CCHDO(ocads, sect_id = "AR07W")
write_CCHDO_exchange(cchdo, out_dir = "output/dir")
```

---

## Important Files and Locations

### Lookup Tables
- **File**: `lookup.sqlite`
- **Purpose**: Contains reference data for:
  - Platform names (ships)
  - Method name translations (BioChem → CCHDO/OCADS)
  - Unit conversions
- **Management**: See `LookupTables.R` for update procedures
- **Common tasks**:
  - Adding new ship: Update `platforms` table
  - Adding new method: Update `methods` table with BioChem name, CCHDO name, and unit

### BioChem Query
- **File**: `inst/BCD_QUERY.sql`
- **Purpose**: Standard SQL query for extracting BCD format from BioChem
- **Usage**: Run in SQL Developer or Oracle client
- **Output**: CSV with required columns for `convert_OCADS()`

### Example Files
- **Location**: `inst/extdata/`
- **Contents**: Example BCD and processing scripts for LAT2025146
- **Use**: Reference for handling complex preprocessing cases

---

## Common Issues and Solutions

### Historical Problems (Now Addressed)

1. **Latitude/Longitude errors**
   - **Problem**: Missing negatives, out of range values
   - **Solution**: Built-in validation in `convert_OCADS()`
   - **Action**: Review validation warnings carefully

2. **Precision loss in coordinates**
   - **Problem**: Missing decimal places
   - **Solution**: Validation flags this issue
   - **Action**: Check CSV export settings from BioChem/SQL Developer

3. **Date format inconsistencies**
   - **Problem**: Different date formats from different sources
   - **Solution**: Clear format requirements documented
   - **Action**: Verify dates are in %m/%d/%Y before conversion

4. **Method naming conflicts**
   - **Problem**: Multiple BioChem methods map to same CCHDO parameter
   - **Solution**: Lookup table with proper handling
   - **Action**: Consolidate methods in preprocessing if needed

### Troubleshooting Quick Reference

| Error Message | Cause | Solution |
|--------------|-------|----------|
| "Platform name not found" | Ship code not in lookup | Add to `platforms` table |
| "EXPOCODE not properly generated" | Wrong date format | Fix to %m/%d/%Y |
| "Multiple DISTINCT units found" | Lookup table conflict | Resolve unit conflict in lookup |
| "No sounding data found" | BioChem connection issue | Check credentials, VPN, mission descriptor |

---

## Maintenance Tasks

### Regular Maintenance
- **Update lookup tables**: When new ships or methods are added
- **Review validation rules**: If new parameter types are added
- **Update documentation**: When workflow changes

### When to Update Lookup Tables

**Platform (ships):**
- New vessel used for sampling


**Methods:**
- New analytical technique added in BioChem
- Parameter name standardization changes at CCHDO/OCADS
- Unit conversions modified

**Example update procedure:**
```r
library(DBI)
library(RSQLite)

con <- dbConnect(RSQLite::SQLite(), 'lookup.sqlite')

# View current entries
dbReadTable(con, "methods")

# Add new entry (example)
new_method <- data.frame(
  BIOCHEM = "NewParam_Method",
  CCHDO = "NEWPARAM",
  Unit = "µmol/kg"
)
dbWriteTable(con, "methods", new_method, append = TRUE)

dbDisconnect(con)
```

---

## Testing

### Current Test Suite
- **Location**: `testthat/test_OCADS.R`
- **Status**: Basic framework exists, needs expansion
- **Priority**: Add tests for validation functions

### Recommended Test Additions
1. Latitude/longitude validation edge cases
2. Date format conversion
3. Method name translation with lookup table
4. Unit conversion accuracy
5. Flag translation correctness
6. Replicate averaging

### Running Tests
```r
devtools::test()
```

---

## Future Development Priorities

### Short-term (Next 6 months)
1. ✅ **COMPLETE**: Integrate CCHDO conversion into package
2. ✅ **COMPLETE**: Create comprehensive documentation
3. **TODO**: Expand test coverage
4. **TODO**: Add data validation function (as per suggestion 1 from analysis)
5. **TODO**: Implement configuration file system (as per suggestion 2)

### Medium-term (6-12 months)
1. Complete SDG formatting function
2. Add automated validation reports
3. Create web interface for non-R users
4. Integrate with ERDDAP or other data servers

### Long-term
1. Real-time BioChem connection without credentials file
2. Automated submission to platforms (API integration)
3. Version control for submissions
4. Quality dashboard for monitoring submissions

---

## Dependencies

### Required Packages
- **tidyverse**: Data manipulation and file I/O
- **DBI / RSQLite**: Lookup table access
- **ROracle**: BioChem database connection
- **oce**: Oceanographic calculations (density, pressure)

### System Requirements
- R >= 4.0.0
- Oracle client (for BioChem access)
- VPN connection to DFO network (for BioChem)

### Installation Issues
- **ROracle**: May require manual Oracle instant client installation
- **oce**: Usually straightforward from CRAN
- **VPN**: Required for BioChem access

---

## Key Contacts

### Internal (DFO)
- **BioChem Support**: ODIS
- **AZMP/AZOMP Data**: Lindsay Beazley/ Marc Ringuette

### External (Data Platforms)
- **OCADS**: Alex Kozyr - NOAA Affiliate <alex.kozyr@noaa.gov>
- **CCHDO**: cchdo@ucsd.edu 
- **SDG/IODE**: Schoo, Katherina <k.schoo@unesco.org> , Isensee, Kirsten <k.isensee@unesco.org>

---

## Handoff Checklist for New Maintainer

### Initial Setup
- [ ] Install R and RStudio
- [ ] Install required packages
- [ ] Set up Oracle client for BioChem
- [ ] Get BioChem credentials
- [ ] Clone package repository
- [ ] Read quickstart vignette
- [ ] Read submission guide vignette

### Knowledge Transfer
- [ ] Review this handoff document
- [ ] Walk through standard workflow with example data (LAT2025146)
- [ ] Review historical submission issues
- [ ] Understand lookup table structure
- [ ] Review BioChem extraction query
- [ ] Practice submission to one platform

### Access Verification
- [ ] BioChem database access working
- [ ] Can read/write to shared file locations
- [ ] Have contact info for platform submission coordinators

### Documentation Review
- [ ] Read all vignettes
- [ ] Review function documentation
- [ ] Understand validation rules
- [ ] Know how to update lookup tables

---

## Resources

### Package Documentation
- README: Overview and quick start
- Quickstart vignette: Detailed workflow
- Submission guide: Platform-specific instructions
- Function docs: `?convert_OCADS`, `?convert_CCHDO`

### External Resources
- **CCHDO Exchange Format**: https://exchange-format.readthedocs.io/
- **WOCE Quality Flags**: http://cchdo.github.io/hdo-assets/documentation/WHP_Exchange_Description.pdf
- **OCADS**: https://oceans.imas.utas.edu.au/OCADS/

### Historical Context
- Previous workflow by Reid Steele
- Issues log in submission guide vignette
- Example processing: LAT2025146 in `inst/extdata/`

---

## Questions to Ask Previous Maintainer


---

## Version History

### v0.2.0 (May 2026) - Current
- Added `convert_CCHDO()` function
- Created comprehensive vignettes
- Updated all templates
- Enhanced DESCRIPTION file
- Improved package documentation

### v0.1.0 (Earlier)
- Initial package structure
- `convert_OCADS()` function
- Basic lookup tables
- Template scripts

---

**Last Updated**: May 14, 2026  
**Next Review Due**: November 2026

For questions about this handoff document or package:
- Current maintainer: Emily O'Grady
- GitHub: https://github.com/eogrady21/BIOsubmissions
