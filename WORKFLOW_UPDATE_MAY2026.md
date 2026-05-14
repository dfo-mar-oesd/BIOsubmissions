# BIOsubmissions Workflow Update - May 2026

## Summary

The BIOsubmissions package workflow has been updated to **fully rely on BioChem as the sole data source**, with enhanced automated validation checks. All data values and quality control flags are now extracted directly from BioChem, eliminating the need to reference raw lab data sheets for submission files.

---

## Key Changes

### 1. NEW: Direct BioChem Data Extraction

Added `extract_from_biochem()` function to **eliminate the need for SQL Developer**:

#### What It Does
- Connects directly to BioChem from R
- Executes the BCD query automatically for your mission
- Returns properly formatted BCD data ready for `convert_OCADS()`
- Formats dates correctly (%m/%d/%Y)
- Adds all required BioChem metadata columns
- Performs preliminary validation before conversion

#### Benefits
- ✅ No need to open SQL Developer
- ✅ No manual CSV export/import steps
- ✅ Eliminates copy-paste errors in mission descriptors
- ✅ Automatic date formatting
- ✅ Consistent column structure
- ✅ Preliminary validation catches issues early

#### Helper Function
Added `list_biochem_missions()` to query available missions:
- List missions by year
- Filter by ship code
- Shows sample counts and date ranges
- Useful when mission descriptor is unknown

### 2. Enhanced Data Validation

Added comprehensive automated checks in the `convert_OCADS()` function:

#### Carbonate Chemistry Parameter Validation
- Checks for presence of: `ALKALI`, `PH_TOT`, `TCARBN`, `PCO2`
- Reports which parameters are found vs. missing
- Warns if data exists but QC flags are missing or suspicious

#### Tracer Parameter Validation
- Checks for presence of: `CFC-12`, `SF6`, `DEL18O`
- Reports which tracers are found vs. missing
- Warns if data exists but QC flags are missing or suspicious

#### QC Flag Validation
- **Critical check**: Identifies parameters where all QC flags are `0` (no quality control applied)
- Warns about parameters with data values but missing QC flags
- Ensures data integrity before international submission


## What Gets Validated

### During `convert_OCADS()` Execution

The function now prints detailed validation messages:

```
--- BioChem Data Quality Validation ---
  [✓] Found carbonate chemistry parameters: ALKALI, PH_TOT, TCARBN
  [NOTE] Missing carbonate chemistry parameters: PCO2
         This may be expected if this mission did not collect carbonate chemistry data.
  [✓] Found tracer parameters: CFC-12, SF6
  [NOTE] Missing tracer parameters: CFC-11, CFC-113
         This may be expected if this mission did not collect tracer data.
  [!] All QC flags for ALKALI are 0 - no quality control has been applied!
  [✓] Data quality validation complete
```

### Expected Parameters

The validation checks for these **expected parameters** (may not be present for all missions):

**Carbonate Chemistry:**
- `ALKALI` (Total Alkalinity)
- `PH_TOT` (pH, Total scale)
- `TCARBN` (Total Inorganic Carbon / DIC)
- `PCO2` (Partial pressure of CO2)

**Tracers:**
- `CFC-12` (Chlorofluorocarbon-12)
- `SF6` (Sulfur hexafluoride)
- `DELO18` (Delta Oxygen 18)

---

## How to Use the New Validation

### 1. Before Running Conversion

**Ensure data is properly uploaded to BioChem:**
- All carbonate chemistry analyses uploaded with data values
- All tracer analyses uploaded with data values
- **All parameters have appropriate QC flags** (not all zeros!)
- CRM analyses recorded for accuracy tracking
- Replicate analyses recorded for precision tracking

### 2. During Conversion

**Review validation messages carefully:**

#### `[✓]` Success Messages
These confirm expected data was found. No action needed.

#### `[NOTE]` Informational Messages
These indicate missing parameters. Action depends on your mission:
- **If the parameter wasn't collected**: This is expected, ignore the message
- **If the parameter WAS collected**: Check BioChem upload, data may be missing

#### `[!]` Warning Messages - REQUIRE ACTION
These indicate potential data quality issues:

**"All QC flags for [PARAM] are 0 - no quality control has been applied!"**
- **Problem**: Data exists but no QC has been applied in BioChem
- **Action**: Return to BioChem and apply proper QC flags (1-9 scale)
- **Why it matters**: International platforms require documented QC

**"[PARAM] has data values but missing QC flags!"**
- **Problem**: Data exists but QC flag column is empty/NA
- **Action**: Add QC flags to BioChem for this parameter
- **Why it matters**: Cannot submit data without quality assessment

### 3. After Seeing Warnings

**DO NOT PROCEED WITH SUBMISSION** if you see `[!]` warnings.

**Corrective workflow:**
1. Return to BioChem interface
2. Review QC procedures for flagged parameters
3. Apply appropriate QC flags based on:
   - CRM accuracy
   - Replicate precision
   - Analyst review
   - Known instrument issues
4. Update flags in BioChem
5. Re-extract BCD data using `inst/BCD_QUERY.sql`
6. Re-run `convert_OCADS()` conversion
7. Verify warnings are resolved

---

## Updated Workflow Summary

### NEW Streamlined Workflow (Recommended)

```r
library(BIOsubmissions)
library(tidyverse)

# 1. Source BioChem credentials
source("C:/users/YOUR_USERNAME/desktop/biochem_creds.R")

# 2. Extract data directly from BioChem (NEW!)
data <- extract_from_biochem("LAT2025146", biochem.user, biochem.password)
# Automatic validation happens here - review console messages

# 3. Convert to OCADS (with additional validation checks)
ocads_data <- convert_OCADS(data, biochem.password, biochem.user)
# Review validation messages - address any [!] warnings before proceeding

# 4. Save OCADS output
write_csv(ocads_data, "MISSION_OCADS.csv")

# 5. Convert to CCHDO (if needed)
cchdo_data <- convert_CCHDO(ocads_data)
write_csv(cchdo_data, "MISSION_CCHDO.csv", quote = 'none')
```

### Alternative Legacy Workflow (Still Supported)

```r
# 1. Run SQL query in SQL Developer
# 2. Export to CSV
# 3. Read CSV and continue as before
```

### Standard Processing Steps

```r
library(BIOsubmissions)
library(tidyverse)

# 1. Source BioChem credentials
source("C:/users/YOUR_USERNAME/desktop/biochem_creds.R")

# 2. Load BCD data (extracted from BioChem)
data <- read_csv("MISSION_BCD.csv", show_col_types = FALSE)

# 3. Convert to OCADS (with validation checks)
ocads_data <- convert_OCADS(data, biochem.password, biochem.user)
# Review validation messages - address any [!] warnings before proceeding

# 4. Save OCADS output
write_csv(ocads_data, "MISSION_OCADS.csv")

# 5. Convert to CCHDO (if needed)
cchdo_data <- convert_CCHDO(ocads_data)
write_csv(cchdo_data, "MISSION_CCHDO.csv", quote = 'none')
```

### Metadata Preparation

When preparing submission metadata files:
- Reference **BioChem database** as the data source
- CRM batch numbers can be queried from BioChem
- Analyst names are recorded in BioChem
- Method references should point to lab SOPs
- Note that QC flags follow BioChem → WOCE translation

---

## Benefits of This Approach

### Data Integrity
✅ Single source of truth (BioChem)  
✅ No manual transcription errors  
✅ Complete traceability  
✅ Automated validation catches issues early

### Quality Assurance
✅ Forces proper QC flag application in BioChem  
✅ Identifies missing data before submission  
✅ Validates expected parameters for carbonate/tracer missions  
✅ Prevents submission of un-QC'd data

### Efficiency
✅ No need to dig through lab files for metadata  
✅ Automated checks reduce manual review time  
✅ Clear error messages guide corrective action  
✅ Consistent processing across all missions

---

## Important Notes

### What Still Requires Manual Work

Even though data comes from BioChem, you still need to:
- **Prepare metadata documents** for submission (cruise info, PI details, methods)
- **Review validation warnings** and take corrective action
- **Verify converted data** looks reasonable (spot checks)
- **Follow platform-specific submission procedures** (see submission guide vignette)

### What No Longer Requires Lab Files

You do NOT need to reference raw lab files for:
- ✅ Data values (all from BioChem)
- ✅ QC flags (all from BioChem)
- ✅ Replicate handling (automated by package)
- ✅ Unit conversions (automated by package)

However, lab files may still be useful for:
- Method descriptions in metadata
- Troubleshooting unexpected values
- Documenting instrument issues

---

## Testing the Updates

To test the new validation features:

1. Run conversion on a recent mission with carbonate chemistry:
```r
data <- read_csv("MISSION_WITH_CARBONATE_BCD.csv")
ocads <- convert_OCADS(data, biochem.password, biochem.user)
```

2. Check console output for validation messages

3. Verify expected messages appear:
   - Found carbonate parameters
   - Found or missing tracers (depending on mission)
   - QC flag validation results

4. If you see QC warnings, test the corrective workflow by fixing flags in BioChem and re-running

---

## Questions or Issues?

If you encounter problems with the new validation:
- Check that BioChem method names are in the lookup table
- Verify data was properly uploaded to BioChem
- Review the quickstart vignette for troubleshooting guidance
- Check log files for previous similar missions

---

## File Change Summary

### New Files

**Code:**
- `R/extract_biochem.R` - New functions for direct BioChem data extraction
  - `extract_from_biochem()` - Extract BCD data directly from BioChem
  - `list_biochem_missions()` - Query available missions in BioChem

### Modified Files

**Code:**
- `R/OCADS.R` - Added validation function and checks

**Documentation:**
- `README.Rmd` - Added extract_from_biochem() documentation and updated workflow
- `vignettes/quickstart.Rmd` - Added Step 2 showing new extract function, updated validation section
- `vignettes/submission-guide.Rmd` - Updated metadata requirements
- `HANDOFF_DOCUMENTATION.md` - Updated core workflow to show new streamlined process

**Templates:**
- `OCADS_template.R` - Updated to use extract_from_biochem() as primary method

### No Changes Required To

- Lookup table structure
- BCD query SQL (still used internally by extract_from_biochem)
- Unit conversion logic
- Flag translation mapping
- CCHDO conversion function
- NAMESPACE (auto-exports new functions)

---

*Updated: May 14, 2026*
