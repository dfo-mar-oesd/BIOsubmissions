# Mission Notes Quick Reference Card

## 🎯 NEW: Automatic Integration

**Notes prompting is now built into the conversion functions!**

```r
# When you run convert_OCADS(), it automatically prompts for notes
ocads_data <- convert_OCADS(bcd_data, biochem.password, biochem.username)
# ↓ Prompts after conversion ↓

# Disable if needed (for automated scripts)
ocads_data <- convert_OCADS(bcd_data, biochem.password, biochem.username, 
                            prompt_notes = FALSE)
```

---

## Common Commands

### Read Notes
```r
# Read mission notes
read_mission_notes("OCADS", "BBMP", 2022)

# Read SDG notes
read_mission_notes("SDG", "2024")
```

### Add Notes
```r
# Interactive prompt (easiest!)
prompt_submission_notes("OCADS", "BBMP", 2023)

# Append programmatically
append_mission_notes("OCADS", "BBMP", 2023, 
                     notes = "Your notes here")
```

### Search & List
```r
# Search all notes
search_mission_notes("pH")
search_mission_notes("accession")

# List all missions
list_mission_notes()
list_mission_notes("OCADS")  # Filter by platform
```

## What to Document

✓ **Accession numbers**  
✓ **Submission dates and contacts**  
✓ **Manual corrections** (what and why)  
✓ **Issues encountered** and solutions  
✓ **Action items** for next time  
✓ **Lessons learned**

## Typical Workflow

```r
# 1. BEFORE submission - check previous notes
read_mission_notes("OCADS", "BBMP", 2022)

# 2. DURING submission - note any issues

# 3. AFTER submission - document everything
prompt_submission_notes("OCADS", "BBMP", 2023)
```

## Note Template

```markdown
## Date: YYYY-MM-DD

**Accession:** 0XXXXXX  
**Submitted to:** email@address.com

### Issues Encountered
- Issue 1: description
- Issue 2: description

### Manual Corrections
- Changed X to Y because...
- Fixed Z in metadata

### Action Items
- [ ] Follow up with lab
- [ ] Update script to fix...
```

## Search Tips

```r
# Find accession numbers
search_mission_notes("accession.*\\d+")

# Find pH issues
search_mission_notes("pH")

# Find manual changes
search_mission_notes("manual")

# Find action items
search_mission_notes("\\[ \\]")  # uncompleted

# Find emails
search_mission_notes("@")
```

## Common Issues to Document

- Date format corrections
- CTD vs Niskin bottle assignments
- Instrument type updates
- Investigator changes
- Abstract special characters
- Unit conversion issues
- Flag translation problems
- Lat/lon corrections
- Bottle flip issues
- Nutrient pair flips (NO2/NO3)

## Contact Info

**OCADS submissions:**  
alex.kozyr@noaa.gov

**BioChem:**  
biochem@dfo-mpo.gc.ca

---

📖 **Full Documentation:**  
`vignette("mission-notes", package = "BIOsubmissions")`

🔍 **Examples:**  
`inst/examples/mission_notes_example.R`
