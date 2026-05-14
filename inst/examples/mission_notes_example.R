# Example: Using the Mission Notes System
# This script demonstrates typical usage of the mission logging functions

library(BIOsubmissions)

# ==============================================================================
# EXAMPLE 1: After completing a submission
# ==============================================================================

# You've just submitted BBMP 2023 data. Add notes interactively:
prompt_submission_notes("OCADS", "BBMP", 2023)

# Or programmatically:
append_mission_notes(
  platform = "OCADS",
  mission = "BBMP",
  year = 2023,
  notes = c(
    "## Submission Summary",
    "- Accession: 0240502",
    "- Submitted to: alex.kozyr@noaa.gov",
    "- Submission date: 2023-05-15",
    "",
    "## Manual Corrections Made",
    "- Fixed date formatting (YYYYMMDD → YYYY-MM-DD)",
    "- Updated CTDFLUOR sampling instrument to CTD",
    "- Removed special characters from abstract",
    "- Updated investigator list",
    "",
    "## Issues Encountered",
    "- Script generated incorrect date format",
    "- Metadata template had wrong instrument types",
    "",
    "## Action Items",
    "- [ ] Fix date formatting in processing script",
    "- [ ] Update instrument mapping in template",
    "- [ ] Verify pH metadata with lab"
  )
)

# ==============================================================================
# EXAMPLE 2: Before starting a new submission
# ==============================================================================

# Check previous year's notes to learn from past issues
cat("=== BBMP 2022 Notes ===\n")
notes_2022 <- read_mission_notes("OCADS", "BBMP", 2022)
cat(notes_2022, sep = "\n")

# Search for specific issues across all missions
cat("\n=== Searching for pH-related issues ===\n")
ph_issues <- search_mission_notes("pH", platform = "OCADS")
if (!is.null(ph_issues)) {
  print(ph_issues)
}

# ==============================================================================
# EXAMPLE 3: Managing notes for multiple missions
# ==============================================================================

# List all OCADS missions with notes
cat("\n=== All OCADS Missions ===\n")
ocads_missions <- list_mission_notes("OCADS")
print(ocads_missions)

# ==============================================================================
# EXAMPLE 4: Creating notes for a new mission
# ==============================================================================

# Starting a completely new mission
write_mission_notes(
  platform = "OCADS",
  mission = "NewPlatform",
  year = 2024,
  notes = c(
    "# OCADS - NewPlatform 2024",
    "",
    paste0("**Created:** ", Sys.Date()),
    "",
    "---",
    "",
    "## Initial Setup",
    "",
    "- First submission for this platform",
    "- Need to establish baseline procedures",
    ""
  ),
  overwrite = FALSE
)

# ==============================================================================
# EXAMPLE 5: Finding specific information
# ==============================================================================

# Find all mentions of accession numbers
cat("\n=== Accession Numbers ===\n")
accessions <- search_mission_notes("accession.*\\d+", platform = "OCADS")
if (!is.null(accessions)) {
  print(accessions[, c("mission", "match")])
}

# Find all resubmissions
cat("\n=== Resubmissions ===\n")
resubmissions <- search_mission_notes("resubmit", ignore_case = TRUE)
if (!is.null(resubmissions)) {
  print(resubmissions[, c("platform", "mission", "match")])
}

# ==============================================================================
# EXAMPLE 6: Workflow integration
# ==============================================================================

# Example function that integrates note prompting into submission workflow
submit_with_notes <- function(platform, mission, year, data, metadata) {
  
  # ... your submission code here ...
  cat("Submitting data...\n")
  
  # After submission completes, prompt for notes
  prompt_submission_notes(platform, mission, year)
  
  message("✓ Submission complete and notes saved!")
}

# Usage:
# submit_with_notes("OCADS", "BBMP", 2024, my_data, my_metadata)

# ==============================================================================
# EXAMPLE 7: Batch review of historical notes
# ==============================================================================

# Review all BBMP submissions
cat("\n=== BBMP Historical Review ===\n")
bbmp_missions <- list_mission_notes()
bbmp_missions <- bbmp_missions[grepl("BBMP", bbmp_missions$mission), ]

for (i in seq_len(nrow(bbmp_missions))) {
  cat("\n", rep("=", 60), "\n", sep = "")
  cat("Mission:", bbmp_missions$mission[i], "-", bbmp_missions$filename[i], "\n")
  cat(rep("=", 60), "\n", sep = "")
  
  notes <- read_mission_notes(
    platform = bbmp_missions$platform[i],
    mission = bbmp_missions$mission[i]
  )
  
  # Print first 20 lines
  cat(head(notes, 20), sep = "\n")
  cat("...\n")
}

# ==============================================================================
# EXAMPLE 8: Common searches
# ==============================================================================

# Find all CTD instrument issues
search_mission_notes("CTD|CTDFLUOR")

# Find all manual corrections
search_mission_notes("manually|manual")

# Find all email contacts
search_mission_notes("@")

# Find all action items
search_mission_notes("\\[ \\]")  # Uncompleted tasks

# Find funding-related notes
search_mission_notes("funding|grant")
