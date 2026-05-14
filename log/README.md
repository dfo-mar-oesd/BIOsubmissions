# Mission Submission Logs

This directory contains submission notes and troubleshooting documentation organized by platform and mission.

## Structure

```
log/
├── OCADS/          # Ocean Carbon and Acidification Data System submissions
│   ├── BBMP/       # Bedford Basin Monitoring Program
│   ├── AZOMP/      # Atlantic Zone Offshore Monitoring Program
│   └── JC/         # Jacques Cartier missions
├── CCHDO/          # CLIVAR and Carbon Hydrographic Data Office submissions
└── SDG/            # Sustainable Development Goals submissions
```

## File Format

Notes are stored as Markdown files (`.md`) with the following naming convention:
- Mission with year: `MISSIONNAME_YEAR.md` (e.g., `AT4805_2022.md`)
- Annual program: `YEAR.md` (e.g., `2022.md`)
- Single mission: `MISSIONNAME.md` (e.g., `JC24301.md`)

## Using the Notes System

### R Functions

Use the provided R functions to interact with notes:

```r
library(BIOsubmissions)

# Read notes
read_mission_notes("OCADS", "BBMP", 2022)

# Add notes after submission
append_mission_notes("OCADS", "BBMP", 2023, 
                     notes = "Updated metadata for pH")

# Interactive prompt
prompt_submission_notes("OCADS", "BBMP", 2023)

# Search across all notes
search_mission_notes("accession")

# List all missions with notes
list_mission_notes()
```

### Manual Editing

You can also edit notes files directly in any text editor. The files use standard Markdown formatting.

## What to Document

Include in your notes:
- **Accession numbers**
- **Submission dates and contacts**
- **Manual adjustments** made to data or metadata
- **Issues encountered** and solutions
- **Action items** for future submissions
- **Lessons learned**

## Best Practices

1. **Add notes immediately after submission** while details are fresh
2. **Be specific** - include error messages, file names, exact changes
3. **Use consistent formatting** - follow the template structure
4. **Include "why"** not just "what" - explain your reasoning
5. **Cross-reference** related issues in other missions
6. **Update when needed** - add information as you learn more

## Templates

### Basic Note Structure

```markdown
# PLATFORM - MISSION (YEAR)

**Created:** YYYY-MM-DD
**Last Updated:** YYYY-MM-DD
**Accession Number:** XXXXXXX

---

## Submission Details

- **Status:** [Submitted/Pending/Resubmitted]
- **Submission Date:** YYYY-MM-DD
- **Contact:** email@address.com

## Issues Encountered

### Issue Title
- **Problem:** Description
- **Solution:** How it was fixed
- **Future:** How to prevent

## Action Items

- [ ] Task to complete
- [x] Completed task

---

*Submitted by: Your Name*
```

## See Also

- [Mission Notes Vignette](../vignettes/mission-notes.Rmd) - Complete usage guide
- [Submission Guide](../vignettes/submission-guide.Rmd) - Full submission workflow
- `?read_mission_notes` - Function documentation
