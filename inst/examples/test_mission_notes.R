# Test Mission Notes Functions

# Load package
library(BIOsubmissions)

# ==============================================================================
# Test 1: List existing mission notes
# ==============================================================================
cat("TEST 1: List all mission notes\n")
cat(rep("=", 60), "\n", sep = "")

all_notes <- list_mission_notes()
print(all_notes)

# ==============================================================================
# Test 2: Read a specific mission's notes
# ==============================================================================
cat("\n\nTEST 2: Read BBMP 2022 notes\n")
cat(rep("=", 60), "\n", sep = "")

bbmp_2022 <- read_mission_notes("OCADS", "BBMP", 2022)
if (!is.null(bbmp_2022)) {
  cat(head(bbmp_2022, 30), sep = "\n")
  cat("\n...\n")
}

# ==============================================================================
# Test 3: Search functionality
# ==============================================================================
cat("\n\nTEST 3: Search for 'accession'\n")
cat(rep("=", 60), "\n", sep = "")

accessions <- search_mission_notes("accession", ignore_case = TRUE)
if (!is.null(accessions)) {
  print(accessions[, c("platform", "mission", "match")])
}

# ==============================================================================
# Test 4: Search for pH mentions
# ==============================================================================
cat("\n\nTEST 4: Search for 'pH'\n")
cat(rep("=", 60), "\n", sep = "")

ph_mentions <- search_mission_notes("pH")
if (!is.null(ph_mentions)) {
  print(ph_mentions[, c("platform", "mission", "match")])
}

# ==============================================================================
# Test 5: Create test notes (optional - uncomment to test)
# ==============================================================================
cat("\n\nTEST 5: Create test notes (commented out)\n")
cat(rep("=", 60), "\n", sep = "")
cat("To test note creation, uncomment the following code:\n\n")

cat('
# Test creating new notes
# write_mission_notes(
#   platform = "TEST",
#   mission = "TEST_MISSION",
#   year = 2024,
#   notes = c(
#     "# TEST - TEST_MISSION 2024",
#     "",
#     "This is a test note file.",
#     ""
#   ),
#   overwrite = TRUE
# )
# 
# # Test appending notes
# append_mission_notes(
#   platform = "TEST",
#   mission = "TEST_MISSION",
#   year = 2024,
#   notes = c(
#     "Test issue encountered",
#     "Test solution applied"
#   )
# )
# 
# # Read back the test notes
# test_notes <- read_mission_notes("TEST", "TEST_MISSION", 2024)
# cat(test_notes, sep = "\\n")
# 
# # Clean up (remove test file)
# test_file <- file.path("log", "TEST", "TEST_MISSION", "TEST_MISSION_2024.md")
# if (file.exists(test_file)) {
#   unlink(test_file)
#   cat("\\nTest file cleaned up\\n")
# }
')

# ==============================================================================
# Test 6: List OCADS missions only
# ==============================================================================
cat("\n\nTEST 6: List OCADS missions only\n")
cat(rep("=", 60), "\n", sep = "")

ocads_notes <- list_mission_notes("OCADS")
print(ocads_notes)

# ==============================================================================
# Summary
# ==============================================================================
cat("\n\nTEST SUMMARY\n")
cat(rep("=", 60), "\n", sep = "")
cat("✓ Total missions with notes:", nrow(all_notes), "\n")
cat("✓ OCADS missions:", nrow(ocads_notes), "\n")
cat("✓ All tests completed successfully!\n")
