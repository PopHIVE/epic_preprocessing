# =============================================================================
# Epic Cosmos - Wrapper Script
# Runs sub-projects and copies outputs to cosmos/standard/ for backward
# compatibility. Actual processing lives in:
#   - cosmos_resp_infections/  (weekly respiratory, RSV tests)     ~biweekly
#   - cosmos_injury/           (opioid, firearm, heat ED visits)   ~annual
#   - cosmos_chronic/          (diabetes A1C, obesity BMI)         ~annual
#
# To run only selected sub-projects, set flags before sourcing this script:
#   run_resp_infections <- TRUE; run_injury <- FALSE; run_chronic <- FALSE
#   source("data/cosmos/ingest.R")
#
# Or set them here and source normally. Unset variables default to TRUE.
# =============================================================================

if (!exists("run_resp_infections")) run_resp_infections <- TRUE
if (!exists("run_injury"))          run_injury          <- TRUE
if (!exists("run_chronic"))         run_chronic         <- TRUE

old_wd <- getwd()

# --- Respiratory Infections ---
if (run_resp_infections) {
  setwd("../cosmos_resp_infections")
  source("ingest.R")
  setwd(old_wd)
}

# --- Injury / Overdose ---
if (run_injury) {
  setwd("../cosmos_injury")
  source("ingest.R")
  setwd(old_wd)
}

# --- Chronic Diseases ---
if (run_chronic) {
  setwd("../cosmos_chronic")
  source("ingest.R")
  setwd(old_wd)
}

# --- Copy standard files for backward compatibility with main Pophive/ingest repo---
# Only copy files whose sub-project was just run (or always copy if the
# file exists, to keep cosmos/standard/ consistent after partial runs)
copy_if_exists <- function(src, dst) {
  if (file.exists(src)) file.copy(src, dst, overwrite = TRUE)
}

copy_if_exists("../cosmos_resp_infections/standard/weekly.csv.gz",        "standard/weekly.csv.gz")
copy_if_exists("../cosmos_resp_infections/standard/monthly_tests.csv.gz", "standard/monthly_tests.csv.gz")
copy_if_exists("../cosmos_resp_infections/standard/no_geo.csv.gz",        "standard/no_geo.csv.gz")

copy_if_exists("../cosmos_injury/standard/monthly_injury.csv.gz",         "standard/monthly_injury.csv.gz")
copy_if_exists("../cosmos_injury/standard/yearly_injury.csv.gz",          "standard/yearly_injury.csv.gz")
copy_if_exists("../cosmos_injury/standard/heat_year_county.csv",          "standard/heat_year_county.csv")
copy_if_exists("../cosmos_injury/standard/heat_year_county.csv.gz",       "standard/heat_year_county.csv.gz")

copy_if_exists("../cosmos_chronic/standard/state_year.csv.gz",            "standard/state_year.csv.gz")
copy_if_exists("../cosmos_chronic/standard/county_year.csv.gz",           "standard/county_year.csv.gz")
copy_if_exists("../cosmos_chronic/standard/state_no_time.csv.gz",         "standard/state_no_time.csv.gz")
copy_if_exists("../cosmos_chronic/standard/county_no_time.csv.gz",        "standard/county_no_time.csv.gz")

# --- Merge measure_info.json from all sub-projects ---
sub_measure_info_files <- c(
  "../cosmos_resp_infections/measure_info.json",
  "../cosmos_injury/measure_info.json",
  "../cosmos_chronic/measure_info.json"
)

merged_measure_info <- lapply(sub_measure_info_files, jsonlite::read_json) |>
  Reduce(function(a, b) modifyList(a, b), x = _)

# Preserve "sources" as JSON arrays — auto_unbox would collapse
# [{"id": "x"}] into {"id": "x"} without I()
for (key in names(merged_measure_info)) {
  if (key == "_sources") next
  if (!is.null(merged_measure_info[[key]]$sources)) {
    merged_measure_info[[key]]$sources <- I(merged_measure_info[[key]]$sources)
  }
}

jsonlite::write_json(
  merged_measure_info,
  "measure_info.json",
  pretty     = TRUE,
  auto_unbox = TRUE
)
