# =============================================================================
# Epic Cosmos Birthing Parent Vaccination Data Ingestion
# Source: Epic Cosmos SlicerDicer export, "Patients with Birthing Parent
#         Information" data model (Session IDs 2855051 / 2855069)
#
# Two population bases, exported as separate sessions with identical layout:
#   raw/staging/            -> standard/data.csv.gz           (all patients
#                               with birthing parent information -- no
#                               additional population filter)
#   raw/staging_cpt_birth/  -> standard/data_cpt_birth.csv.gz (subset filtered
#                               to a billed procedure indicating birth: CPT
#                               99460-99465, 99468, 99477 -- "1st hosp/birthing
#                               center care", "normal newborn care", etc.)
#
# Both sessions share the same layout: rows = Year x State of Residence;
# measures = Vit K (%), RSV (%), Hep B (%), Percentage of Population (%),
# Number of Patients with Birthing Parent Information. "Percentage of
# Population" is read but dropped from the standardized output -- it's not a
# coverage/vaccination measure, just each state's share of that session's
# total patient count for the year (not useful without also carrying the
# session total, and out of scope for this source).
#
# RSV (%) is blank for 2018-2022 in both sessions -- nirsevimab (the RSV
# monoclonal antibody this measure tracks) was not approved until 2023, so
# this is a genuine "measure not yet available" gap, not suppression.
#
# Suppressed cells ("10 or fewer" for counts, "-" for percentages) are
# imputed rather than left missing: counts as 5, percentages as 5 divided by
# the row's patient count. The suppressed_flag stays 1 either way, to mark
# the value as imputed. Neither session's raw export contains any suppressed
# cells as of the 2026-09-08 export, but both markers are still handled
# defensively since they are standard Epic Cosmos suppression tokens and may
# appear in future re-exports.
# =============================================================================

library(dplyr)
library(tidyr)
library(openxlsx2)
library(vroom)

# Check that msoffcrypto-tool is available (required for decrypting
# password-protected xlsx files)
msoffcrypto_check <- system(
  paste(Sys.which("python"), "-c \"import msoffcrypto\""),
  ignore.stdout = TRUE, ignore.stderr = TRUE
)
if (msoffcrypto_check != 0) {
  stop(
    "Python package 'msoffcrypto-tool' is required but not installed.\n",
    "Install it with: python -m pip install msoffcrypto-tool"
  )
}

# Initialize process record
if (!file.exists("process.json")) {
  process <- list(raw_state = NULL, raw_state_cpt_birth = NULL)
} else {
  process <- dcf::dcf_process_record()
}

if (!dir.exists("standard")) dir.create("standard")

xlsx_password <- Sys.getenv("EPIC_XLSX_PASSWORD")
if (!nzchar(xlsx_password)) {
  stop(
    "EPIC_XLSX_PASSWORD not set. ",
    "Add it to ~/.Renviron via usethis::edit_r_environ()"
  )
}

# Load FIPS lookup (state-level)
all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
state_fips_lookup <- all_fips %>%
  filter(nchar(geography) == 2) %>%
  select(geography, geography_name)

# ---------------------------------------------------------------------------
# Helper: decrypt one password-protected xlsx and return the sheet as an
# all-character grid with no header interpretation
# ---------------------------------------------------------------------------
read_slicerdicer_grid <- function(file, password) {
  temp_input     <- tempfile(fileext = ".xlsx")
  decrypted_file <- tempfile(fileext = ".xlsx")
  file.copy(file, temp_input, overwrite = TRUE)

  cmd <- sprintf(
    'python -m msoffcrypto -p "%s" "%s" "%s"',
    password,
    normalizePath(temp_input, winslash = "/"),
    decrypted_file
  )
  status <- system(cmd)
  unlink(temp_input)
  if (status != 0) stop("Failed to decrypt: ", file)

  wb   <- openxlsx2::wb_load(decrypted_file)
  grid <- openxlsx2::wb_to_df(
    wb, sheet = 1, col_names = FALSE,
    skip_empty_rows = FALSE, skip_empty_cols = FALSE
  )
  unlink(decrypted_file)

  as.data.frame(
    lapply(grid, function(x) { x <- as.character(x); x[is.na(x)] <- ""; x }),
    stringsAsFactors = FALSE
  )
}

# ---------------------------------------------------------------------------
# Helper: parse one measure column. "10 or fewer" (Epic's suppression marker
# for counts) is imputed as 5 and flagged; "-" (Epic's suppression marker for
# percentages) is left NA here and flagged -- the percentage measures impute
# it afterward as 5 / n_patients (see impute_suppressed_pct()), since that
# requires the row's patient count, which isn't available inside this
# function. A genuinely blank cell (e.g. RSV before 2023, when nirsevimab did
# not exist) is left NA, unflagged.
# ---------------------------------------------------------------------------
parse_measure <- function(raw) {
  raw <- trimws(as.character(raw))
  suppressed_count <- raw == "10 or fewer"
  suppressed_dash  <- !is.na(raw) & raw == "-"
  value <- suppressWarnings(as.numeric(gsub("[%,]", "", raw)))
  value[suppressed_count] <- 5
  list(
    value           = value,
    suppressed_flag = as.integer(suppressed_count | suppressed_dash)
  )
}

# ---------------------------------------------------------------------------
# Helper: impute a suppressed percentage cell as the standard imputed
# numerator (5) over the row's patient count, rather than leaving it NA.
# The suppressed flag stays 1 -- it still marks the value as imputed, just
# imputed to a number instead of to missing.
# ---------------------------------------------------------------------------
impute_suppressed_pct <- function(measure, n_patients) {
  needs_impute <- measure$suppressed_flag == 1 & is.na(measure$value)
  measure$value[needs_impute] <- 5 / n_patients[needs_impute] * 100
  measure
}

# ---------------------------------------------------------------------------
# Parse and standardize one SlicerDicer export: rows are Year x State of
# Residence, followed by 5 measure columns in a fixed order (Vit K %, RSV %,
# Hep B %, Percentage of Population %, Number of Patients). "Percentage of
# Population" is read (to keep column positions aligned) but intentionally
# not carried into the standardized output -- it's not a coverage measure,
# just each state's share of that session's total patient count.
# ---------------------------------------------------------------------------
standardize_export <- function(file, password, label) {
  grid <- read_slicerdicer_grid(file, password)

  header_row <- which(trimws(grid[[1]]) == "Year")[1]
  if (is.na(header_row)) {
    stop("[", label, "] Could not locate the 'Year' header row in: ", basename(file))
  }

  data_raw <- grid[(header_row + 1):nrow(grid), 1:7, drop = FALSE]
  rownames(data_raw) <- NULL
  colnames(data_raw) <- c(
    "year", "state_name", "raw_vitk", "raw_rsv", "raw_hepb",
    "raw_pctpop", "raw_npatients"
  )

  # Year is a merged cell, filled only on the first row of each year's states
  data_raw$year[data_raw$year == ""] <- NA
  data_raw <- tidyr::fill(data_raw, year, .direction = "down")

  # Drop "None of the above" (unknown state of residence)
  data_raw <- data_raw %>% filter(trimws(state_name) != "None of the above")

  vitk <- parse_measure(data_raw$raw_vitk)
  rsv  <- parse_measure(data_raw$raw_rsv)
  hepb <- parse_measure(data_raw$raw_hepb)
  npt  <- parse_measure(data_raw$raw_npatients)

  # Nirsevimab (the RSV monoclonal antibody this measure tracks) was not
  # approved until 2023, so the measure did not exist before then. The raw
  # export currently reports these years as blank (already NA), but force it
  # explicitly rather than relying on that -- a future re-export could report
  # 0% instead of blank, which would be misleading, not a true zero.
  pre_2023 <- as.integer(data_raw$year) < 2023
  rsv$value[pre_2023]           <- NA
  rsv$suppressed_flag[pre_2023] <- 0

  # Impute suppressed percentages as 5 / n_patients, since each is computed
  # by Epic Cosmos over the row's patient count. Done after the pre-2023
  # override above, so a suppressed-looking pre-2023 RSV cell (there
  # shouldn't be one, but just in case) stays NA rather than being imputed.
  vitk <- impute_suppressed_pct(vitk, npt$value)
  rsv  <- impute_suppressed_pct(rsv,  npt$value)
  hepb <- impute_suppressed_pct(hepb, npt$value)

  data_standard <- data_raw %>%
    mutate(
      geography_name = if_else(
        trimws(state_name) == "Total", "United States", trimws(state_name)
      ),
      time                                    = format(as.Date(paste0(year, "-12-31")), "%m-%d-%Y"),
      epic_pct_vitamin_k                      = vitk$value,
      epic_pct_vitamin_k_suppressed_flag      = vitk$suppressed_flag,
      epic_pct_rsv                            = rsv$value,
      epic_pct_rsv_suppressed_flag            = rsv$suppressed_flag,
      epic_pct_hepb                           = hepb$value,
      epic_pct_hepb_suppressed_flag           = hepb$suppressed_flag,
      epic_n_patients                         = npt$value,
      epic_n_patients_suppressed_flag         = npt$suppressed_flag
    ) %>%
    left_join(state_fips_lookup, by = "geography_name")

  n_unmatched <- sum(is.na(data_standard$geography))
  if (n_unmatched > 0) {
    unmatched <- unique(data_standard$state_name[is.na(data_standard$geography)])
    warning(
      "[", label, "] ", n_unmatched, " rows could not be matched to a FIPS code. ",
      "Unmatched: ", paste(unmatched, collapse = ", ")
    )
  }

  data_standard <- data_standard %>%
    filter(!is.na(geography)) %>%
    select(
      geography, time,
      epic_pct_vitamin_k, epic_pct_vitamin_k_suppressed_flag,
      epic_pct_rsv, epic_pct_rsv_suppressed_flag,
      epic_pct_hepb, epic_pct_hepb_suppressed_flag,
      epic_n_patients, epic_n_patients_suppressed_flag
    ) %>%
    arrange(geography, time)

  dupes <- data_standard %>% count(geography, time) %>% filter(n > 1)
  if (nrow(dupes) > 0) {
    stop("[", label, "] Duplicate geography/time combinations found: ", nrow(dupes))
  }

  message(
    "[", label, "] Standardized ", nrow(data_standard), " rows | ",
    length(unique(data_standard$geography)), " geographies | ",
    min(data_standard$time), " to ", max(data_standard$time)
  )

  data_standard
}

# =============================================================================
# BLOCK 1: All patients with birthing parent information
#   raw/staging/ -> standard/data.csv.gz
# =============================================================================
raw_files_all <- list.files("raw/staging", pattern = "\\.xlsx$", full.names = TRUE)
if (length(raw_files_all) == 0) {
  stop("No xlsx files found in raw/staging/")
}
if (length(raw_files_all) > 1) {
  stop("Expected exactly one xlsx file in raw/staging/, found ", length(raw_files_all))
}

raw_state_all <- as.list(tools::md5sum(raw_files_all))
names(raw_state_all) <- basename(raw_files_all)

if (!identical(process$raw_state, raw_state_all)) {
  data_all <- standardize_export(raw_files_all[1], xlsx_password, "all")
  vroom::vroom_write(data_all, "standard/data.csv.gz", delim = ",")

  process$raw_state <- raw_state_all
  dcf::dcf_process_record(updated = process)
}

# =============================================================================
# BLOCK 2: Patients with a CPT code indicating birth (billed procedure filter)
#   raw/staging_cpt_birth/ -> standard/data_cpt_birth.csv.gz
# =============================================================================
raw_files_cpt <- list.files("raw/staging_cpt_birth", pattern = "\\.xlsx$", full.names = TRUE)
if (length(raw_files_cpt) == 0) {
  stop("No xlsx files found in raw/staging_cpt_birth/")
}
if (length(raw_files_cpt) > 1) {
  stop("Expected exactly one xlsx file in raw/staging_cpt_birth/, found ", length(raw_files_cpt))
}

raw_state_cpt <- as.list(tools::md5sum(raw_files_cpt))
names(raw_state_cpt) <- basename(raw_files_cpt)

if (!identical(process$raw_state_cpt_birth, raw_state_cpt)) {
  data_cpt <- standardize_export(raw_files_cpt[1], xlsx_password, "cpt_birth")
  vroom::vroom_write(data_cpt, "standard/data_cpt_birth.csv.gz", delim = ",")

  process$raw_state_cpt_birth <- raw_state_cpt
  dcf::dcf_process_record(updated = process)
}
