# =============================================================================
# Epic Cosmos Vector-Borne Disease Data Ingestion
# Source: Epic Cosmos SlicerDicer exports (raw/staging/**/*.xlsx), three
# exports, three standardized outputs:
#
#   1. raw/staging/*.xlsx            -> standard/data.csv.gz
#      Monthly patient counts with ANY vector-borne disease diagnosis (new or
#      ongoing), by state: Lyme, Babesiosis, Malaria, RMSF (Rocky Mountain
#      Spotted Fever), West Nile, and Dengue.
#      SlicerDicer session 2852889 (originally created as session 2852629;
#      Epic assigns a new session ID whenever the session is re-saved, but the
#      query is unchanged), "Lyme N and Babesiosis N and Malaria N and RMSF N
#      and West Nile N and Dengue N and Number of Patients by State of
#      Residence":
#        Data model      : Patients
#        Population base : All Patients
#        Criteria        : Country of Care = United States of America,
#                           Has Any Encounters
#        Measures        : Lyme N, Babesiosis N, Malaria N, RMSF N, West Nile
#                           N, Dengue N (numerators), Number of Patients
#                           (denominator)
#      As of the 2026-08-28 export, the measure labels changed from the
#      "<Disease> N" form (e.g. "Lyme N") to the lowercase "n <disease>" form
#      (e.g. "n lyme"). MEASURE_PATTERNS matches both forms.
#
#   2. raw/staging/new_cases/*.xlsx  -> standard/data_new_cases.csv.gz
#      Same population, diseases and layout as (1), but counting only NEW
#      (first-time) diagnoses. SlicerDicer session 2861722, "n new west nile
#      and n new babesiosis and n new malaria and n new RMSF and n new dengue
#      and Number of Patients and n new lyme by State of Residence".
#      Output columns carry a `new_` infix (epic_n_new_lyme, ...) so they
#      cannot collide with (1) in a bundle.
#
#   3. raw/staging/lab_tests/*.xlsx  -> standard/data_lab_tests.csv.gz
#      MONTHLY lab component results for each pathogen (babesia, RMSF, West\n#      Nile, dengue, malaria), by state. SlicerDicer session 2863670, "Number
#      of Lab Component Results and n west nile tests and n babesia and n RMSF
#      tests and n malaria tests and n dengue tests by State of Residence".
#        Data model      : Lab Component Results
#        Population base : All Lab Component Results
#        Criteria        : Country of Care = United States of America
#      Time is monthly (columns are measure > year > month, states are rows);\n#      time = last day of month. The trailing partial month ("Sep 1 - Sep 8")\n#      is dropped, as in the other monthly exports.
#   4. raw/staging/new_cases_pop_filter/*.xlsx
#                                    -> standard/data_new_cases_pop_filter.csv.gz
#      MONTHLY new-case counts (Year > Month > State, same layout as (1)) for\n#      the same six diseases, but the population itself is filtered: only patients
#      with a first diagnosis of one of the diseases (ICD-10 based, "Not
#      Preceded By" the same diagnosis within 6 months), Country of Care = United
#      States, Has Any Encounters. SlicerDicer session 2863672. "Number of
#      Patients" is therefore the number of patients in that filtered
#      population, NOT all patients, so percents are shares of patients with a
#      new vector-borne diagnosis. Columns carry a `new_popfilter_` infix.
#      The export (1)-(2) call this the "problem list" definition of new
#      cases; (4) is the "pop filter" definition.
## Each export is tracked with its own key in process.json (raw_state,
# new_cases_state, lab_state, new_cases_pop_filter_state) and only reprocessed
# when its files change.
#
# Raw layout of the monthly exports (rows, 1-indexed as in the spreadsheet):
#   1-8   : session metadata
#   11    : measure labels for the value columns
#   12    : row-dimension labels (A: Year, B: Month, C: State of Residence)
#   13+   : data rows, ordered Year > Month > State (State changes fastest).
#           Year and Month are merged cells - blank until the next value, so
#           both must be filled down. State of Residence is present on every
#           row (never blank).
# Raw layout of the lab export:
#   1-8   : session metadata
#   11    : measure labels, only on the first column of each measure group
#   12    : year (first column of each year within a measure group)\n#   13    : month of each column (last one is the partial month)\n#   14    : A: State of Residence\n#   15+   : one row per state, plus "None of the above" and "Total"
#
# Output (PopHIVE wide format):
#   index   : geography (FIPS string, "00" = national), time
#   measures, one triplet per disease:
#     epic_n_<disease>, epic_pct_<disease> -> epic_<disease>_suppressed_flag
#       (the flag covers both; the percent is derived from the same
#       numerator cell)
#   denominator: epic_n_patients -> epic_n_patients_suppressed_flag
#   (new cases: epic_n_new_<disease>, epic_pct_new_<disease>,
#    epic_new_<disease>_suppressed_flag; lab tests: epic_n_tests_<disease>,
#    epic_pct_tests_<disease>, epic_tests_<disease>_suppressed_flag, with
#    denominator epic_n_lab_results -> epic_n_lab_results_suppressed_flag)
#
# Conventions applied here:
#   - time is the LAST day of the month, formatted YYYY-mm-dd; the trailing
#     partial period at the end of the exported range (e.g. "Jul 1 - Jul 28")
#     is dropped
#   - state_name is resolved to a FIPS `geography` and then dropped
#   - suppression is handled per measure (blank / "10 or fewer" -> 5, flag 1)
#   - each disease measure is a PERCENT of its denominator (all patients, or
#     all lab component results), not a rate per 100,000
# =============================================================================

library(dplyr)
library(tidyr)
library(lubridate)
library(openxlsx2)

# Check that msoffcrypto-tool is available (required for decrypting password-protected xlsx files)
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

# Load FIPS lookup
all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

state_fips_lookup <- all_fips %>%
  filter(nchar(geography) == 2) %>%
  select(geography, geography_name)

# Initialize process record
if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

# Password for xlsx files (set in .Renviron via usethis::edit_r_environ())
xlsx_password <- Sys.getenv("EPIC_XLSX_PASSWORD")

# -----------------------------------------------------------------------------
# Label maps: the stable part of each measure column label, matched against the
# measure-label row. Unrecognized/ambiguous labels stop the run instead of
# silently landing on the wrong column - extend these maps when a session
# changes.
# -----------------------------------------------------------------------------
MEASURE_PATTERNS <- c(
  lyme        = "^(Lyme N|n lyme)$",
  babesiosis  = "^(Babesiosis N|n babesiosis)$",
  malaria     = "^(Malaria N|n malaria)$",
  rmsf        = "^(RMSF N|n RMSF)$",
  west_nile   = "^(West Nile N|n west nile)$",
  dengue      = "^(Dengue N|n dengue)$",
  n_patients  = "^Number of Patients$"
)
NEW_CASE_PATTERNS <- c(
  lyme        = "^n new lyme$",
  babesiosis  = "^n new babesiosis$",
  malaria     = "^n new malaria$",
  rmsf        = "^n new RMSF$",
  west_nile   = "^n new west nile$",
  dengue      = "^n new dengue$",
  n_patients  = "^Number of Patients$"
)
POP_FILTER_PATTERNS <- c(
  lyme        = "^n lyme$",
  babesiosis  = "^n babesiosis$",
  malaria     = "^n malaria$",
  rmsf        = "^n RMSF$",
  west_nile   = "^n west nile$",
  dengue      = "^n dengue$",
  n_patients  = "^Number of Patients$"
)
LAB_PATTERNS <- c(
  babesiosis    = "^n babesia( tests)?$",
  rmsf          = "^n RMSF tests$",
  west_nile     = "^n west nile tests$",
  dengue        = "^n dengue tests$",
  malaria       = "^n malaria tests$",
  n_lab_results = "^Number of Lab Component Results$"
)

# -----------------------------------------------------------------------------
# Shared helpers
# -----------------------------------------------------------------------------
match_measure_labels <- function(labels, patterns) {
  vapply(labels, function(lbl) {
    hit <- names(patterns)[vapply(patterns, function(p) grepl(p, lbl), logical(1))]
    if (length(hit) != 1L) {
      stop(
        sprintf(
          "Unrecognized or ambiguous measure column label in export: '%s' (matched %d pattern%s). Update the measure patterns in ingest.R.",
          lbl, length(hit), if (length(hit) == 1L) "" else "s"
        ),
        call. = FALSE
      )
    }
    hit
  }, character(1), USE.NAMES = FALSE)
}

read_epic_grid <- function(file, password) {
  if (!grepl("\\.xlsx$", file, ignore.case = TRUE)) {
    stop("Expected a password-protected .xlsx SlicerDicer export, got: ", file)
  }
  if (is.null(password) || !nzchar(password)) {
    stop(
      "EPIC_XLSX_PASSWORD is not set, but ", basename(file), " is a ",
      "password-protected SlicerDicer export.\n",
      "Set it in .Renviron via usethis::edit_r_environ()."
    )
  }
  decrypted <- tempfile(fileext = ".xlsx")
  cmd <- sprintf(
    'python -m msoffcrypto -p "%s" "%s" "%s"',
    password, normalizePath(file, winslash = "/"), decrypted
  )
  if (system(cmd) != 0) stop("Decryption failed: ", file)
  on.exit(unlink(decrypted))

  wb <- wb_load(decrypted)
  grid <- wb_to_df(
    wb, sheet = 1, col_names = FALSE,
    skip_empty_rows = FALSE, skip_empty_cols = FALSE
  )
  as.data.frame(lapply(grid, as.character), stringsAsFactors = FALSE)
}

# Locate the single staging file in `dir` and describe it for change detection.
get_staging_state <- function(dir, what) {
  files <- list.files(dir, pattern = "\\.(csv|xlsx)$", full.names = TRUE)
  if (length(files) == 0) {
    stop(
      "No staging files found in ", dir, "/ (", what, ").\n",
      "Export data from Epic Cosmos SlicerDicer and place .xlsx files there."
    )
  }
  if (length(files) > 1) {
    stop(
      "Multiple staging files found in ", dir, "/ (", paste(basename(files), collapse = ", "), ").\n",
      "This ingest expects a single ", what, " export. Remove extras, ",
      "or extend ingest.R to combine multiple exports."
    )
  }
  list(files = files, hashes = unname(tools::md5sum(files)))
}

# Epic suppresses counts of 10 or fewer as the literal string "10 or fewer";
# in these exports most suppressed cells arrive blank instead. Both mean "10 or
# fewer patients".
is_suppressed_count <- function(x) {
  x <- trimws(x)
  is.na(x) | x == "" | x == "-" | x == "10 or fewer"
}
unsuppress_count <- function(x) {
  x <- trimws(x)
  suppressWarnings(as.numeric(ifelse(is_suppressed_count(x), "5", gsub(",", "", x))))
}

# Map a SlicerDicer "State of Residence" label to a geography_name for the FIPS
# join (NA = drop the row).
to_geography_name <- function(state_name) {
  valid_states <- c(state.name, "District of Columbia")
  case_when(
    # National total row. Epic sometimes appends a footnote to "Total"
    # (e.g. "Total: Total includes all data under the Apr bucket,
    # including data from rows not currently displayed.") when a bucket
    # has additional suppressed/hidden rows; match on the prefix since
    # no real state name starts with "Total".
    grepl("^Total(:|$)", state_name) ~ "United States",
    state_name %in% valid_states ~ state_name,
    TRUE ~ NA_character_
  )
}

attach_geography <- function(data_raw) {
  n_before_geo <- nrow(data_raw)
  data_raw$geography_name <- to_geography_name(data_raw$state_name)
  dropped_states <- setdiff(unique(data_raw$state_name[is.na(data_raw$geography_name)]), NA_character_)

  data_raw <- data_raw %>%
    filter(!is.na(geography_name)) %>%
    left_join(state_fips_lookup, by = "geography_name") %>%
    filter(!is.na(geography))

  if (length(dropped_states) > 0) {
    message(
      "Dropped ", n_before_geo - nrow(data_raw), " non-US / catch-all row(s) (state of residence: ",
      paste(sort(dropped_states), collapse = ", "), ")"
    )
  }
  data_raw
}

# Pivot the tagged long data wide: one count column per measure
# (epic_n_<measure>) plus one flag column per measure
# (epic_<measure>_suppressed_flag). Flags record what Epic withheld, so they
# are taken from the raw cells BEFORE imputation.
long_to_wide <- function(data_long) {
  wide_n <- data_long %>%
    select(geography, time, measure, value) %>%
    pivot_wider(names_from = measure, values_from = value, values_fn = sum,
                names_glue = "epic_n_{measure}")

  wide_flag <- data_long %>%
    select(geography, time, measure, suppressed) %>%
    pivot_wider(names_from = measure, values_from = suppressed, values_fn = max,
                names_glue = "epic_{measure}_suppressed_flag")

  left_join(wide_n, wide_flag, by = c("geography", "time"))
}

# =============================================================================
# Monthly by-state exports (any diagnosis, new cases)
# =============================================================================

# Read one monthly crosstab and return the standardized wide data with plain
# disease names (epic_n_<dz>, epic_pct_<dz>, epic_<dz>_suppressed_flag).
standardize_monthly <- function(file, patterns) {
  disease_keys <- setdiff(names(patterns), "n_patients")
  grid <- read_epic_grid(file, xlsx_password)

  # Locate and validate header rows (fails loudly if the session drifts)
  dim_label_row <- which(grid[[3]] == "State of Residence")
  if (length(dim_label_row) != 1) {
    stop("Could not find exactly one 'State of Residence' row-label row; export layout changed.")
  }
  if (!identical(grid[[1]][[dim_label_row]], "Year") || !identical(grid[[2]][[dim_label_row]], "Month")) {
    stop("Expected columns A/B row-dimension labels 'Year'/'Month' on row ", dim_label_row,
         "; export layout changed.")
  }

  measure_label_row <- dim_label_row - 1L
  data_start <- dim_label_row + 1L

  n_cols <- ncol(grid)
  if (n_cols < 4) stop("Unexpected export width (", n_cols, " columns); expected year/month/state plus value columns.")

  value_col_idx <- 4:n_cols
  measure_labels_raw <- trimws(as.character(grid[measure_label_row, value_col_idx]))
  measure_keys <- match_measure_labels(measure_labels_raw, patterns)

  missing_measures <- setdiff(names(patterns), measure_keys)
  if (length(missing_measures) > 0) {
    stop("Expected measure(s) not found in export: ", paste(missing_measures, collapse = ", "))
  }

  col_meta <- data.frame(col_idx = value_col_idx, measure = measure_keys, stringsAsFactors = FALSE)
  message("Measure columns found: ", paste(measure_keys, collapse = ", "))

  # Data rows: fill down merged year/month cells (state is present on every row)
  data_raw <- grid[data_start:nrow(grid), , drop = FALSE]
  colnames(data_raw)[1:3] <- c("year", "month", "state_name")
  colnames(data_raw)[value_col_idx] <- as.character(value_col_idx)

  data_raw <- data_raw %>%
    mutate(
      state_name = iconv(state_name, to = "UTF-8", sub = ""),
      month      = iconv(month,      to = "UTF-8", sub = ""),
      state_name = na_if(trimws(state_name), ""),
      year       = na_if(trimws(year), ""),
      month      = na_if(trimws(month), "")
    ) %>%
    fill(year, month, .direction = "down") %>%
    filter(!is.na(state_name))

  # Drop the trailing partial period (e.g. "Jul 1 - Jul 28")
  is_full_month <- grepl("^[A-Za-z]{3}$", trimws(data_raw$month))
  n_partial <- sum(!is_full_month)
  if (n_partial > 0) {
    message(
      "Dropping ", n_partial, " row(s) from partial period(s): ",
      paste(unique(data_raw$month[!is_full_month]), collapse = ", ")
    )
    if (n_partial == nrow(data_raw)) {
      stop("Every row was classified as a partial period - the month label format probably changed.")
    }
  }
  data_raw <- data_raw[is_full_month, ]

  data_raw$time <- format(
    ceiling_date(as.Date(paste(data_raw$year, data_raw$month, "01"), format = "%Y %b %d"), "month") - days(1),
    "%Y-%m-%d"
  )

  # Geography: state name -> FIPS
  data_raw <- attach_geography(data_raw)

  # Pivot value columns to long, tag with measure
  data_long <- data_raw %>%
    select(geography, time, all_of(as.character(value_col_idx))) %>%
    pivot_longer(cols = all_of(as.character(value_col_idx)), names_to = "col_idx", values_to = "raw_value") %>%
    mutate(col_idx = as.integer(col_idx)) %>%
    left_join(col_meta, by = "col_idx") %>%
    select(-col_idx) %>%
    mutate(
      suppressed = as.integer(is_suppressed_count(raw_value)),
      value = unsuppress_count(raw_value)
    )

  data_clean <- long_to_wide(data_long) %>%
    rename(epic_n_patients = epic_n_n_patients)

  # When the denominator itself was suppressed it has already been imputed to
  # 5, so 5/5*100 would assert a meaningless 100% - leave those cells NA, as
  # in cosmos_gas/cosmos_concussions; epic_n_patients_suppressed_flag marks them.
  for (dz in disease_keys) {
    data_clean[[paste0("epic_pct_", dz)]] <- if_else(
      data_clean$epic_n_patients_suppressed_flag == 1L | data_clean$epic_n_patients == 0,
      NA_real_,
      data_clean[[paste0("epic_n_", dz)]] / data_clean$epic_n_patients * 100
    )
  }

  measure_cols <- unlist(lapply(disease_keys, function(dz) {
    c(paste0("epic_n_", dz), paste0("epic_pct_", dz), paste0("epic_", dz, "_suppressed_flag"))
  }))

  data_clean <- data_clean %>%
    select(
      geography, time,
      all_of(measure_cols),
      epic_n_patients, epic_n_patients_suppressed_flag
    ) %>%
    arrange(geography, time)

  validate_wide(data_clean, disease_keys, denom = "epic_n_patients", monthly = TRUE)
  data_clean
}

# Validation shared by the monthly and lab outputs (works on plain disease
# names, before any renaming). `denom` is the denominator column.
validate_wide <- function(data_clean, disease_keys, denom, monthly,
                          n_prefix = "epic_n_", pct_prefix = "epic_pct_",
                          flag_infix = "epic_") {
  denom_flag <- paste0(denom, "_suppressed_flag")

  dupes <- data_clean %>%
    count(geography, time) %>%
    filter(n > 1)
  if (nrow(dupes) > 0) {
    stop("Duplicate rows per geography/time (", nrow(dupes), " combinations). ",
         "Check for overlapping staging files.")
  }

  stopifnot(
    # Geography: FIPS strings, national is "00"
    all(nchar(data_clean$geography) == 2),
    "00" %in% data_clean$geography,
    # Time: YYYY-mm-dd
    all(grepl("^\\d{4}-\\d{2}-\\d{2}$", data_clean$time)),
    # Denominator is present and non-negative
    !any(is.na(data_clean[[denom]])),
    all(data_clean[[denom]] >= 0),
    all(data_clean[[denom_flag]] %in% c(0L, 1L)),
    all(data_clean[[denom]][data_clean[[denom_flag]] == 1L] == 5)
  )
  if (monthly) {
    stopifnot(all(as.Date(data_clean$time) == ceiling_date(as.Date(data_clean$time), "month") - days(1)))
  }

  for (dz in disease_keys) {
    n_col <- data_clean[[paste0(n_prefix, dz)]]
    pct_col <- data_clean[[paste0(pct_prefix, dz)]]
    flag_col <- data_clean[[paste0(flag_infix, dz, "_suppressed_flag")]]
    stopifnot(
      !any(is.na(n_col)),
      all(n_col >= 0),
      all(pct_col >= 0 & pct_col <= 100, na.rm = TRUE),
      all(flag_col %in% c(0L, 1L)),
      all(n_col[flag_col == 1L] == 5),
      identical(is.na(pct_col), data_clean[[denom_flag]] == 1L)
    )
  }
}

report_monthly <- function(data_clean, disease_keys, denom) {
  message(
    "Standardized ", nrow(data_clean), " rows | ",
    length(unique(data_clean$geography)), " geographies | ",
    min(data_clean$time), " to ", max(data_clean$time)
  )
  for (dz in disease_keys) {
    message("  epic_", dz, "_suppressed_flag: ",
            sum(data_clean[[paste0("epic_", dz, "_suppressed_flag")]]), " suppressed/imputed")
  }
  message("  ", denom, "_suppressed_flag: ", sum(data_clean[[paste0(denom, "_suppressed_flag")]]),
          " suppressed/imputed")
}

if (!dir.exists("standard")) dir.create("standard")

# -----------------------------------------------------------------------------
# 1. Any diagnosis (raw/staging/*.xlsx) -> standard/data.csv.gz
# -----------------------------------------------------------------------------
current_state <- get_staging_state("raw/staging", "vector-borne disease crosstab")

if (!identical(process$raw_state, current_state)) {
  message("== Any-diagnosis export: ", basename(current_state$files))
  data_any <- standardize_monthly(current_state$files[[1]], MEASURE_PATTERNS)
  report_monthly(data_any, setdiff(names(MEASURE_PATTERNS), "n_patients"), "epic_n_patients")

  vroom::vroom_write(data_any, "standard/data.csv.gz", delim = ",")

  process$raw_state <- current_state
  dcf::dcf_process_record(updated = process)
}

# -----------------------------------------------------------------------------
# 2. New cases (raw/staging/new_cases/*.xlsx) -> standard/data_new_cases.csv.gz
# -----------------------------------------------------------------------------
new_cases_state <- get_staging_state("raw/staging/new_cases", "new-cases crosstab")

if (!identical(process$new_cases_state, new_cases_state)) {
  message("== New-cases export: ", basename(new_cases_state$files))
  data_new <- standardize_monthly(new_cases_state$files[[1]], NEW_CASE_PATTERNS)
  new_keys <- setdiff(names(NEW_CASE_PATTERNS), "n_patients")
  report_monthly(data_new, new_keys, "epic_n_patients")

  # Rename disease columns to the `new_` infix (validated above under the
  # plain names shared with the any-diagnosis logic).
  for (dz in new_keys) {
    names(data_new)[names(data_new) == paste0("epic_n_", dz)] <- paste0("epic_n_new_", dz)
    names(data_new)[names(data_new) == paste0("epic_pct_", dz)] <- paste0("epic_pct_new_", dz)
    names(data_new)[names(data_new) == paste0("epic_", dz, "_suppressed_flag")] <- paste0("epic_new_", dz, "_suppressed_flag")
  }

  vroom::vroom_write(data_new, "standard/data_new_cases.csv.gz", delim = ",")

  process$new_cases_state <- new_cases_state
  dcf::dcf_process_record(updated = process)
}

# -----------------------------------------------------------------------------
# 3. Lab tests (raw/staging/lab_tests/*.xlsx) -> standard/data_lab_tests.csv.gz
# -----------------------------------------------------------------------------
lab_state <- get_staging_state("raw/staging/lab_tests", "lab component results crosstab")

if (!identical(process$lab_state, lab_state)) {
  message("== Lab-tests export: ", basename(lab_state$files))
  lab_keys <- setdiff(names(LAB_PATTERNS), "n_lab_results")
  grid <- read_epic_grid(lab_state$files[[1]], xlsx_password)

  # Header rows: four-level crosstab, states are rows. Row 11 holds the measure
  # label (only on the first column of each measure group), row 12 the year
  # (first column of each year within a group), row 13 the month of each
  # column, row 14 "State of Residence". Measure and year labels are carried
  # forward across the columns they span. The trailing partial month (e.g.
  # "Sep 1 - Sep 8") is dropped.
  dim_label_row <- which(grid[[1]] == "State of Residence")
  if (length(dim_label_row) != 1) {
    stop("Could not find exactly one 'State of Residence' row-label row; export layout changed.")
  }
  month_row <- dim_label_row - 1L
  year_row <- dim_label_row - 2L
  measure_label_row <- dim_label_row - 3L
  data_start <- dim_label_row + 1L
  if (ncol(grid) < 3) stop("Unexpected export width (", ncol(grid), " columns); expected state plus value columns.")

  value_col_idx <- 2:ncol(grid)
  carry <- function(row) {
    x <- na_if(trimws(as.character(unlist(grid[row, value_col_idx]))), "")
    if (is.na(x[[1]])) stop("First value column has no label on row ", row, "; export layout changed.")
    fill(data.frame(v = x), v, .direction = "down")$v
  }
  measure_keys <- match_measure_labels(carry(measure_label_row), LAB_PATTERNS)
  missing_measures <- setdiff(names(LAB_PATTERNS), measure_keys)
  if (length(missing_measures) > 0) {
    stop("Expected measure(s) not found in export: ", paste(missing_measures, collapse = ", "))
  }
  year_raw <- carry(year_row)
  if (!all(grepl("^\\d{4}$", year_raw))) stop("Unexpected year label(s) on row ", year_row, ": ",
                                              paste(setdiff(unique(year_raw), grep("^\\d{4}$", year_raw, value = TRUE)), collapse = ", "))
  month_raw <- trimws(as.character(unlist(grid[month_row, value_col_idx])))
  is_full_month <- grepl("^[A-Za-z]{3}$", month_raw)
  if (!any(is_full_month)) stop("No full-month columns found on row ", month_row, "; export layout changed.")
  message(
    "Dropping ", sum(!is_full_month), " column(s) from partial period(s): ",
    paste(unique(month_raw[!is_full_month]), collapse = ", ")
  )
  month_end <- ceiling_date(as.Date(paste(year_raw, month_raw, "01"), format = "%Y %b %d"), "month") - days(1)
  col_meta <- data.frame(
    col_idx = value_col_idx, measure = measure_keys,
    time = ifelse(is_full_month, format(month_end, "%Y-%m-%d"), NA_character_),
    stringsAsFactors = FALSE
  ) %>% filter(!is.na(time))
  message("Measure columns found: ", paste(unique(measure_keys), collapse = ", "),
          " | months: ", min(col_meta$time), " to ", max(col_meta$time))

  data_raw <- grid[data_start:nrow(grid), , drop = FALSE]
  colnames(data_raw)[1] <- "state_name"
  colnames(data_raw)[value_col_idx] <- as.character(value_col_idx)
  data_raw <- data_raw %>%
    mutate(state_name = na_if(trimws(iconv(state_name, to = "UTF-8", sub = "")), "")) %>%
    filter(!is.na(state_name))

  data_raw <- attach_geography(data_raw)

  data_long <- data_raw %>%
    select(geography, all_of(as.character(col_meta$col_idx))) %>%
    pivot_longer(cols = -geography, names_to = "col_idx", values_to = "raw_value") %>%
    mutate(col_idx = as.integer(col_idx)) %>%
    left_join(col_meta, by = "col_idx") %>%
    select(-col_idx) %>%
    mutate(
      suppressed = as.integer(is_suppressed_count(raw_value)),
      value = unsuppress_count(raw_value)
    )

  data_lab <- long_to_wide(data_long) %>%
    rename(epic_n_lab_results = epic_n_n_lab_results)

  # Percent of all lab component results; NA where the denominator was
  # suppressed (see standardize_monthly).
  for (dz in lab_keys) {
    data_lab[[paste0("epic_pct_", dz)]] <- if_else(
      data_lab$epic_n_lab_results_suppressed_flag == 1L | data_lab$epic_n_lab_results == 0,
      NA_real_,
      data_lab[[paste0("epic_n_", dz)]] / data_lab$epic_n_lab_results * 100
    )
  }

  measure_cols <- unlist(lapply(lab_keys, function(dz) {
    c(paste0("epic_n_", dz), paste0("epic_pct_", dz), paste0("epic_", dz, "_suppressed_flag"))
  }))
  data_lab <- data_lab %>%
    select(geography, time, all_of(measure_cols),
           epic_n_lab_results, epic_n_lab_results_suppressed_flag) %>%
    arrange(geography, time)

  validate_wide(data_lab, lab_keys, denom = "epic_n_lab_results", monthly = TRUE)
  report_monthly(data_lab, lab_keys, "epic_n_lab_results")

  # Rename disease columns to the `tests_` infix
  for (dz in lab_keys) {
    names(data_lab)[names(data_lab) == paste0("epic_n_", dz)] <- paste0("epic_n_tests_", dz)
    names(data_lab)[names(data_lab) == paste0("epic_pct_", dz)] <- paste0("epic_pct_tests_", dz)
    names(data_lab)[names(data_lab) == paste0("epic_", dz, "_suppressed_flag")] <- paste0("epic_tests_", dz, "_suppressed_flag")
  }

  vroom::vroom_write(data_lab, "standard/data_lab_tests.csv.gz", delim = ",")

  process$lab_state <- lab_state
  dcf::dcf_process_record(updated = process)
}

# -----------------------------------------------------------------------------
# 4. New cases, pop filter (raw/staging/new_cases_pop_filter/*.xlsx)
#    -> standard/data_new_cases_pop_filter.csv.gz
# -----------------------------------------------------------------------------
pf_state <- get_staging_state("raw/staging/new_cases_pop_filter", "new-cases pop filter crosstab")

if (!identical(process$new_cases_pop_filter_state, pf_state)) {
  message("== New-cases pop-filter export: ", basename(pf_state$files))
  pf_keys <- setdiff(names(POP_FILTER_PATTERNS), "n_patients")
  data_pf <- standardize_monthly(pf_state$files[[1]], POP_FILTER_PATTERNS)
  report_monthly(data_pf, pf_keys, "epic_n_patients")

  # Percents here are shares of the FILTERED population (epic_n_patients is the
  # number of patients with a new diagnosis of any of the six diseases, not all
  # patients). Rename to the `new_popfilter_` infix.
  for (dz in pf_keys) {
    names(data_pf)[names(data_pf) == paste0("epic_n_", dz)] <- paste0("epic_n_new_popfilter_", dz)
    names(data_pf)[names(data_pf) == paste0("epic_pct_", dz)] <- paste0("epic_pct_new_popfilter_", dz)
    names(data_pf)[names(data_pf) == paste0("epic_", dz, "_suppressed_flag")] <- paste0("epic_new_popfilter_", dz, "_suppressed_flag")
  }
  names(data_pf)[names(data_pf) == "epic_n_patients"] <- "epic_n_new_popfilter_patients"
  names(data_pf)[names(data_pf) == "epic_n_patients_suppressed_flag"] <- "epic_n_new_popfilter_patients_suppressed_flag"

  vroom::vroom_write(data_pf, "standard/data_new_cases_pop_filter.csv.gz", delim = ",")

  process$new_cases_pop_filter_state <- pf_state
  dcf::dcf_process_record(updated = process)
}
