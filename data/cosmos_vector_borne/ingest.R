# =============================================================================
# Epic Cosmos Vector-Borne Disease Data Ingestion
# Source: Epic Cosmos SlicerDicer exports (raw/staging/**/*.xlsx), two
# exports, two standardized outputs:
#
#   1. raw/staging/lab_tests/*.xlsx  -> standard/data_lab_tests.csv.gz
#      MONTHLY lab component results for each pathogen (babesia, RMSF, West
#      Nile, dengue, malaria), by state. SlicerDicer session 2863670, "Number
#      of Lab Component Results and n west nile tests and n babesia and n RMSF
#      tests and n malaria tests and n dengue tests by State of Residence".
#        Data model      : Lab Component Results
#        Population base : All Lab Component Results
#        Criteria        : Country of Care = United States of America
#      Time is monthly (columns are measure > year > month, states are rows);
#      time = last day of month. The trailing partial month ("Sep 1 - Sep 8")
#      is dropped, as in the other monthly exports.
#
#   2. raw/staging/new_cases/*.xlsx  -> standard/data_new_cases.csv.gz
#      MONTHLY new-case counts (Year > Month > State) for Lyme, Babesiosis,
#      Malaria, RMSF, West Nile and Dengue, where the population itself is
#      filtered: only patients with a first diagnosis of one of the diseases
#      (ICD-10 based, "Not Preceded By" the same diagnosis within 6 months),
#      Country of Care = United States, Has Any Encounters. SlicerDicer
#      session 2863672. "Number of Patients" is therefore the number of
#      patients in that filtered population, NOT all patients, so percents are
#      shares of patients with a new vector-borne diagnosis. Columns carry a
#      `new_` infix (epic_n_new_lyme, ..., epic_n_new_patients).
#
# Each export is tracked with its own key in process.json (lab_state,
# new_cases_state) and only reprocessed when its files change.
#
# Raw layout of the new-cases export (rows, 1-indexed as in the spreadsheet):
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
#   12    : year (first column of each year within a measure group)
#   13    : month of each column (last one is the partial month)
#   14    : A: State of Residence
#   15+   : one row per state, plus "None of the above" and "Total"
#
# Output (PopHIVE wide format):
#   index   : geography (FIPS string, "00" = national), time
#   new cases: epic_n_new_<disease>, epic_pct_new_<disease>,
#     epic_new_<disease>_suppressed_flag (the flag covers both; the percent is
#     derived from the same numerator cell); denominator epic_n_new_patients ->
#     epic_n_new_patients_suppressed_flag
#   lab tests: epic_n_tests_<disease>, epic_pct_tests_<disease>,
#     epic_tests_<disease>_suppressed_flag, denominator epic_n_lab_results ->
#     epic_n_lab_results_suppressed_flag
#
# Conventions applied here:
#   - time is the LAST day of the month, formatted YYYY-mm-dd; the trailing
#     partial period at the end of the exported range (e.g. "Jul 1 - Jul 28")
#     is dropped
#   - state_name is resolved to a FIPS `geography` and then dropped
#   - suppression is handled per measure (blank / "10 or fewer" -> 5, flag 1)
#   - each disease measure is a PERCENT of its denominator (the filtered
#     new-case population, or all lab component results), not a rate per
#     100,000
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
NEW_CASE_PATTERNS <- c(
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
# Monthly by-state export (new cases)
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
# 1. Lab tests (raw/staging/lab_tests/*.xlsx) -> standard/data_lab_tests.csv.gz
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
# 2. New cases (raw/staging/new_cases/*.xlsx) -> standard/data_new_cases.csv.gz
# -----------------------------------------------------------------------------
new_cases_state <- get_staging_state("raw/staging/new_cases", "new-cases crosstab")

if (!identical(process$new_cases_state, new_cases_state)) {
  message("== New-cases export: ", basename(new_cases_state$files))
  new_keys <- setdiff(names(NEW_CASE_PATTERNS), "n_patients")
  data_new <- standardize_monthly(new_cases_state$files[[1]], NEW_CASE_PATTERNS)
  report_monthly(data_new, new_keys, "epic_n_patients")

  # Percents here are shares of the FILTERED population (epic_n_patients is the
  # number of patients with a new diagnosis of any of the six diseases, not all
  # patients). Rename to the `new_` infix.
  for (dz in new_keys) {
    names(data_new)[names(data_new) == paste0("epic_n_", dz)] <- paste0("epic_n_new_", dz)
    names(data_new)[names(data_new) == paste0("epic_pct_", dz)] <- paste0("epic_pct_new_", dz)
    names(data_new)[names(data_new) == paste0("epic_", dz, "_suppressed_flag")] <- paste0("epic_new_", dz, "_suppressed_flag")
  }
  names(data_new)[names(data_new) == "epic_n_patients"] <- "epic_n_new_patients"
  names(data_new)[names(data_new) == "epic_n_patients_suppressed_flag"] <- "epic_n_new_patients_suppressed_flag"

  vroom::vroom_write(data_new, "standard/data_new_cases.csv.gz", delim = ",")

  process$new_cases_state <- new_cases_state
  dcf::dcf_process_record(updated = process)
}

# -----------------------------------------------------------------------------
# 3. West Nile new cases by age (raw/staging/west_nile_age/*.xlsx)
#    -> standard/data_west_nile_age.csv.gz
#
# SlicerDicer session 2872838, "n west nile and west nile and Number of Patients
# by Patient Age in Years (Current) Range and State of Residence and Diagnosis
# (All)", by Year. Data model Patients, population base All Patients, criteria:
# first West Nile diagnosis (A92.3, not preceded by A92.3 within 6 months),
# Country of Care = United States of America, Has Any Encounters. Session date
# range 1/1/2018 - 10/10/2026 -> ANNUAL data, time = YYYY-12-31; the trailing
# partial year ("Jan 1 - Oct 10 2026") is dropped.
# Raw layout: row 11 measure labels (E:G); row 12 dimension labels (A: year,
# B: age range, C: state, D: diagnosis group); 13+ data rows ordered
# Year > Age > State > Diagnosis. Year, age and state are merged cells (fill down).
# `n west nile` and `Number of Patients` are identical (the population is
# already the West Nile patients) and `west nile (%)` is always 100%, so only
# the count is kept, one column per diagnosis group. A second export (all
# patients by year/age/state) supplies the denominator epic_n_patients.
# -----------------------------------------------------------------------------
WNV_AGE_LABELS <- c(
  "Less than 40"      = "<40",
  "≥ 40 and < 70" = "40-69",
  "70 or more"        = "70+",
  "Total"             = "Overall"
)
WNV_DX_LABELS <- c(
  "Total"             = "total",
  "Neuroinvasive"     = "neuroinvasive",
  "Non-neuroinvasive" = "non_neuroinvasive"
)
WNV_MEASURE_PATTERNS <- c(
  n_west_nile = "^n west nile$",
  pct         = "^west nile \\(%\\)$",
  n_patients  = "^Number of Patients$"
)

# Companion denominator export, raw/staging/west_nile_age_patients/*.xlsx:
# SlicerDicer session 2875932, "Number of Patients by Patient Age in Years
# (Current) Range and State of Residence" (data model Patients, population base
# All Patients, criteria Country of Care = United States of America AND Has Any
# Encounters; same date range). Rows Year > Age > State, one value column
# (A: year, B: age range, C: state, D: Number of Patients). It is the number of
# ALL patients per year/age/state, joined on as `epic_n_patients` (+ flag).
wnv_age_state <- list(
  age = get_staging_state("raw/staging/west_nile_age", "West Nile by age crosstab"),
  patients = get_staging_state("raw/staging/west_nile_age_patients", "all-patients by age crosstab")
)

if (!identical(process$wnv_age_state, wnv_age_state)) {
  message("== West Nile by age export: ", basename(wnv_age_state$age$files))
  grid <- read_epic_grid(wnv_age_state$age$files[[1]], xlsx_password)

  dim_label_row <- which(grid[[3]] == "State of Residence")
  if (length(dim_label_row) != 1 ||
      !identical(grid[[1]][[dim_label_row]], "Year") ||
      !grepl("^Patient Age in Years", grid[[2]][[dim_label_row]]) ||
      !grepl("^Diagnosis", grid[[4]][[dim_label_row]])) {
    stop("Expected row labels 'Year' / 'Patient Age in Years' / 'State of Residence' / 'Diagnosis' on one row; export layout changed.")
  }
  measure_keys <- match_measure_labels(
    trimws(as.character(unlist(grid[dim_label_row - 1L, 5:ncol(grid)]))), WNV_MEASURE_PATTERNS
  )
  if (!setequal(measure_keys, names(WNV_MEASURE_PATTERNS)) || length(measure_keys) != 3L) {
    stop("Expected exactly the measures ", paste(names(WNV_MEASURE_PATTERNS), collapse = ", "), "; export layout changed.")
  }
  col_n <- 4L + which(measure_keys == "n_west_nile")
  col_pat <- 4L + which(measure_keys == "n_patients")

  data_raw <- grid[(dim_label_row + 1L):nrow(grid), , drop = FALSE]
  data_raw <- data.frame(
    year = data_raw[[1]], age_raw = data_raw[[2]], state_name = data_raw[[3]], dx_raw = data_raw[[4]],
    raw_value = data_raw[[col_n]], raw_patients = data_raw[[col_pat]],
    stringsAsFactors = FALSE
  ) %>%
    mutate(
      across(c(year, age_raw, state_name, dx_raw), ~ na_if(trimws(iconv(.x, to = "UTF-8", sub = "")), ""))
    ) %>%
    fill(year, age_raw, state_name, .direction = "down") %>%
    filter(!is.na(dx_raw))

  # Annual time: keep full calendar years only; drop the trailing partial year
  # (e.g. "Jan 1 - Oct 10 2026") and any year-level total row.
  is_full_year <- grepl("^\\d{4}$", data_raw$year)
  if (any(!is_full_year)) {
    message("Dropping ", sum(!is_full_year), " row(s) from partial/total year(s): ",
            paste(unique(data_raw$year[!is_full_year]), collapse = ", "))
  }
  data_raw <- data_raw[is_full_year, ]
  if (nrow(data_raw) == 0) stop("No full-year rows found; the year label format probably changed.")
  data_raw$time <- paste0(data_raw$year, "-12-31")

  # "Total: Total includes all data ..." footnotes are appended to total labels
  data_raw <- data_raw %>%
    mutate(
      age_raw = if_else(grepl("^Total(:|$)", age_raw), "Total", age_raw),
      dx_raw  = if_else(grepl("^Total(:|$)", dx_raw), "Total", dx_raw)
    )

  unknown_age <- setdiff(unique(data_raw$age_raw), names(WNV_AGE_LABELS))
  if (length(unknown_age) > 0) {
    stop("Unrecognized age group(s): ", paste(unknown_age, collapse = ", "), ". Update WNV_AGE_LABELS in ingest.R.")
  }
  unknown_dx <- setdiff(unique(data_raw$dx_raw), c(names(WNV_DX_LABELS), "None of the above"))
  if (length(unknown_dx) > 0) {
    stop("Unrecognized diagnosis group(s): ", paste(unknown_dx, collapse = ", "), ". Update WNV_DX_LABELS in ingest.R.")
  }
  n_none <- sum(data_raw$dx_raw == "None of the above")
  message("Dropping ", n_none, " 'None of the above' diagnosis row(s)")
  data_raw <- data_raw %>% filter(dx_raw != "None of the above")

  data_raw <- attach_geography(data_raw)

  data_long <- data_raw %>%
    mutate(
      age = unname(WNV_AGE_LABELS[age_raw]),
      dx = unname(WNV_DX_LABELS[dx_raw]),
      suppressed = as.integer(is_suppressed_count(raw_value)),
      value = unsuppress_count(raw_value)
    )

  # n west nile and Number of Patients must agree wherever both are observed
  both <- !is_suppressed_count(data_long$raw_value) & !is_suppressed_count(data_long$raw_patients)
  stopifnot(all(as.numeric(data_long$raw_value[both]) == as.numeric(gsub(",", "", data_long$raw_patients[both]))))

  wide_n <- data_long %>%
    select(geography, time, age, dx, value) %>%
    pivot_wider(names_from = dx, values_from = value, names_glue = "epic_n_west_nile_new_{dx}")
  wide_flag <- data_long %>%
    select(geography, time, age, dx, suppressed) %>%
    pivot_wider(names_from = dx, values_from = suppressed, names_glue = "epic_west_nile_new_{dx}_suppressed_flag")

  # All-patients denominator (Year > Age > State; same labels as above)
  message("== All-patients by age export: ", basename(wnv_age_state$patients$files))
  pgrid <- read_epic_grid(wnv_age_state$patients$files[[1]], xlsx_password)
  p_label_row <- which(pgrid[[3]] == "State of Residence")
  if (length(p_label_row) != 1 || ncol(pgrid) != 4 ||
      !identical(pgrid[[1]][[p_label_row]], "Year") ||
      !grepl("^Patient Age in Years", pgrid[[2]][[p_label_row]]) ||
      !identical(trimws(pgrid[[4]][[p_label_row - 1L]]), "Number of Patients")) {
    stop("Expected 'Year' / 'Patient Age in Years' / 'State of Residence' row labels and a single 'Number of Patients' column in the all-patients export; export layout changed.")
  }
  data_pat <- pgrid[(p_label_row + 1L):nrow(pgrid), , drop = FALSE]
  data_pat <- data.frame(
    year = data_pat[[1]], age_raw = data_pat[[2]], state_name = data_pat[[3]], raw_patients = data_pat[[4]],
    stringsAsFactors = FALSE
  ) %>%
    mutate(across(c(year, age_raw, state_name), ~ na_if(trimws(iconv(.x, to = "UTF-8", sub = "")), ""))) %>%
    fill(year, age_raw, .direction = "down") %>%
    filter(!is.na(state_name), grepl("^\\d{4}$", year)) %>%
    mutate(
      time = paste0(year, "-12-31"),
      age_raw = if_else(grepl("^Total(:|$)", age_raw), "Total", age_raw)
    )
  unknown_age_p <- setdiff(unique(data_pat$age_raw), names(WNV_AGE_LABELS))
  if (length(unknown_age_p) > 0) {
    stop("Unrecognized age group(s) in all-patients export: ", paste(unknown_age_p, collapse = ", "),
         ". Update WNV_AGE_LABELS in ingest.R.")
  }
  data_pat <- attach_geography(data_pat) %>%
    mutate(
      age = unname(WNV_AGE_LABELS[age_raw]),
      epic_n_patients = unsuppress_count(raw_patients),
      epic_n_patients_suppressed_flag = as.integer(is_suppressed_count(raw_patients))
    ) %>%
    select(geography, time, age, epic_n_patients, epic_n_patients_suppressed_flag)
  dupes_p <- data_pat %>% count(geography, time, age) %>% filter(n > 1)
  if (nrow(dupes_p) > 0) stop("Duplicate rows per geography/time/age in all-patients export.")

  dx_keys <- unname(WNV_DX_LABELS)
  data_wnv <- left_join(wide_n, wide_flag, by = c("geography", "time", "age")) %>%
    left_join(data_pat, by = c("geography", "time", "age")) %>%
    select(geography, time, age,
           all_of(unlist(lapply(dx_keys, function(k) {
             c(paste0("epic_n_west_nile_new_", k), paste0("epic_west_nile_new_", k, "_suppressed_flag"))
           }))),
           epic_n_patients, epic_n_patients_suppressed_flag) %>%
    arrange(geography, time, age)
  if (anyNA(data_wnv$epic_n_patients)) {
    stop(sum(is.na(data_wnv$epic_n_patients)), " West Nile row(s) have no matching all-patients denominator.")
  }
  stopifnot(
    all(data_wnv$epic_n_patients >= 0),
    all(data_wnv$epic_n_patients_suppressed_flag %in% c(0L, 1L)),
    all(data_wnv$epic_n_patients[data_wnv$epic_n_patients_suppressed_flag == 1L] == 5)
  )

  # Validation
  dupes <- data_wnv %>% count(geography, time, age) %>% filter(n > 1)
  if (nrow(dupes) > 0) stop("Duplicate rows per geography/time/age (", nrow(dupes), " combinations).")
  stopifnot(
    all(nchar(data_wnv$geography) == 2),
    "00" %in% data_wnv$geography,
    all(grepl("^\\d{4}-12-31$", data_wnv$time)),
    setequal(data_wnv$age, unname(WNV_AGE_LABELS))
  )
  for (k in dx_keys) {
    n_col <- data_wnv[[paste0("epic_n_west_nile_new_", k)]]
    flag_col <- data_wnv[[paste0("epic_west_nile_new_", k, "_suppressed_flag")]]
    stopifnot(
      !any(is.na(n_col)), all(n_col >= 0),
      all(flag_col %in% c(0L, 1L)),
      all(n_col[flag_col == 1L] == 5)
    )
    message("  epic_west_nile_new_", k, "_suppressed_flag: ", sum(flag_col), " suppressed/imputed")
  }
  message(
    "Standardized ", nrow(data_wnv), " rows | ", length(unique(data_wnv$geography)),
    " geographies | ", length(unique(data_wnv$age)), " age groups | time ", unique(data_wnv$time)
  )

  vroom::vroom_write(data_wnv, "standard/data_west_nile_age.csv.gz", delim = ",")

  process$wnv_age_state <- wnv_age_state
  dcf::dcf_process_record(updated = process)
}


