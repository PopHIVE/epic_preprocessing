# =============================================================================
# Epic Cosmos Vector-Borne Disease Ingestion (Travel History Excluded)
# Source: Epic Cosmos SlicerDicer export (raw/staging/*.xlsx)
#         Monthly, NATIONAL-ONLY patient counts with a vector-borne disease
#         diagnosis, for the same six diseases as cosmos_vector_borne, but
#         restricted to patients WITHOUT a travel history. Intended as a
#         sensitivity comparison against cosmos_vector_borne to see how much
#         of the Malaria/Dengue/RMSF signal there is travel-associated.
#
# SlicerDicer session 2852825, "n babesiosis and n malaria and n RMSF and n
# west nile and n dengue and Number of Patients and n lyme":
#   Data model      : Patients
#   Population base : All Patients
#   Criteria        : Country of Care = United States of America,
#                      Not Travel History (New User Grouper 1),
#                      Has Any Encounters
#   Measures        : n lyme, n babesiosis, n malaria, n RMSF, n west nile,
#                      n dengue (numerators), Number of Patients (denominator)
#
# This export has NO "State of Residence" stratification - every row is
# national, so geography is hardcoded to "00" rather than resolved from a
# state name. If a future export adds a state breakdown, that is a structure
# change (see the ingest-source skill, Path B) - model it on
# cosmos_vector_borne instead of silently keeping geography = "00".
#
# Raw export layout (rows, 1-indexed as in the spreadsheet):
#   1-8   : session metadata
#   11    : measure labels for the value columns (row: <NA>, "Measures",
#           "n babesiosis", "n malaria", "n RMSF", "n west nile", "n dengue",
#           "Number of Patients", "n lyme")
#   12    : row-dimension labels (A: Year, B: Month; no third column)
#   13+   : data rows, ordered Year > Month. Year is a merged cell (blank
#           until the next value) and must be filled down; Month is present
#           on every row.
#
# Output (PopHIVE wide format, standard/data.csv.gz):
#   index   : geography (always "00"), time
#   measures, one triplet per disease:
#     epic_n_<disease>, epic_pct_<disease> -> epic_<disease>_suppressed_flag
#       (the flag covers both; the percent is derived from the same
#       numerator cell)
#   denominator: epic_n_patients -> epic_n_patients_suppressed_flag
#
# Conventions applied here (see cosmos_vector_borne for the state-level
# counterpart with the unrestricted population base):
#   - time is the LAST day of the month, formatted YYYY-mm-dd; the trailing
#     partial period at the end of the exported range (e.g. "Jul 1 - Jul 28")
#     is dropped
#   - suppression is handled per measure (blank / "10 or fewer" -> 5, flag 1)
#   - each disease measure is a PERCENT of patients meeting this session's
#     population base, not a rate per 100,000, and NOT directly comparable to
#     cosmos_vector_borne's percent because the denominator criteria differ
#     (see README)
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

# Initialize process record
if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

# Password for xlsx files (set in .Renviron via usethis::edit_r_environ())
xlsx_password <- Sys.getenv("EPIC_XLSX_PASSWORD")

# The stable part of each measure column label, matched against row 11.
# Unrecognized/ambiguous labels stop the run instead of silently landing on
# the wrong column - extend this map when the session changes.
MEASURE_PATTERNS <- c(
  lyme        = "^n lyme$",
  babesiosis  = "^n babesiosis$",
  malaria     = "^n malaria$",
  rmsf        = "^n RMSF$",
  west_nile   = "^n west nile$",
  dengue      = "^n dengue$",
  n_patients  = "^Number of Patients$"
)
DISEASE_KEYS <- setdiff(names(MEASURE_PATTERNS), "n_patients")

match_measure_labels <- function(labels, patterns) {
  vapply(labels, function(lbl) {
    hit <- names(patterns)[vapply(patterns, function(p) grepl(p, lbl), logical(1))]
    if (length(hit) != 1L) {
      stop(
        sprintf(
          "Unrecognized or ambiguous measure column label in export: '%s' (matched %d pattern%s). Update MEASURE_PATTERNS in ingest.R.",
          lbl, length(hit), if (length(hit) == 1L) "" else "s"
        ),
        call. = FALSE
      )
    }
    hit
  }, character(1), USE.NAMES = FALSE)
}

# =============================================================================
# 1. Locate staging files & detect change
# =============================================================================

staging_files <- list.files("raw/staging", pattern = "\\.(csv|xlsx)$", full.names = TRUE)
if (length(staging_files) == 0) {
  stop(
    "No staging files found in raw/staging/.\n",
    "Export data from Epic Cosmos SlicerDicer and place .xlsx files there."
  )
}
if (length(staging_files) > 1) {
  stop(
    "Multiple staging files found (", paste(basename(staging_files), collapse = ", "), ").\n",
    "This ingest expects a single national vector-borne disease crosstab export. Remove ",
    "extras from raw/staging/, or extend ingest.R to combine multiple exports."
  )
}

current_state <- list(
  files = staging_files,
  hashes = unname(tools::md5sum(staging_files))
)

if (!identical(process$raw_state, current_state)) {

  # ---------------------------------------------------------------------------
  # 2. Decrypt and read the raw grid
  # ---------------------------------------------------------------------------
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

  grid <- read_epic_grid(staging_files[[1]], xlsx_password)

  # ---------------------------------------------------------------------------
  # 3. Locate and validate header rows (fails loudly if the session drifts)
  # ---------------------------------------------------------------------------
  dim_label_row <- which(grid[[1]] == "Year" & grid[[2]] == "Month")
  if (length(dim_label_row) != 1) {
    stop("Could not find exactly one row with columns A/B = 'Year'/'Month'; export layout changed.")
  }
  if (ncol(grid) >= 3 && !is.na(grid[[3]][[dim_label_row]]) && nzchar(trimws(grid[[3]][[dim_label_row]]))) {
    stop(
      "Column C on the dimension-label row is '", grid[[3]][[dim_label_row]], "', but this ",
      "ingest expects a national-only export (Year, Month only, no geography stratification). ",
      "If the session now includes a 'State of Residence' breakdown, model this ingest on ",
      "cosmos_vector_borne instead."
    )
  }

  measure_label_row <- dim_label_row - 1L
  data_start <- dim_label_row + 1L

  n_cols <- ncol(grid)
  if (n_cols < 3) stop("Unexpected export width (", n_cols, " columns); expected year/month plus value columns.")

  value_col_idx <- 3:n_cols
  measure_labels_raw <- trimws(as.character(grid[measure_label_row, value_col_idx]))
  measure_keys <- match_measure_labels(measure_labels_raw, MEASURE_PATTERNS)

  missing_measures <- setdiff(names(MEASURE_PATTERNS), measure_keys)
  if (length(missing_measures) > 0) {
    stop("Expected measure(s) not found in export: ", paste(missing_measures, collapse = ", "))
  }

  col_meta <- data.frame(
    col_idx = value_col_idx,
    measure = measure_keys,
    stringsAsFactors = FALSE
  )

  message("Measure columns found: ", paste(measure_keys, collapse = ", "))

  # ---------------------------------------------------------------------------
  # 4. Data rows: fill down the merged Year cell (Month is present every row)
  # ---------------------------------------------------------------------------
  data_raw <- grid[data_start:nrow(grid), , drop = FALSE]
  colnames(data_raw)[1:2] <- c("year", "month")
  colnames(data_raw)[value_col_idx] <- as.character(value_col_idx)

  data_raw <- data_raw %>%
    mutate(
      month = iconv(month, to = "UTF-8", sub = ""),
      year  = na_if(trimws(year), ""),
      month = na_if(trimws(month), "")
    ) %>%
    fill(year, month, .direction = "down") %>%
    filter(!is.na(month))

  # --- Drop the trailing partial period (e.g. "Jul 1 - Jul 28") ---
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
  data_raw$geography <- "00"

  # ---------------------------------------------------------------------------
  # 5. Pivot value columns to long, tag with measure
  # ---------------------------------------------------------------------------
  data_long <- data_raw %>%
    select(geography, time, all_of(as.character(value_col_idx))) %>%
    pivot_longer(cols = all_of(as.character(value_col_idx)), names_to = "col_idx", values_to = "raw_value") %>%
    mutate(col_idx = as.integer(col_idx)) %>%
    left_join(col_meta, by = "col_idx") %>%
    select(-col_idx)

  # --- Suppression + numeric parsing ---
  # Epic suppresses counts of 10 or fewer as the literal string "10 or fewer";
  # suppressed cells can also arrive blank. Both mean "10 or fewer patients".
  is_suppressed_count <- function(x) {
    x <- trimws(x)
    is.na(x) | x == "" | x == "-" | x == "10 or fewer"
  }
  unsuppress_count <- function(x) {
    x <- trimws(x)
    suppressWarnings(as.numeric(ifelse(is_suppressed_count(x), "5", gsub(",", "", x))))
  }

  data_long <- data_long %>%
    mutate(
      suppressed = as.integer(is_suppressed_count(raw_value)),
      value = unsuppress_count(raw_value)
    )

  # ---------------------------------------------------------------------------
  # 6. Pivot measures wide
  # ---------------------------------------------------------------------------
  wide_n <- data_long %>%
    select(geography, time, measure, value) %>%
    pivot_wider(names_from = measure, values_from = value, values_fn = sum,
                names_glue = "epic_n_{measure}") %>%
    rename(epic_n_patients = epic_n_n_patients)

  wide_flag <- data_long %>%
    select(geography, time, measure, suppressed) %>%
    pivot_wider(names_from = measure, values_from = suppressed, values_fn = max,
                names_glue = "epic_{measure}_suppressed_flag")

  data_clean <- wide_n %>%
    left_join(wide_flag, by = c("geography", "time"))

  # Flags are computed BEFORE imputation, so they record what Epic withheld.
  # When the denominator itself was suppressed it has already been imputed to
  # 5, so 5/5*100 would assert a meaningless 100% - leave those cells NA, as
  # in cosmos_vector_borne/cosmos_gas/cosmos_concussions.
  for (dz in DISEASE_KEYS) {
    n_col <- paste0("epic_n_", dz)
    pct_col <- paste0("epic_pct_", dz)
    data_clean[[pct_col]] <- if_else(
      data_clean$epic_n_patients_suppressed_flag == 1L | data_clean$epic_n_patients == 0,
      NA_real_,
      data_clean[[n_col]] / data_clean$epic_n_patients * 100
    )
  }

  measure_cols <- unlist(lapply(DISEASE_KEYS, function(dz) {
    c(paste0("epic_n_", dz), paste0("epic_pct_", dz), paste0("epic_", dz, "_suppressed_flag"))
  }))

  data_clean <- data_clean %>%
    select(
      geography, time,
      all_of(measure_cols),
      epic_n_patients, epic_n_patients_suppressed_flag
    ) %>%
    arrange(time)

  # ---------------------------------------------------------------------------
  # 7. Validate
  # ---------------------------------------------------------------------------
  dupes <- data_clean %>%
    count(geography, time) %>%
    filter(n > 1)
  if (nrow(dupes) > 0) {
    stop("Duplicate rows per geography/time (", nrow(dupes), " combinations). ",
         "Check for overlapping staging files in raw/staging/.")
  }

  stopifnot(
    # Geography: always national, since this export has no state breakdown
    all(data_clean$geography == "00"),
    # Time: YYYY-mm-dd, always the last day of a month
    all(grepl("^\\d{4}-\\d{2}-\\d{2}$", data_clean$time)),
    all(as.Date(data_clean$time) == ceiling_date(as.Date(data_clean$time), "month") - days(1)),
    # Denominator is present and non-negative
    !any(is.na(data_clean$epic_n_patients)),
    all(data_clean$epic_n_patients >= 0),
    all(data_clean$epic_n_patients_suppressed_flag %in% c(0L, 1L)),
    all(data_clean$epic_n_patients[data_clean$epic_n_patients_suppressed_flag == 1L] == 5)
  )

  for (dz in DISEASE_KEYS) {
    n_col <- data_clean[[paste0("epic_n_", dz)]]
    pct_col <- data_clean[[paste0("epic_pct_", dz)]]
    flag_col <- data_clean[[paste0("epic_", dz, "_suppressed_flag")]]
    stopifnot(
      !any(is.na(n_col)),
      all(n_col >= 0),
      all(pct_col >= 0 & pct_col <= 100, na.rm = TRUE),
      all(flag_col %in% c(0L, 1L)),
      all(n_col[flag_col == 1L] == 5),
      identical(is.na(pct_col), data_clean$epic_n_patients_suppressed_flag == 1L)
    )
  }

  message(
    "Standardized ", nrow(data_clean), " rows | ",
    length(unique(data_clean$geography)), " geography(ies) | ",
    min(data_clean$time), " to ", max(data_clean$time)
  )
  for (dz in DISEASE_KEYS) {
    flag_col <- data_clean[[paste0("epic_", dz, "_suppressed_flag")]]
    message("  epic_", dz, "_suppressed_flag: ", sum(flag_col), " suppressed/imputed")
  }
  message(
    "  epic_n_patients_suppressed_flag: ", sum(data_clean$epic_n_patients_suppressed_flag),
    " suppressed/imputed"
  )

  # ---------------------------------------------------------------------------
  # 8. Write standardized output
  # ---------------------------------------------------------------------------
  if (!dir.exists("standard")) dir.create("standard")
  vroom::vroom_write(data_clean, "standard/data.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 9. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- current_state
  dcf::dcf_process_record(updated = process)
}
