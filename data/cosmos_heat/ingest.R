# =============================================================================
# Epic Cosmos Heat-Related Illness (HRI) Data Ingestion
# Source: Epic Cosmos SlicerDicer export (raw/staging/*.xlsx)
#         Number of Patients, and percent of patients with High/Med/Low
#         severity heat-related illness (HRI), by State of Residence and Month
#
# SlicerDicer session 2858385, "Number of Patients and High severity HRI and
# Med severity HRI and Low severity HRI by State of Residence":
#   Data model      : Patients
#   Population base : All Patients
#   Criteria        : Country of Residence = United States of America;
#                      Has Any Encounters?
#
# Layout is TRANSPOSED relative to most SlicerDicer exports in this repo:
#   - Rows are (Measure block) x (State of Residence), one measure per block
#     of 62 rows (50 states + DC + territories + "None of the above" + Total)
#   - Columns are (Year) x (Month), one column per calendar month; Year is
#     only populated on the first column of each year (fill right)
#   - The 4 measure blocks, identified by the label in column A: "Number of
#     Patients", "High severity HRI (%)", "Med severity HRI (%)",
#     "Low severity HRI (%)"
#
# Output (PopHIVE wide format, standard/data.csv.gz):
#   index   : geography (FIPS string, "00" = national), time (month-end)
#   measures: epic_n_patients                -> epic_n_patients_suppressed_flag
#             epic_pct_hri_high              -> epic_pct_hri_high_suppressed_flag
#             epic_pct_hri_med               -> epic_pct_hri_med_suppressed_flag
#             epic_pct_hri_low               -> epic_pct_hri_low_suppressed_flag
#
# Suppression / imputation notes (see README for full detail):
#   - epic_n_patients: blank / "10 or fewer" -> 5, flag 1 (in practice, none
#     of the 50 states/DC/Total ever hit this - only tiny territories do,
#     and those rows are dropped before this point)
#   - epic_pct_hri_*: this is "% of ALL patients" (not % of a heat-illness
#     cohort), so with denominators in the millions, suppressed (<=10
#     patient) numerators are common and show as a BLANK cell -> imputed as
#     5 / epic_n_patients * 100, flag 1
#   - Epic also bounds small-but-not-suppressed percentages as the literal
#     string "<0.01%" (numerator > 10 but tiny relative to the denominator).
#     Per project convention this is treated with the same half-the-bound
#     imputation logic as a suppressed count: imputed as 0.005 (i.e.
#     0.005%), flag 1. This is NOT necessarily privacy suppression - it is
#     Epic's display rounding - but it is flagged identically because the
#     exact value is unknown and was imputed. In this export the large
#     majority of HRI percent cells fall into blank/"<0.01%" precisely
#     because heat-related illness is rare relative to the entire "All
#     Patients" cohort used as the denominator.
#   - If epic_n_patients itself was suppressed (imputed to 5), the percent
#     is left NA (flag still 1) rather than asserting a meaningless
#     5/5*100 = 100%, matching the precedent in cosmos_gas/cosmos_vaccines.
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

# Load FIPS lookup (preferred over cdlTools::fips())
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

# =============================================================================
# 1. Locate staging file & detect change
# =============================================================================

staging_files <- list.files("raw/staging", pattern = "\\.(csv|xlsx)$", full.names = TRUE)
if (length(staging_files) == 0) {
  stop(
    "No staging files found in raw/staging/.\n",
    "Export data from Epic Cosmos SlicerDicer and place .xlsx files there."
  )
}

current_state <- list(
  files = staging_files,
  hashes = unname(tools::md5sum(staging_files))
)

if (!identical(process$raw_state, current_state)) {

  f <- staging_files[1]
  if (length(staging_files) > 1) {
    message("Multiple staging files found; using the most recent: ", basename(f))
  }

  # ---------------------------------------------------------------------------
  # 2. Decrypt and load the raw grid
  # ---------------------------------------------------------------------------
  if (is.null(xlsx_password) || !nzchar(xlsx_password)) {
    stop(
      "EPIC_XLSX_PASSWORD is not set, but ", basename(f), " is a ",
      "password-protected SlicerDicer export.\n",
      "Set it in .Renviron via usethis::edit_r_environ()."
    )
  }

  decrypted <- tempfile(fileext = ".xlsx")
  cmd <- sprintf(
    'python -m msoffcrypto -p "%s" "%s" "%s"',
    xlsx_password, normalizePath(f, winslash = "/"), decrypted
  )
  if (system(cmd) != 0) stop("Decryption failed: ", f)
  on.exit(unlink(decrypted), add = TRUE)

  wb <- wb_load(decrypted)
  grid <- wb_to_df(wb, sheet = 1, col_names = FALSE, skip_empty_rows = FALSE, skip_empty_cols = FALSE)
  grid <- as.data.frame(lapply(grid, as.character), stringsAsFactors = FALSE)

  # ---------------------------------------------------------------------------
  # 3. Locate the id header row ("Measures" / "State of Residence") by
  #    content, not a fixed row number, since SlicerDicer sessions drift
  # ---------------------------------------------------------------------------
  header_row <- which(trimws(grid[[1]]) == "Measures" & trimws(grid[[2]]) == "State of Residence")
  if (length(header_row) != 1) {
    stop(
      "Could not find the 'Measures' / 'State of Residence' header row - ",
      "export layout may have changed. Update ingest.R."
    )
  }
  year_row_idx <- header_row - 2L
  month_row_idx <- header_row - 1L

  n_cols <- ncol(grid)
  value_col_idx <- 3:n_cols

  year_raw <- unlist(grid[year_row_idx, value_col_idx], use.names = FALSE)
  month_raw <- trimws(unlist(grid[month_row_idx, value_col_idx], use.names = FALSE))

  year_raw[year_raw == ""] <- NA
  year_filled <- zoo::na.locf(year_raw, na.rm = FALSE)

  # Only fully-labeled months (e.g. "Jan") are usable; partial leading/
  # trailing periods (e.g. "Sep 1") are not comparable to whole months
  keep_col <- month_raw %in% month.abb
  if (any(!keep_col)) {
    message(
      "Dropping ", sum(!keep_col), " partial-period column(s): ",
      paste(unique(month_raw[!keep_col]), collapse = ", ")
    )
  }
  if (all(!keep_col)) {
    stop("Every month column was classified as partial - the month label format probably changed.")
  }

  col_time <- as.Date(paste(year_filled, month_raw, "01"), format = "%Y %b %d")
  col_time <- format(ceiling_date(col_time, "month") - days(1), "%Y-%m-%d")

  # ---------------------------------------------------------------------------
  # 4. Identify measure blocks (column A, filled only on the block's first row)
  # ---------------------------------------------------------------------------
  data_rows_idx <- (header_row + 1):nrow(grid)
  data <- grid[data_rows_idx, , drop = FALSE]

  measure_label <- data[[1]]
  measure_label[measure_label == ""] <- NA
  measure_label <- zoo::na.locf(measure_label, na.rm = FALSE)

  MEASURE_LABELS <- c(
    "Number of Patients" = "n_patients",
    "High severity HRI (%)" = "pct_hri_high",
    "Med severity HRI (%)" = "pct_hri_med",
    "Low severity HRI (%)" = "pct_hri_low"
  )
  unknown_measures <- setdiff(unique(measure_label), names(MEASURE_LABELS))
  if (length(unknown_measures) > 0) {
    stop(
      "Unrecognized measure label(s) in export: ",
      paste(unknown_measures, collapse = ", "),
      ". Update MEASURE_LABELS in ingest.R."
    )
  }
  measure_std <- unname(MEASURE_LABELS[measure_label])

  state_name <- trimws(iconv(data[[2]], to = "UTF-8", sub = ""))

  # ---------------------------------------------------------------------------
  # 5. Reshape to long: one row per (measure, state, month)
  # ---------------------------------------------------------------------------
  value_block <- data[, value_col_idx, drop = FALSE]
  colnames(value_block) <- paste0("t", seq_along(value_col_idx))

  long <- bind_cols(
    data.frame(measure = measure_std, state_name = state_name, stringsAsFactors = FALSE),
    value_block
  ) %>%
    pivot_longer(cols = starts_with("t"), names_to = "col_key", values_to = "raw_value") %>%
    mutate(col_idx = as.integer(sub("^t", "", col_key))) %>%
    filter(keep_col[col_idx]) %>%
    mutate(time = col_time[col_idx]) %>%
    select(measure, state_name, time, raw_value)

  # ---------------------------------------------------------------------------
  # 6. Geography: state name -> FIPS via all_fips.csv.gz; drop non-US/catch-all
  # ---------------------------------------------------------------------------
  valid_states <- c(state.name, "District of Columbia")

  long_geo <- long %>%
    mutate(
      geography_name = case_when(
        state_name == "Total" ~ "United States",
        state_name %in% valid_states ~ state_name,
        TRUE ~ NA_character_
      )
    ) %>%
    filter(!is.na(geography_name)) %>%
    left_join(state_fips_lookup, by = "geography_name") %>%
    filter(!is.na(geography))

  dropped_states <- setdiff(unique(long$state_name), c(valid_states, "Total"))
  if (length(dropped_states) > 0) {
    message(
      "Dropped non-US / catch-all rows: ",
      paste(sort(dropped_states), collapse = ", ")
    )
  }

  # ---------------------------------------------------------------------------
  # 7. Pivot to wide (one column per measure) and apply suppression rules
  # ---------------------------------------------------------------------------
  wide <- long_geo %>%
    select(geography, time, measure, raw_value) %>%
    pivot_wider(names_from = measure, values_from = raw_value)

  dupes <- wide %>% count(geography, time) %>% filter(n > 1)
  if (nrow(dupes) > 0) {
    stop(
      "Duplicate rows per geography/time (", nrow(dupes), " combinations). ",
      "Check for overlapping staging files in raw/staging/."
    )
  }

  is_blank <- function(x) is.na(x) | trimws(x) %in% c("", "-")
  is_bound_pct <- function(x) trimws(x) == "<0.01%"
  is_suppressed_count <- function(x) is.na(x) | trimws(x) %in% c("", "10 or fewer")

  unsuppress_count <- function(x) {
    suppressWarnings(as.numeric(if_else(is_suppressed_count(x), "5", gsub(",", "", x))))
  }

  # A percent measure with the numerator suppressed/bounded gets imputed
  # relative to epic_n_patients, UNLESS the denominator itself was
  # suppressed, in which case the percent is left NA (see header notes)
  impute_pct <- function(x, denom_suppressed_flag, n_patients) {
    case_when(
      denom_suppressed_flag == 1L ~ NA_real_,
      is_bound_pct(x) ~ 0.005,
      is_blank(x) ~ 5 / n_patients * 100,
      TRUE ~ suppressWarnings(as.numeric(gsub("%", "", x)))
    )
  }
  flag_pct <- function(x, denom_suppressed_flag) {
    as.integer(denom_suppressed_flag == 1L | is_blank(x) | is_bound_pct(x))
  }

  data_clean <- wide %>%
    mutate(
      epic_n_patients_suppressed_flag = as.integer(is_suppressed_count(n_patients)),
      epic_n_patients = unsuppress_count(n_patients),

      epic_pct_hri_high_suppressed_flag = flag_pct(pct_hri_high, epic_n_patients_suppressed_flag),
      epic_pct_hri_high = impute_pct(pct_hri_high, epic_n_patients_suppressed_flag, epic_n_patients),

      epic_pct_hri_med_suppressed_flag = flag_pct(pct_hri_med, epic_n_patients_suppressed_flag),
      epic_pct_hri_med = impute_pct(pct_hri_med, epic_n_patients_suppressed_flag, epic_n_patients),

      epic_pct_hri_low_suppressed_flag = flag_pct(pct_hri_low, epic_n_patients_suppressed_flag),
      epic_pct_hri_low = impute_pct(pct_hri_low, epic_n_patients_suppressed_flag, epic_n_patients)
    ) %>%
    select(
      geography, time,
      epic_n_patients, epic_n_patients_suppressed_flag,
      epic_pct_hri_high, epic_pct_hri_high_suppressed_flag,
      epic_pct_hri_med, epic_pct_hri_med_suppressed_flag,
      epic_pct_hri_low, epic_pct_hri_low_suppressed_flag
    ) %>%
    arrange(geography, time)

  # ---------------------------------------------------------------------------
  # 8. Validate
  # ---------------------------------------------------------------------------
  stopifnot(
    # Geography: FIPS strings, national is "00"
    all(nchar(data_clean$geography) == 2),
    "00" %in% data_clean$geography,
    length(unique(data_clean$geography)) == 52, # 50 states + DC + national
    # Time: YYYY-mm-dd, always the last day of a month
    all(grepl("^\\d{4}-\\d{2}-\\d{2}$", data_clean$time)),
    all(as.Date(data_clean$time) == ceiling_date(as.Date(data_clean$time), "month") - days(1)),
    # Counts are present, non-negative, and every imputed count is exactly 5
    !any(is.na(data_clean$epic_n_patients)),
    all(data_clean$epic_n_patients >= 0),
    all(data_clean$epic_n_patients[data_clean$epic_n_patients_suppressed_flag == 1L] == 5),
    # Flags are 0/1
    all(data_clean$epic_n_patients_suppressed_flag %in% c(0L, 1L)),
    all(data_clean$epic_pct_hri_high_suppressed_flag %in% c(0L, 1L)),
    all(data_clean$epic_pct_hri_med_suppressed_flag %in% c(0L, 1L)),
    all(data_clean$epic_pct_hri_low_suppressed_flag %in% c(0L, 1L)),
    # Percentages are in [0, 100] where present
    all(data_clean$epic_pct_hri_high >= 0 & data_clean$epic_pct_hri_high <= 100, na.rm = TRUE),
    all(data_clean$epic_pct_hri_med >= 0 & data_clean$epic_pct_hri_med <= 100, na.rm = TRUE),
    all(data_clean$epic_pct_hri_low >= 0 & data_clean$epic_pct_hri_low <= 100, na.rm = TRUE),
    # A percent cell is NA only where the denominator itself was suppressed
    identical(is.na(data_clean$epic_pct_hri_high), data_clean$epic_n_patients_suppressed_flag == 1L),
    identical(is.na(data_clean$epic_pct_hri_med), data_clean$epic_n_patients_suppressed_flag == 1L),
    identical(is.na(data_clean$epic_pct_hri_low), data_clean$epic_n_patients_suppressed_flag == 1L)
  )

  message(
    "Standardized ", nrow(data_clean), " rows | ",
    length(unique(data_clean$geography)), " geographies | ",
    min(data_clean$time), " to ", max(data_clean$time)
  )
  message("  epic_n_patients_suppressed_flag: ", sum(data_clean$epic_n_patients_suppressed_flag), " imputed")
  message(
    "  epic_pct_hri_high_suppressed_flag: ", sum(data_clean$epic_pct_hri_high_suppressed_flag),
    " imputed/bounded (", sum(is.na(data_clean$epic_pct_hri_high)), " left NA)"
  )
  message(
    "  epic_pct_hri_med_suppressed_flag:  ", sum(data_clean$epic_pct_hri_med_suppressed_flag),
    " imputed/bounded (", sum(is.na(data_clean$epic_pct_hri_med)), " left NA)"
  )
  message(
    "  epic_pct_hri_low_suppressed_flag:  ", sum(data_clean$epic_pct_hri_low_suppressed_flag),
    " imputed/bounded (", sum(is.na(data_clean$epic_pct_hri_low)), " left NA)"
  )

  # ---------------------------------------------------------------------------
  # 9. Write standardized output
  # ---------------------------------------------------------------------------
  if (!dir.exists("standard")) dir.create("standard")
  vroom::vroom_write(data_clean, "standard/data.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 10. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- current_state
  dcf::dcf_process_record(updated = process)
}
