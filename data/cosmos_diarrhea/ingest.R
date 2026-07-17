# =============================================================================
# Epic Cosmos Diarrhea Data Ingestion
# Source: Epic SlicerDicer exports - ED visits for all-cause diarrhea
#         (ICD-10: A00-09, R19.7), weekly, by state and age
# =============================================================================

library(tidyverse)
library(dcf)
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
all_fips <- vroom::vroom('https://github.com/PopHIVE/Ingest/raw/refs/heads/main/resources/all_fips.csv.gz')

state_fips_lookup <- all_fips %>%
  filter(nchar(geography) == 2) %>%
  select(geography, geography_name)

# =============================================================================
# Function: Parse wide-format Epic SlicerDicer crosstab
# =============================================================================
# Layout: outcome blocks (diarrhea, Total) across columns, each block spanning
# 7 age columns. Rows are one per (State of Residence, Year, Week/Month).
# Row 12: outcome labels (fill rightward, one label per block)
# Row 13: age labels (one per column, repeats within each block)
# Row 14: id column headers (State of Residence, Year, Week or Month)
# Row 15+: data rows (state and year fill down, week/month per row)
# =============================================================================

parse_week_range <- function(week_str, year_str) {
  year1 <- as.integer(stringr::str_extract(year_str, "^\\d{4}"))
  year2 <- as.integer(stringr::str_extract(year_str, "\\d{4}$"))
  is_cross_year <- year1 != year2

  start_match <- stringr::str_match(
    week_str, "^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\s+(\\d+)"
  )
  start_month_num <- match(start_match[, 2], month.abb)
  start_day <- as.integer(start_match[, 3])

  end_match <- stringr::str_match(
    week_str, "(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\s+(\\d+)\\s*$"
  )
  end_month_num <- match(end_match[, 2], month.abb)
  end_day <- as.integer(end_match[, 3])

  start_year <- ifelse(is_cross_year & start_month_num == 12, year1, year2)
  end_year   <- ifelse(is_cross_year & end_month_num == 12, year1, year2)

  start_date <- as.Date(paste(start_year, start_month_num, start_day, sep = "-"))
  end_date   <- as.Date(paste(end_year, end_month_num, end_day, sep = "-"))
  n_days <- as.integer(end_date - start_date) + 1L

  list(start_date = start_date, end_date = end_date, n_days = n_days)
}

process_diarrhea_wide <- function(file, granularity, password = NULL) {
  message("Processing ", granularity, " file: ", basename(file))

  decrypted_file <- tempfile(fileext = ".xlsx")
  cmd <- sprintf(
    'python -m msoffcrypto -p "%s" "%s" "%s"',
    password, normalizePath(file, winslash = "/"), decrypted_file
  )
  status <- system(cmd)
  if (status != 0) stop("Failed to decrypt: ", file)

  wb <- openxlsx2::wb_load(decrypted_file)
  all_rows <- openxlsx2::wb_to_df(
    wb, sheet = 1, col_names = FALSE,
    skip_empty_rows = FALSE, skip_empty_cols = FALSE
  )
  if (file.exists(decrypted_file)) unlink(decrypted_file)

  # Convert all columns to character; replace NA with ""
  all_rows <- as.data.frame(
    lapply(all_rows, function(x) {
      x <- as.character(x)
      x[is.na(x)] <- ""
      x
    }),
    stringsAsFactors = FALSE
  )

  # --- Extract metadata from header rows 1-9 ---
  meta_fields <- c(
    "Session Title", "Session ID", "Data Model", "Population Base",
    "Population Criteria Filters", "Session Date Range", "Measure",
    "Export User", "Date of Export"
  )
  meta <- list(
    file = file,
    md5 = unname(tools::md5sum(file)),
    date_processed = as.character(Sys.time())
  )
  for (i in seq_along(meta_fields)) {
    meta[[meta_fields[i]]] <- trimws(as.character(all_rows[i, 2]))
  }

  row12 <- as.character(all_rows[12, ])
  row13 <- as.character(all_rows[13, ])

  n_cols <- ncol(all_rows)
  id_cols <- 3L

  # --- Fill outcome labels rightward within the value columns ---
  outcome_raw <- row12[(id_cols + 1):n_cols]
  outcome_raw[outcome_raw == ""] <- NA
  outcome_raw <- zoo::na.locf(outcome_raw, na.rm = FALSE)
  outcome_std <- if_else(grepl("^Total", outcome_raw), "all_encounters", "diarrhea")

  age_raw <- trimws(row13[(id_cols + 1):n_cols])

  col_meta <- data.frame(
    col_idx = (id_cols + 1):n_cols,
    outcome = outcome_std,
    age_raw = age_raw,
    stringsAsFactors = FALSE
  )

  # --- Data rows (row 15 onward) ---
  data_raw <- all_rows[15:nrow(all_rows), , drop = FALSE]
  colnames(data_raw) <- c("state_name", "year_raw", "time_raw", paste0("v", (id_cols + 1):n_cols))
  rownames(data_raw) <- NULL

  data_raw$state_name[data_raw$state_name == ""] <- NA
  data_raw$year_raw[data_raw$year_raw == ""] <- NA
  data_raw <- tidyr::fill(data_raw, state_name, year_raw, .direction = "down")

  data_long <- data_raw %>%
    tidyr::pivot_longer(
      cols = starts_with("v"),
      names_to = "col_name",
      values_to = "val"
    ) %>%
    mutate(col_idx = as.integer(sub("v", "", col_name))) %>%
    left_join(col_meta, by = "col_idx")

  if (granularity == "week") {
    # Epic's weekly buckets are 7-day spans anchored to the export date, not
    # CDC Sunday-Saturday epiweeks (e.g. "Jun 23 - Jun 29" runs Thu-Wed). Map
    # each complete 7-day span to the Saturday that falls within it, per the
    # project's Saturday-end-of-week convention.
    parsed <- parse_week_range(data_long$time_raw, data_long$year_raw)
    data_long <- data_long %>%
      mutate(start_date = parsed$start_date, n_days = parsed$n_days) %>%
      filter(n_days == 7) %>%
      mutate(
        sat_offset = (6 - as.integer(format(start_date, "%u"))) %% 7,
        time = start_date + sat_offset
      ) %>%
      select(-n_days, -sat_offset, -start_date)
  } else {
    # Monthly: keep only rows where the time label is a full 3-letter month
    # abbreviation (partial start/end months use day numbers/ranges instead)
    data_long <- data_long %>%
      filter(time_raw %in% month.abb) %>%
      mutate(
        month_num = match(time_raw, month.abb),
        year_num = as.integer(year_raw),
        time = lubridate::ceiling_date(
          as.Date(paste(year_num, month_num, "01", sep = "-")), "month"
        ) - lubridate::days(1)
      ) %>%
      select(-month_num, -year_num)
  }

  data_long <- data_long %>%
    mutate(
      val = trimws(val),
      suppressed = val == "10 or fewer",
      value = suppressWarnings(as.numeric(ifelse(suppressed, "5", val)))
    ) %>%
    select(state_name, age_raw, outcome, time, value, suppressed)

  return(list(data = data_long, metadata = meta))
}

# =============================================================================
# 1. Process wide-format files from raw/staging_diarrhea_wide/
# =============================================================================

xlsx_password <- Sys.getenv("EPIC_XLSX_PASSWORD")  # add password to environment using usethis::edit_r_environ()

wide_files <- list.files("raw/staging_diarrhea_wide", "\\.xlsx$", full.names = TRUE)

# Only the weekly export is retained in this folder now (monthly removed).
wide_results <- lapply(wide_files, process_diarrhea_wide, granularity = "week", password = xlsx_password)

# Save metadata from wide-format file headers to JSON
wide_metadata <- lapply(wide_results, `[[`, "metadata")
jsonlite::write_json(
  wide_metadata,
  "raw/staging_diarrhea_wide.json",
  auto_unbox = TRUE,
  pretty = TRUE
)

standardize_diarrhea_data <- function(data_long) {
  valid_states <- c(state.name, "District of Columbia")

  data_long <- data_long %>%
    mutate(
      is_national = grepl("^Total", state_name),
      state_clean = case_when(
        is_national ~ "Total",
        state_name %in% valid_states ~ state_name,
        TRUE ~ NA_character_
      ),
      # "Total: ..." -> "Total"; numeric ranges standardized below
      age = ifelse(grepl("^Total:", age_raw), "Total", age_raw)
    ) %>%
    filter(!is.na(state_clean)) %>%
    select(state_name = state_clean, age, outcome, time, value, suppressed)

  # --- Standardize age groups ---
  # Raw ages use exclusive upper bounds ("< 5", "< 18", etc.), so subtract 1
  # from the upper bound to get inclusive ranges: 1-4, 5-17, 18-49, 50-64
  data_long <- data_long %>%
    mutate(
      age = trimws(age),
      age = stringr::str_replace(age, "^Less than\\s+(\\d+).*$", "<\\1 Years"),
      age = stringr::str_replace(age, "^(\\d+)\\s+Years or more$", "\\1+ Years"),
      age = {
        m <- stringr::str_match(age, "^[^0-9]*?(\\d+)\\s+and\\s+<\\s*(\\d+)\\s*Years?$")
        lower <- m[, 2]
        upper <- as.character(as.integer(m[, 3]) - 1L)
        ifelse(!is.na(lower), paste0(lower, "-", upper, " Years"), age)
      }
    )

  # --- Map state names to FIPS geography codes ---
  data_long <- data_long %>%
    mutate(geography_name = ifelse(state_name == "Total", "United States", state_name)) %>%
    left_join(state_fips_lookup, by = "geography_name") %>%
    filter(!is.na(geography)) %>%
    select(-state_name, -geography_name)

  data_long
}

weekly_long <- standardize_diarrhea_data(wide_results[[1]]$data)

# =============================================================================
# 2. Pivot to wide standard format and compute percentages
# =============================================================================

build_standard_table <- function(data_long, suffix) {
  value_wide <- data_long %>%
    select(geography, age, time, outcome, value) %>%
    pivot_wider(names_from = outcome, values_from = value, values_fn = sum)

  suppressed_wide <- data_long %>%
    mutate(
      supp_col = paste0("suppressed_flag_", outcome),
      suppressed = as.integer(suppressed)
    ) %>%
    select(geography, age, time, supp_col, suppressed) %>%
    pivot_wider(names_from = supp_col, values_from = suppressed, values_fn = max)

  merged <- value_wide %>%
    left_join(suppressed_wide, by = c("geography", "age", "time")) %>%
    filter(!is.na(age)) %>%
    rename(
      n_diarrhea = diarrhea,
      !!paste0("n_all_encounters_", suffix) := all_encounters
    ) %>%
    mutate(
      pct_diarrhea = 100 * n_diarrhea / .data[[paste0("n_all_encounters_", suffix)]]
    ) %>%
    rename(
      suppressed_flag_all_encounters = paste0("suppressed_flag_all_encounters")
    ) %>%
    rename(!!paste0("suppressed_flag_all_encounters_", suffix) := suppressed_flag_all_encounters) %>%
    rename_with(~ paste0("epic_", .x), .cols = -c(geography, time, age)) %>%
    arrange(geography, age, time) %>%
    select(
      geography, age, time,
      starts_with("epic_n"),
      starts_with("epic_pct"),
      starts_with("epic_suppressed")
    )

  merged
}

weekly_standard <- build_standard_table(weekly_long, "weekly")

# =============================================================================
# 2b. Process the state x age x week "all encounters" (non-ED) diarrhea
#     crosstab and merge it with the ED weekly measure into one standard file,
#     standard/data_weekly.csv.gz. The two sources use different SlicerDicer
#     weekly bucket anchors (the ED export's buckets run Thu-Wed; the
#     all-encounters export's run Fri-Thu -- both are 7-day spans anchored to
#     their own export date, not CDC epiweeks), so their most recent
#     *complete* week can land on different Saturdays depending on when each
#     was exported. A full join on (geography, age, time) merges them without
#     truncating either source to the other's max date -- weeks present in
#     only one source get NA for the other source's columns rather than
#     being silently dropped.
# =============================================================================

process_diarrhea_all_encounters_weekly_wide <- function(file, password = NULL) {
  message("Processing diarrhea all-encounters (state x age) weekly file: ", basename(file))

  decrypted_file <- tempfile(fileext = ".xlsx")
  cmd <- sprintf(
    'python -m msoffcrypto -p "%s" "%s" "%s"',
    password, normalizePath(file, winslash = "/"), decrypted_file
  )
  status <- system(cmd)
  if (status != 0) stop("Failed to decrypt: ", file)

  wb <- openxlsx2::wb_load(decrypted_file)
  all_rows <- openxlsx2::wb_to_df(
    wb, sheet = 1, col_names = FALSE,
    skip_empty_rows = FALSE, skip_empty_cols = FALSE
  )
  if (file.exists(decrypted_file)) unlink(decrypted_file)

  all_rows <- as.data.frame(
    lapply(all_rows, function(x) {
      x <- as.character(x)
      x[is.na(x)] <- ""
      x
    }),
    stringsAsFactors = FALSE
  )

  meta_fields <- c(
    "Session Title", "Session ID", "Data Model", "Population Base",
    "Population Criteria Filters", "Session Date Range", "Measure",
    "Export User", "Date of Export"
  )
  meta <- list(
    file = file,
    md5 = unname(tools::md5sum(file)),
    date_processed = as.character(Sys.time())
  )
  for (i in seq_along(meta_fields)) {
    meta[[meta_fields[i]]] <- trimws(as.character(all_rows[i, 2]))
  }

  row12 <- as.character(all_rows[12, ])
  row13 <- as.character(all_rows[13, ])

  n_cols <- ncol(all_rows)
  id_cols <- 3L  # Year, Week, State of Residence (note: different column
                 # order than the ED crosstab's State, Year, Week)

  # Three outcome blocks: diarrhea, "None of the above" (dropped -- fully
  # redundant with Total - diarrhea), Total (all-cause, any reason).
  outcome_raw <- row12[(id_cols + 1):n_cols]
  outcome_raw[outcome_raw == ""] <- NA
  outcome_raw <- zoo::na.locf(outcome_raw, na.rm = FALSE)
  outcome_std <- case_when(
    grepl("^Total", outcome_raw) ~ "encounters_total_weekly",
    grepl("^diarrhea", outcome_raw, ignore.case = TRUE) ~ "all_diarrhea",
    TRUE ~ NA_character_
  )

  age_raw <- trimws(row13[(id_cols + 1):n_cols])

  col_meta <- data.frame(
    col_idx = (id_cols + 1):n_cols,
    outcome = outcome_std,
    age_raw = age_raw,
    stringsAsFactors = FALSE
  )

  data_raw <- all_rows[15:nrow(all_rows), , drop = FALSE]
  colnames(data_raw) <- c("year_raw", "week_raw", "state_name", paste0("v", (id_cols + 1):n_cols))
  rownames(data_raw) <- NULL

  data_raw$year_raw[data_raw$year_raw == ""] <- NA
  data_raw$week_raw[data_raw$week_raw == ""] <- NA
  data_raw$state_name[data_raw$state_name == ""] <- NA
  data_raw <- tidyr::fill(data_raw, year_raw, week_raw, state_name, .direction = "down")

  data_long <- data_raw %>%
    tidyr::pivot_longer(
      cols = starts_with("v"),
      names_to = "col_name",
      values_to = "val"
    ) %>%
    mutate(col_idx = as.integer(sub("v", "", col_name))) %>%
    left_join(col_meta, by = "col_idx") %>%
    filter(!is.na(outcome))  # drop "None of the above" block

  # Thu-Wed 7-day weekly buckets, same as the other weekly crosstabs -- map
  # to the Saturday within each complete week.
  parsed <- parse_week_range(data_long$week_raw, data_long$year_raw)
  data_long <- data_long %>%
    mutate(start_date = parsed$start_date, n_days = parsed$n_days) %>%
    filter(n_days == 7) %>%
    mutate(
      sat_offset = (6 - as.integer(format(start_date, "%u"))) %% 7,
      time = start_date + sat_offset
    ) %>%
    select(-n_days, -sat_offset, -start_date)

  data_long <- data_long %>%
    mutate(
      val = trimws(val),
      suppressed = val == "10 or fewer",
      value = suppressWarnings(as.numeric(ifelse(suppressed, "5", val))),
      age = trimws(age_raw)
    ) %>%
    # "No value" = encounters with undocumented age; no equivalent bucket in
    # the ED crosstab, so dropped rather than merged as a fabricated group.
    filter(age != "No value") %>%
    mutate(
      age = stringr::str_replace(age, "^Less than\\s+(\\d+)(?:\\s+Years?)?$", "<\\1 Years"),
      age = stringr::str_replace(age, "^(\\d+)\\s+(?:Years\\s+)?or more$", "\\1+ Years"),
      age = {
        m <- stringr::str_match(age, "^[^0-9]*?(\\d+)\\s+and\\s+<\\s*(\\d+)(?:\\s*Years?)?$")
        lower <- m[, 2]
        upper <- as.character(as.integer(m[, 3]) - 1L)
        ifelse(!is.na(lower), paste0(lower, "-", upper, " Years"), age)
      }
    ) %>%
    select(state_name, age, outcome, time, value, suppressed)

  return(list(data = data_long, metadata = meta))
}

standardize_all_encounters_geo <- function(data_long) {
  valid_states <- c(state.name, "District of Columbia")

  data_long %>%
    mutate(
      is_national = grepl("^Total", state_name),
      state_clean = case_when(
        is_national ~ "Total",
        state_name %in% valid_states ~ state_name,
        TRUE ~ NA_character_
      )
    ) %>%
    filter(!is.na(state_clean)) %>%
    mutate(geography_name = ifelse(state_clean == "Total", "United States", state_clean)) %>%
    left_join(state_fips_lookup, by = "geography_name") %>%
    filter(!is.na(geography)) %>%
    select(geography, age, outcome, time, value, suppressed)
}

build_all_encounters_weekly_table <- function(data_long) {
  value_wide <- data_long %>%
    select(geography, age, time, outcome, value) %>%
    pivot_wider(names_from = outcome, values_from = value, values_fn = sum)

  suppressed_wide <- data_long %>%
    mutate(
      supp_col = paste0("suppressed_flag_", outcome),
      suppressed = as.integer(suppressed)
    ) %>%
    select(geography, age, time, supp_col, suppressed) %>%
    pivot_wider(names_from = supp_col, values_from = suppressed, values_fn = max)

  value_wide %>%
    left_join(suppressed_wide, by = c("geography", "age", "time")) %>%
    filter(!is.na(age)) %>%
    rename(
      n_all_diarrhea = all_diarrhea,
      n_encounters_total_weekly = encounters_total_weekly
    ) %>%
    mutate(pct_all_diarrhea = 100 * n_all_diarrhea / n_encounters_total_weekly) %>%
    rename_with(~ paste0("epic_", .x), .cols = -c(geography, time, age)) %>%
    arrange(geography, age, time) %>%
    select(
      geography, age, time,
      starts_with("epic_n"),
      starts_with("epic_pct"),
      starts_with("epic_suppressed")
    )
}

all_encounters_weekly_files <- list.files(
  "raw/staging_diarrhea_all_encounters_weekly_wide", "\\.xlsx$", full.names = TRUE
)
all_encounters_weekly_results <- lapply(
  all_encounters_weekly_files, process_diarrhea_all_encounters_weekly_wide, password = xlsx_password
)

jsonlite::write_json(
  lapply(all_encounters_weekly_results, `[[`, "metadata"),
  "raw/staging_diarrhea_all_encounters_weekly_wide.json",
  auto_unbox = TRUE, pretty = TRUE
)

all_encounters_weekly_long <- standardize_all_encounters_geo(
  bind_rows(lapply(all_encounters_weekly_results, `[[`, "data"))
)
all_encounters_weekly_standard <- build_all_encounters_weekly_table(all_encounters_weekly_long)

weekly_standard_ed <- weekly_standard %>%
  rename(
    epic_n_ed_diarrhea = epic_n_diarrhea,
    epic_n_ed_encounters_weekly = epic_n_all_encounters_weekly,
    epic_pct_ed_diarrhea = epic_pct_diarrhea,
    epic_suppressed_flag_ed_diarrhea = epic_suppressed_flag_diarrhea,
    epic_suppressed_flag_ed_encounters_weekly = epic_suppressed_flag_all_encounters_weekly
  ) %>%
  arrange(geography, age, time)

data_weekly <- full_join(
  weekly_standard_ed, all_encounters_weekly_standard,
  by = c("geography", "age", "time")
) %>%
  arrange(geography, age, time)

vroom::vroom_write(data_weekly, "standard/data_weekly.csv.gz", ",")

# =============================================================================
# 3. Process cyclospora lab test crosstab from raw/staging_cyclospora_wide/
# =============================================================================
# This export has a different layout and grain than the ED diarrhea crosstab
# above: id columns are Year, Month, "(Abnormal) Lab Components", State of
# Residence (no age breakdown), with two value columns nested under "(All)
# Lab Components": "cyclospora lab tests" and "Total". The row-level
# abnormal-lab dimension has two categories:
#   - "Cyclospora abnormal": encounters with an abnormal (positive) cyclospora
#     lab result. Both value columns collapse to the same count here.
#   - "Total" (all abnormal lab types, i.e. the unfiltered population): the
#     "cyclospora lab tests" column gives the number of encounters with a
#     cyclospora test performed (any result), and "Total" gives all
#     encounters for any reason -- the population denominator. Note this
#     denominator is NOT ED-specific (Population Base: "All Encounters"),
#     unlike epic_n_ed_encounters_weekly in data_weekly.csv.gz above.
# Because this file has no age breakdown, it is kept as its own standardized
# output (state x month grain) rather than merged into data_weekly.csv.gz.
# =============================================================================

process_cyclospora_wide <- function(file, password = NULL) {
  message("Processing cyclospora file: ", basename(file))

  decrypted_file <- tempfile(fileext = ".xlsx")
  cmd <- sprintf(
    'python -m msoffcrypto -p "%s" "%s" "%s"',
    password, normalizePath(file, winslash = "/"), decrypted_file
  )
  status <- system(cmd)
  if (status != 0) stop("Failed to decrypt: ", file)

  wb <- openxlsx2::wb_load(decrypted_file)
  all_rows <- openxlsx2::wb_to_df(
    wb, sheet = 1, col_names = FALSE,
    skip_empty_rows = FALSE, skip_empty_cols = FALSE
  )
  if (file.exists(decrypted_file)) unlink(decrypted_file)

  all_rows <- as.data.frame(
    lapply(all_rows, function(x) {
      x <- as.character(x)
      x[is.na(x)] <- ""
      x
    }),
    stringsAsFactors = FALSE
  )

  meta_fields <- c(
    "Session Title", "Session ID", "Data Model", "Population Base",
    "Population Criteria Filters", "Session Date Range", "Measure",
    "Export User", "Date of Export"
  )
  meta <- list(
    file = file,
    md5 = unname(tools::md5sum(file)),
    date_processed = as.character(Sys.time())
  )
  for (i in seq_along(meta_fields)) {
    meta[[meta_fields[i]]] <- trimws(as.character(all_rows[i, 2]))
  }

  # Row 13 id headers span cols 1-4 (Year, Month, Abnormal Lab Components,
  # State of Residence); value columns are 5 (cyclospora lab tests) and 6 (Total)
  data_raw <- all_rows[14:nrow(all_rows), , drop = FALSE]
  colnames(data_raw) <- c("year_raw", "month_raw", "abn_lab_raw", "state_name", "v_tested", "v_total")
  rownames(data_raw) <- NULL

  data_raw$year_raw[data_raw$year_raw == ""] <- NA
  data_raw$month_raw[data_raw$month_raw == ""] <- NA
  data_raw$abn_lab_raw[data_raw$abn_lab_raw == ""] <- NA
  data_raw <- tidyr::fill(data_raw, year_raw, month_raw, abn_lab_raw, .direction = "down")

  data_raw <- data_raw %>%
    mutate(abn_lab = if_else(grepl("^Total:", abn_lab_raw), "Total", abn_lab_raw)) %>%
    # Keep only complete calendar months (partial start/end months use day
    # numbers/ranges instead of a 3-letter abbreviation)
    filter(month_raw %in% month.abb) %>%
    mutate(
      month_num = match(month_raw, month.abb),
      year_num = as.integer(year_raw),
      time = lubridate::ceiling_date(
        as.Date(paste(year_num, month_num, "01", sep = "-")), "month"
      ) - lubridate::days(1)
    )

  positive <- data_raw %>%
    filter(abn_lab == "Cyclospora abnormal") %>%
    mutate(
      val = trimws(v_tested),
      suppressed = val == "10 or fewer",
      value = suppressWarnings(as.numeric(ifelse(suppressed, "5", val))),
      measure = "cyclospora_positive"
    ) %>%
    select(state_name, time, measure, value, suppressed)

  tested <- data_raw %>%
    filter(abn_lab == "Total") %>%
    mutate(
      val = trimws(v_tested),
      suppressed = val == "10 or fewer",
      value = suppressWarnings(as.numeric(ifelse(suppressed, "5", val))),
      measure = "cyclospora_tested"
    ) %>%
    select(state_name, time, measure, value, suppressed)

  total_encounters <- data_raw %>%
    filter(abn_lab == "Total") %>%
    mutate(
      val = trimws(v_total),
      suppressed = val == "10 or fewer",
      value = suppressWarnings(as.numeric(ifelse(suppressed, "5", val))),
      measure = "encounters_total"
    ) %>%
    select(state_name, time, measure, value, suppressed)

  data_long <- bind_rows(positive, tested, total_encounters)

  return(list(data = data_long, metadata = meta))
}

standardize_cyclospora_data <- function(data_long) {
  valid_states <- c(state.name, "District of Columbia")

  data_long <- data_long %>%
    mutate(
      is_national = grepl("^Total", state_name),
      state_clean = case_when(
        is_national ~ "Total",
        state_name %in% valid_states ~ state_name,
        TRUE ~ NA_character_
      )
    ) %>%
    filter(!is.na(state_clean)) %>%
    select(state_name = state_clean, measure, time, value, suppressed)

  data_long %>%
    mutate(geography_name = ifelse(state_name == "Total", "United States", state_name)) %>%
    left_join(state_fips_lookup, by = "geography_name") %>%
    filter(!is.na(geography)) %>%
    select(-state_name, -geography_name)
}

build_cyclospora_table <- function(data_long) {
  value_wide <- data_long %>%
    select(geography, time, measure, value) %>%
    pivot_wider(names_from = measure, values_from = value, values_fn = sum)

  suppressed_wide <- data_long %>%
    mutate(
      supp_col = paste0("suppressed_flag_", measure),
      suppressed = as.integer(suppressed)
    ) %>%
    select(geography, time, supp_col, suppressed) %>%
    pivot_wider(names_from = supp_col, values_from = suppressed, values_fn = max)

  value_wide %>%
    left_join(suppressed_wide, by = c("geography", "time")) %>%
    rename(
      n_cyclospora_positive = cyclospora_positive,
      n_cyclospora_tested = cyclospora_tested,
      n_encounters_total = encounters_total
    ) %>%
    mutate(
      pct_cyclospora_positive = 100 * n_cyclospora_positive / n_cyclospora_tested,
      pct_cyclospora_tested = 100 * n_cyclospora_tested / n_encounters_total
    ) %>%
    rename_with(~ paste0("epic_", .x), .cols = -c(geography, time)) %>%
    arrange(geography, time) %>%
    select(
      geography, time,
      starts_with("epic_n"),
      starts_with("epic_pct"),
      starts_with("epic_suppressed")
    )
}

cyclospora_files <- list.files("raw/staging_cyclospora_wide", "\\.xlsx$", full.names = TRUE)
cyclospora_results <- lapply(cyclospora_files, process_cyclospora_wide, password = xlsx_password)

cyclospora_metadata <- lapply(cyclospora_results, `[[`, "metadata")
jsonlite::write_json(
  cyclospora_metadata,
  "raw/staging_cyclospora_wide.json",
  auto_unbox = TRUE,
  pretty = TRUE
)

cyclospora_long <- standardize_cyclospora_data(bind_rows(lapply(cyclospora_results, `[[`, "data")))
cyclospora_standard <- build_cyclospora_table(cyclospora_long)

vroom::vroom_write(cyclospora_standard, "standard/monthly_cyclospora.csv.gz", ",")

# =============================================================================
# 4. Process weekly cyclospora + state-level "all encounters" diarrhea
#    crosstabs into standard/weekly_tests.csv.gz
# =============================================================================
# Two new weekly exports, population base "All Encounters" (not ED-specific),
# kept fully separate from the parsing/output above:
#   - raw/staging_cyclospora_weekly_wide/: the same Cyclospora lab-test
#     crosstab as monthly_cyclospora.csv.gz above, but at weekly grain, with
#     a state breakdown (no age).
#   - raw/staging_diarrhea_by_state_wide/: Diagnosis (All) x State of
#     Residence crosstab (no age breakdown) -- diarrhea across ALL encounter
#     types (not ED-specific), by state and week.
# Both report the same "Total encounters, any reason" denominator per state
# per week (verified to match exactly), so they are combined into one file,
# standard/weekly_tests.csv.gz, keyed by (geography, time). No age dimension
# exists in either source.
# =============================================================================

process_diarrhea_by_state_wide <- function(file, password = NULL) {
  message("Processing diarrhea-by-state weekly file: ", basename(file))

  decrypted_file <- tempfile(fileext = ".xlsx")
  cmd <- sprintf(
    'python -m msoffcrypto -p "%s" "%s" "%s"',
    password, normalizePath(file, winslash = "/"), decrypted_file
  )
  status <- system(cmd)
  if (status != 0) stop("Failed to decrypt: ", file)

  wb <- openxlsx2::wb_load(decrypted_file)
  all_rows <- openxlsx2::wb_to_df(
    wb, sheet = 1, col_names = FALSE,
    skip_empty_rows = FALSE, skip_empty_cols = FALSE
  )
  if (file.exists(decrypted_file)) unlink(decrypted_file)

  all_rows <- as.data.frame(
    lapply(all_rows, function(x) {
      x <- as.character(x)
      x[is.na(x)] <- ""
      x
    }),
    stringsAsFactors = FALSE
  )

  meta_fields <- c(
    "Session Title", "Session ID", "Data Model", "Population Base",
    "Population Criteria Filters", "Session Date Range", "Measure",
    "Export User", "Date of Export"
  )
  meta <- list(
    file = file,
    md5 = unname(tools::md5sum(file)),
    date_processed = as.character(Sys.time())
  )
  for (i in seq_along(meta_fields)) {
    meta[[meta_fields[i]]] <- trimws(as.character(all_rows[i, 2]))
  }

  # Row 12 cols 4-6 label each value column directly (diarrhea, None of the
  # above, Total) -- no age nesting; row 13 cols 1-3 are the id headers
  # (Year, Week, State of Residence). The "None of the above" column
  # (non-diarrhea encounters) is dropped: it's fully redundant with
  # Total - diarrhea and adds no information.
  data_raw <- all_rows[14:nrow(all_rows), , drop = FALSE]
  colnames(data_raw) <- c("year_raw", "week_raw", "state_name", "v_diarrhea", "v_none", "v_total")
  rownames(data_raw) <- NULL

  data_raw$year_raw[data_raw$year_raw == ""] <- NA
  data_raw$week_raw[data_raw$week_raw == ""] <- NA
  data_raw$state_name[data_raw$state_name == ""] <- NA
  data_raw <- tidyr::fill(data_raw, year_raw, week_raw, .direction = "down")

  # Thu-Wed 7-day weekly buckets, same as the other weekly crosstabs -- map
  # to the Saturday within each complete week.
  parsed <- parse_week_range(data_raw$week_raw, data_raw$year_raw)
  data_raw <- data_raw %>%
    mutate(start_date = parsed$start_date, n_days = parsed$n_days) %>%
    filter(n_days == 7) %>%
    mutate(
      sat_offset = (6 - as.integer(format(start_date, "%u"))) %% 7,
      time = start_date + sat_offset
    ) %>%
    select(-n_days, -sat_offset, -start_date)

  diarrhea <- data_raw %>%
    mutate(
      val = trimws(v_diarrhea),
      suppressed = val == "10 or fewer",
      value = suppressWarnings(as.numeric(ifelse(suppressed, "5", val))),
      measure = "all_diarrhea"
    ) %>%
    select(state_name, time, measure, value, suppressed)

  total_encounters <- data_raw %>%
    mutate(
      val = trimws(v_total),
      suppressed = val == "10 or fewer",
      value = suppressWarnings(as.numeric(ifelse(suppressed, "5", val))),
      measure = "encounters_total_weekly"
    ) %>%
    select(state_name, time, measure, value, suppressed)

  data_long <- bind_rows(diarrhea, total_encounters)

  return(list(data = data_long, metadata = meta))
}

process_cyclospora_weekly_wide <- function(file, password = NULL) {
  message("Processing cyclospora weekly file: ", basename(file))

  decrypted_file <- tempfile(fileext = ".xlsx")
  cmd <- sprintf(
    'python -m msoffcrypto -p "%s" "%s" "%s"',
    password, normalizePath(file, winslash = "/"), decrypted_file
  )
  status <- system(cmd)
  if (status != 0) stop("Failed to decrypt: ", file)

  wb <- openxlsx2::wb_load(decrypted_file)
  all_rows <- openxlsx2::wb_to_df(
    wb, sheet = 1, col_names = FALSE,
    skip_empty_rows = FALSE, skip_empty_cols = FALSE
  )
  if (file.exists(decrypted_file)) unlink(decrypted_file)

  all_rows <- as.data.frame(
    lapply(all_rows, function(x) {
      x <- as.character(x)
      x[is.na(x)] <- ""
      x
    }),
    stringsAsFactors = FALSE
  )

  meta_fields <- c(
    "Session Title", "Session ID", "Data Model", "Population Base",
    "Population Criteria Filters", "Session Date Range", "Measure",
    "Export User", "Date of Export"
  )
  meta <- list(
    file = file,
    md5 = unname(tools::md5sum(file)),
    date_processed = as.character(Sys.time())
  )
  for (i in seq_along(meta_fields)) {
    meta[[meta_fields[i]]] <- trimws(as.character(all_rows[i, 2]))
  }

  # Same layout as the monthly cyclospora crosstab (id cols: Year, Week,
  # Abnormal Lab Components, State of Residence; value cols: cyclospora lab
  # tests, Total) -- kept as its own function rather than generalizing
  # process_cyclospora_wide(), so the existing monthly output is untouched.
  data_raw <- all_rows[14:nrow(all_rows), , drop = FALSE]
  colnames(data_raw) <- c("year_raw", "week_raw", "abn_lab_raw", "state_name", "v_tested", "v_total")
  rownames(data_raw) <- NULL

  data_raw$year_raw[data_raw$year_raw == ""] <- NA
  data_raw$week_raw[data_raw$week_raw == ""] <- NA
  data_raw$abn_lab_raw[data_raw$abn_lab_raw == ""] <- NA
  data_raw <- tidyr::fill(data_raw, year_raw, week_raw, abn_lab_raw, .direction = "down")

  data_raw <- data_raw %>%
    mutate(abn_lab = if_else(grepl("^Total:", abn_lab_raw), "Total", abn_lab_raw))

  # Thu-Wed 7-day weekly buckets, same as the other weekly crosstabs -- map
  # to the Saturday within each complete week.
  parsed <- parse_week_range(data_raw$week_raw, data_raw$year_raw)
  data_raw <- data_raw %>%
    mutate(start_date = parsed$start_date, n_days = parsed$n_days) %>%
    filter(n_days == 7) %>%
    mutate(
      sat_offset = (6 - as.integer(format(start_date, "%u"))) %% 7,
      time = start_date + sat_offset
    ) %>%
    select(-n_days, -sat_offset, -start_date)

  positive <- data_raw %>%
    filter(abn_lab == "Cyclospora abnormal") %>%
    mutate(
      val = trimws(v_tested),
      suppressed = val == "10 or fewer",
      value = suppressWarnings(as.numeric(ifelse(suppressed, "5", val))),
      measure = "cyclospora_positive"
    ) %>%
    select(state_name, time, measure, value, suppressed)

  tested <- data_raw %>%
    filter(abn_lab == "Total") %>%
    mutate(
      val = trimws(v_tested),
      suppressed = val == "10 or fewer",
      value = suppressWarnings(as.numeric(ifelse(suppressed, "5", val))),
      measure = "cyclospora_tested"
    ) %>%
    select(state_name, time, measure, value, suppressed)

  total_encounters <- data_raw %>%
    filter(abn_lab == "Total") %>%
    mutate(
      val = trimws(v_total),
      suppressed = val == "10 or fewer",
      value = suppressWarnings(as.numeric(ifelse(suppressed, "5", val))),
      measure = "encounters_total_weekly"
    ) %>%
    select(state_name, time, measure, value, suppressed)

  data_long <- bind_rows(positive, tested, total_encounters)

  return(list(data = data_long, metadata = meta))
}

standardize_weekly_tests_data <- function(data_long) {
  valid_states <- c(state.name, "District of Columbia")

  data_long <- data_long %>%
    mutate(
      is_national = grepl("^Total", state_name),
      state_clean = case_when(
        is_national ~ "Total",
        state_name %in% valid_states ~ state_name,
        TRUE ~ NA_character_
      )
    ) %>%
    filter(!is.na(state_clean)) %>%
    select(state_name = state_clean, measure, time, value, suppressed)

  data_long %>%
    mutate(geography_name = ifelse(state_name == "Total", "United States", state_name)) %>%
    left_join(state_fips_lookup, by = "geography_name") %>%
    filter(!is.na(geography)) %>%
    select(-state_name, -geography_name)
}

build_weekly_tests_table <- function(data_long) {
  value_wide <- data_long %>%
    select(geography, time, measure, value) %>%
    # The cyclospora and diarrhea-by-state sources report the same
    # "Total encounters, any reason" value at (geography, time) -- values_fn
    # = max reconciles the duplicate key without double-counting since the
    # two sources agree exactly where they overlap.
    pivot_wider(names_from = measure, values_from = value, values_fn = max)

  suppressed_wide <- data_long %>%
    mutate(
      supp_col = paste0("suppressed_flag_", measure),
      suppressed = as.integer(suppressed)
    ) %>%
    select(geography, time, supp_col, suppressed) %>%
    pivot_wider(names_from = supp_col, values_from = suppressed, values_fn = max)

  value_wide %>%
    left_join(suppressed_wide, by = c("geography", "time")) %>%
    rename(
      n_cyclospora_positive = cyclospora_positive,
      n_cyclospora_tested = cyclospora_tested,
      n_all_diarrhea = all_diarrhea,
      n_encounters_total_weekly = encounters_total_weekly
    ) %>%
    mutate(
      pct_cyclospora_positive = 100 * n_cyclospora_positive / n_cyclospora_tested,
      pct_cyclospora_tested = 100 * n_cyclospora_tested / n_encounters_total_weekly,
      pct_all_diarrhea = 100 * n_all_diarrhea / n_encounters_total_weekly
    ) %>%
    rename_with(~ paste0("epic_", .x), .cols = -c(geography, time)) %>%
    arrange(geography, time) %>%
    select(
      geography, time,
      starts_with("epic_n"),
      starts_with("epic_pct"),
      starts_with("epic_suppressed")
    )
}

diarrhea_by_state_files <- list.files("raw/staging_diarrhea_by_state_wide", "\\.xlsx$", full.names = TRUE)
diarrhea_by_state_results <- lapply(diarrhea_by_state_files, process_diarrhea_by_state_wide, password = xlsx_password)

cyclospora_weekly_files <- list.files("raw/staging_cyclospora_weekly_wide", "\\.xlsx$", full.names = TRUE)
cyclospora_weekly_results <- lapply(cyclospora_weekly_files, process_cyclospora_weekly_wide, password = xlsx_password)

jsonlite::write_json(
  lapply(diarrhea_by_state_results, `[[`, "metadata"),
  "raw/staging_diarrhea_by_state_wide.json",
  auto_unbox = TRUE, pretty = TRUE
)
jsonlite::write_json(
  lapply(cyclospora_weekly_results, `[[`, "metadata"),
  "raw/staging_cyclospora_weekly_wide.json",
  auto_unbox = TRUE, pretty = TRUE
)

weekly_tests_long <- standardize_weekly_tests_data(
  bind_rows(
    lapply(diarrhea_by_state_results, `[[`, "data"),
    lapply(cyclospora_weekly_results, `[[`, "data")
  )
)
weekly_tests_standard <- build_weekly_tests_table(weekly_tests_long)

vroom::vroom_write(weekly_tests_standard, "standard/weekly_tests.csv.gz", ",")

# =============================================================================
# 5. Record processed state
# =============================================================================

process <- dcf::dcf_process_record()
process$vintages <- list(
  data_weekly.csv.gz = list(
    ed = wide_metadata[[1]][["Date of Export"]],
    all_encounters = all_encounters_weekly_results[[1]]$metadata[["Date of Export"]]
  ),
  monthly_cyclospora.csv.gz = cyclospora_metadata[[1]][["Date of Export"]],
  weekly_tests.csv.gz = cyclospora_weekly_results[[1]]$metadata[["Date of Export"]]
)
dcf::dcf_process_record(updated = process)
