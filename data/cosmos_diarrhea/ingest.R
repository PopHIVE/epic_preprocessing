# =============================================================================
# Epic Cosmos Diarrhea Data Ingestion
# Source: Epic SlicerDicer exports - ED visits for all-cause diarrhea
#         (ICD-10: A00-09, R19.7), weekly and monthly, by state and age
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

# Weekly export has a "Week" id column; monthly export has "Month". Detect by
# reading row 14 (id column headers) directly rather than hardcoding filenames.
detect_granularity <- function(file, password) {
  decrypted_file <- tempfile(fileext = ".xlsx")
  cmd <- sprintf(
    'python -m msoffcrypto -p "%s" "%s" "%s"',
    password, normalizePath(file, winslash = "/"), decrypted_file
  )
  system(cmd)
  wb <- openxlsx2::wb_load(decrypted_file)
  row14 <- openxlsx2::wb_to_df(
    wb, sheet = 1, rows = 14, col_names = FALSE,
    skip_empty_rows = FALSE, skip_empty_cols = FALSE
  )
  unlink(decrypted_file)
  header <- trimws(as.character(row14[1, 3]))
  if (grepl("Week", header, ignore.case = TRUE)) "week" else "month"
}

granularities <- sapply(wide_files, detect_granularity, password = xlsx_password)

wide_results <- Map(
  function(file, granularity) process_diarrhea_wide(file, granularity, password = xlsx_password),
  wide_files, granularities
)

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

weekly_long <- standardize_diarrhea_data(wide_results[[which(granularities == "week")]]$data)
monthly_long <- standardize_diarrhea_data(wide_results[[which(granularities == "month")]]$data)

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
monthly_standard <- build_standard_table(monthly_long, "monthly")

vroom::vroom_write(weekly_standard, "standard/weekly.csv.gz", ",")
vroom::vroom_write(monthly_standard, "standard/monthly.csv.gz", ",")

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
#     unlike epic_n_all_encounters_weekly/monthly above.
# Because this file has no age breakdown, it is kept as its own standardized
# output (state x month grain) rather than merged into monthly.csv.gz.
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
# 4. Record processed state
# =============================================================================

process <- dcf::dcf_process_record()
process$vintages <- list(
  weekly.csv.gz = wide_metadata[[which(granularities == "week")]][["Date of Export"]],
  monthly.csv.gz = wide_metadata[[which(granularities == "month")]][["Date of Export"]],
  monthly_cyclospora.csv.gz = cyclospora_metadata[[1]][["Date of Export"]]
)
dcf::dcf_process_record(updated = process)
