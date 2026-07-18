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
# Shared helpers
# =============================================================================
# All of the raw exports below are Epic SlicerDicer crosstabs: password-
# protected xlsx files with a 9-row metadata header, a blank row or two, then
# a wide table of state/time x measure counts. These helpers factor out the
# steps that are identical across every crosstab, leaving only the
# layout-specific column parsing in each process_*() function below.
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

# Epic's weekly buckets are Sunday-Saturday spans, matching the project's
# Saturday-end-of-week convention. Partial weeks (<7 days) at the start/end
# of the exported date range are dropped.
filter_full_weeks <- function(data, week_col, year_col) {
  parsed <- parse_week_range(data[[week_col]], data[[year_col]])
  data %>%
    mutate(time = parsed$end_date, n_days = parsed$n_days) %>%
    filter(n_days == 7) %>%
    select(-n_days)
}

# Decrypt a password-protected SlicerDicer xlsx export, load its raw grid
# (as character, blanks instead of NA), and pull the 9-field metadata block
# out of rows 1-9.
load_slicerdicer_xlsx <- function(file, password) {
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

  list(all_rows = all_rows, meta = meta)
}

# Epic suppresses counts <= 10 as the string "10 or fewer"; these are
# imputed as 5 in `value` and flagged via `suppressed`.
is_suppressed <- function(val) trimws(val) == "10 or fewer"

unsuppress_value <- function(val) {
  val <- trimws(val)
  suppressWarnings(as.numeric(ifelse(val == "10 or fewer", "5", val)))
}

# Raw ages use exclusive upper bounds ("< 5", "< 18", etc.), so subtract 1
# from the upper bound to get inclusive ranges: 1-4, 5-17, 18-49, 50-64
standardize_age_labels <- function(age) {
  age <- trimws(age)
  age <- stringr::str_replace(age, "^Less than\\s+(\\d+)(?:\\s+Years?)?$", "<\\1 Years")
  age <- stringr::str_replace(age, "^(\\d+)\\s+(?:Years\\s+)?or more$", "\\1+ Years")
  m <- stringr::str_match(age, "^[^0-9]*?(\\d+)\\s+and\\s+<\\s*(\\d+)(?:\\s*Years?)?$")
  lower <- m[, 2]
  upper <- as.character(as.integer(m[, 3]) - 1L)
  ifelse(!is.na(lower), paste0(lower, "-", upper, " Years"), age)
}

# Classify `state_name` as a US state/DC or the "Total" (national) row, drop
# everything else (territories, other countries, catch-all rows), then join
# to FIPS via `geography_name` ("Total" -> "United States").
map_state_to_geography <- function(data, state_col = "state_name") {
  valid_states <- c(state.name, "District of Columbia")

  data %>%
    mutate(
      state_clean = case_when(
        grepl("^Total", .data[[state_col]]) ~ "Total",
        .data[[state_col]] %in% valid_states ~ .data[[state_col]],
        TRUE ~ NA_character_
      )
    ) %>%
    filter(!is.na(state_clean)) %>%
    mutate(geography_name = ifelse(state_clean == "Total", "United States", state_clean)) %>%
    left_join(state_fips_lookup, by = "geography_name") %>%
    filter(!is.na(geography)) %>%
    select(-any_of(c(state_col, "state_clean", "geography_name")))
}

# Pull a single suppression-coded measure column (e.g. "v_diarrhea") out of a
# wide data_raw frame into long (state_name, time, measure, value, suppressed).
extract_measure <- function(data, val_col, measure_name) {
  data %>%
    mutate(
      value = unsuppress_value(.data[[val_col]]),
      suppressed = is_suppressed(.data[[val_col]]),
      measure = measure_name
    ) %>%
    select(state_name, time, measure, value, suppressed)
}

# Cyclospora crosstab: "Cyclospora abnormal" gives positives, "Total" gives
# both tests performed (v_tested) and the population denominator (v_total).
extract_cyclospora_measures <- function(data_raw, total_measure_name) {
  bind_rows(
    data_raw %>% filter(abn_lab == "Cyclospora abnormal") %>%
      extract_measure("v_tested", "cyclospora_positive"),
    data_raw %>% filter(abn_lab == "Total") %>%
      extract_measure("v_tested", "cyclospora_tested"),
    data_raw %>% filter(abn_lab == "Total") %>%
      extract_measure("v_total", total_measure_name)
  )
}

# Pivot a standardized long table (one row per group x category) into wide
# value + suppressed-flag columns, joined on the grouping columns.
pivot_epic_measures <- function(data_long, group_cols, category_col, value_fn = sum) {
  value_wide <- data_long %>%
    select(all_of(group_cols), value, category = all_of(category_col)) %>%
    pivot_wider(names_from = category, values_from = value, values_fn = value_fn)

  suppressed_wide <- data_long %>%
    mutate(
      category = paste0("suppressed_flag_", .data[[category_col]]),
      suppressed = as.integer(suppressed)
    ) %>%
    select(all_of(group_cols), category, suppressed) %>%
    pivot_wider(names_from = category, values_from = suppressed, values_fn = max)

  value_wide %>% left_join(suppressed_wide, by = group_cols)
}

write_metadata_json <- function(results, path) {
  jsonlite::write_json(
    lapply(results, `[[`, "metadata"),
    path,
    auto_unbox = TRUE,
    pretty = TRUE
  )
}

# =============================================================================
# 1. Process wide-format ED diarrhea files from raw/staging_diarrhea_wide/
# =============================================================================
# Layout: outcome blocks (diarrhea, Total) across columns, each block spanning
# 7 age columns. Rows are one per (State of Residence, Year, Week/Month).
# Row 12: outcome labels (fill rightward, one label per block)
# Row 13: age labels (one per column, repeats within each block)
# Row 14: id column headers (State of Residence, Year, Week or Month)
# Row 15+: data rows (state and year fill down, week/month per row)
# =============================================================================

process_diarrhea_wide <- function(file, granularity, password = NULL) {
  message("Processing ", granularity, " file: ", basename(file))

  loaded <- load_slicerdicer_xlsx(file, password)
  all_rows <- loaded$all_rows
  meta <- loaded$meta

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
    data_long <- filter_full_weeks(data_long, "time_raw", "year_raw")
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
      value = unsuppress_value(val),
      suppressed = is_suppressed(val)
    ) %>%
    select(state_name, age_raw, outcome, time, value, suppressed)

  return(list(data = data_long, metadata = meta))
}

xlsx_password <- Sys.getenv("EPIC_XLSX_PASSWORD")  # add password to environment using usethis::edit_r_environ()

wide_files <- list.files("raw/staging_diarrhea_wide", "\\.xlsx$", full.names = TRUE)

# Only the weekly export is retained in this folder now (monthly removed).
wide_results <- lapply(wide_files, process_diarrhea_wide, granularity = "week", password = xlsx_password)
write_metadata_json(wide_results, "raw/staging_diarrhea_wide.json")

standardize_diarrhea_data <- function(data_long) {
  data_long %>%
    mutate(
      # "Total: ..." -> "Total"; numeric ranges standardized below
      age = ifelse(grepl("^Total:", age_raw), "Total", age_raw),
      age = standardize_age_labels(age)
    ) %>%
    select(state_name, age, outcome, time, value, suppressed) %>%
    map_state_to_geography()
}

weekly_long <- standardize_diarrhea_data(wide_results[[1]]$data)

# =============================================================================
# 2. Pivot to wide standard format and compute percentages
# =============================================================================

build_standard_table <- function(data_long, suffix) {
  pivot_epic_measures(data_long, c("geography", "age", "time"), "outcome") %>%
    filter(!is.na(age)) %>%
    rename(
      n_diarrhea = diarrhea,
      !!paste0("n_all_encounters_", suffix) := all_encounters
    ) %>%
    mutate(
      pct_diarrhea = 100 * n_diarrhea / .data[[paste0("n_all_encounters_", suffix)]]
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
}

weekly_standard <- build_standard_table(weekly_long, "weekly")

# =============================================================================
# 2b. Process the state x age x week "all encounters" (non-ED) diarrhea
#     crosstab and merge it with the ED weekly measure into one standard file,
#     standard/data_weekly.csv.gz. The two sources are exported separately and
#     can have different most-recent complete weeks. A full join on
#     (geography, age, time) merges them without truncating either source to
#     the other's max date -- weeks present in only one source get NA for the
#     other source's columns rather than being silently dropped.
# =============================================================================

process_diarrhea_all_encounters_weekly_wide <- function(file, password = NULL) {
  message("Processing diarrhea all-encounters (state x age) weekly file: ", basename(file))

  loaded <- load_slicerdicer_xlsx(file, password)
  all_rows <- loaded$all_rows
  meta <- loaded$meta

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

  data_long <- filter_full_weeks(data_long, "week_raw", "year_raw")

  data_long <- data_long %>%
    mutate(
      value = unsuppress_value(val),
      suppressed = is_suppressed(val),
      age = trimws(age_raw)
    ) %>%
    # "No value" = encounters with undocumented age; no equivalent bucket in
    # the ED crosstab, so dropped rather than merged as a fabricated group.
    filter(age != "No value") %>%
    mutate(age = standardize_age_labels(age)) %>%
    select(state_name, age, outcome, time, value, suppressed)

  return(list(data = data_long, metadata = meta))
}

standardize_all_encounters_geo <- function(data_long) {
  map_state_to_geography(data_long) %>%
    select(geography, age, outcome, time, value, suppressed)
}

build_all_encounters_weekly_table <- function(data_long) {
  pivot_epic_measures(data_long, c("geography", "age", "time"), "outcome") %>%
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
write_metadata_json(all_encounters_weekly_results, "raw/staging_diarrhea_all_encounters_weekly_wide.json")

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

standardize_measure_geo <- function(data_long) {
  map_state_to_geography(data_long) %>%
    select(geography, measure, time, value, suppressed)
}

# =============================================================================
# 3. Combine weekly cyclospora lab tests with the state-level "all
#    encounters" diarrhea totals into standard/weekly_tests.csv.gz
# =============================================================================
# raw/staging_diarrhea_all_encounters_weekly_wide/ (section 2b above) already
# contains an age == "Total" row per state/week/outcome -- the all-ages
# diarrhea and total-encounters counts -- so those two measures are reused
# from all_encounters_weekly_long below rather than re-parsed from a separate
# by-state-only crosstab (confirmed identical, aside from +/-1 noise between
# export snapshots, against raw/staging_diarrhea_by_state_wide/ before it was
# dropped). raw/staging_cyclospora_weekly_wide/ has no age dimension and no
# equivalent elsewhere, so it's still parsed directly. Both sources report
# the same "Total encounters, any reason" denominator per state per week, so
# they are combined into one file, standard/weekly_tests.csv.gz, keyed by
# (geography, time).
# =============================================================================

process_cyclospora_weekly_wide <- function(file, password = NULL) {
  message("Processing cyclospora weekly file: ", basename(file))

  loaded <- load_slicerdicer_xlsx(file, password)
  all_rows <- loaded$all_rows
  meta <- loaded$meta

  # id cols: Year, Week, Abnormal Lab Components, State of Residence; value
  # cols: cyclospora lab tests, Total
  data_raw <- all_rows[14:nrow(all_rows), , drop = FALSE]
  colnames(data_raw) <- c("year_raw", "week_raw", "abn_lab_raw", "state_name", "v_tested", "v_total")
  rownames(data_raw) <- NULL

  data_raw$year_raw[data_raw$year_raw == ""] <- NA
  data_raw$week_raw[data_raw$week_raw == ""] <- NA
  data_raw$abn_lab_raw[data_raw$abn_lab_raw == ""] <- NA
  data_raw <- tidyr::fill(data_raw, year_raw, week_raw, abn_lab_raw, .direction = "down")

  data_raw <- data_raw %>%
    mutate(abn_lab = if_else(grepl("^Total:", abn_lab_raw), "Total", abn_lab_raw))

  data_raw <- filter_full_weeks(data_raw, "week_raw", "year_raw")

  data_long <- extract_cyclospora_measures(data_raw, "encounters_total_weekly")

  return(list(data = data_long, metadata = meta))
}

build_weekly_tests_table <- function(data_long) {
  # The cyclospora and all-encounters (age == "Total") sources report the
  # same "Total encounters, any reason" value at (geography, time) --
  # values_fn = max reconciles the duplicate key without double-counting
  # since the two sources agree exactly where they overlap.
  pivot_epic_measures(data_long, c("geography", "time"), "measure", value_fn = max) %>%
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

cyclospora_weekly_files <- list.files("raw/staging_cyclospora_weekly_wide", "\\.xlsx$", full.names = TRUE)
cyclospora_weekly_results <- lapply(cyclospora_weekly_files, process_cyclospora_weekly_wide, password = xlsx_password)
write_metadata_json(cyclospora_weekly_results, "raw/staging_cyclospora_weekly_wide.json")

# All-ages diarrhea/total-encounters rows already computed in section 2b --
# reused here instead of parsing a duplicate by-state-only crosstab.
all_encounters_total_age_long <- all_encounters_weekly_long %>%
  filter(age == "Total") %>%
  select(geography, measure = outcome, time, value, suppressed)

weekly_tests_long <- bind_rows(
  all_encounters_total_age_long,
  standardize_measure_geo(bind_rows(lapply(cyclospora_weekly_results, `[[`, "data")))
)
weekly_tests_standard <- build_weekly_tests_table(weekly_tests_long)

vroom::vroom_write(weekly_tests_standard, "standard/weekly_tests.csv.gz", ",")

# =============================================================================
# 4. Record processed state
# =============================================================================

process <- dcf::dcf_process_record()
process$vintages <- list(
  data_weekly.csv.gz = list(
    ed = wide_results[[1]]$metadata[["Date of Export"]],
    all_encounters = all_encounters_weekly_results[[1]]$metadata[["Date of Export"]]
  ),
  weekly_tests.csv.gz = list(
    all_encounters = all_encounters_weekly_results[[1]]$metadata[["Date of Export"]],
    cyclospora = cyclospora_weekly_results[[1]]$metadata[["Date of Export"]]
  )
)
dcf::dcf_process_record(updated = process)
