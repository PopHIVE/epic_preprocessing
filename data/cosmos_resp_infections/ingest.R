# =============================================================================
# Epic Cosmos Respiratory Infections Data Ingestion
# Source: Epic SlicerDicer exports - Weekly ED visits (COVID, Flu, RSV)
#         and monthly RSV test positivity
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
# Function: Parse wide-format Epic SlicerDicer CSV
# =============================================================================
# New format: all outcomes (RSV, COVID, flu, all-cause) in one file.
# Row 12: outcome labels (fill rightward)
# Row 13: year/season labels (fill rightward)
# Row 14: week date ranges
# Row 15: column headers (State of Residence, Age at Time of Visit)
# Row 16+: data rows (state fills down, age per row)
# =============================================================================

process_wide_epic_file <- function(file, password = NULL) {
  message("Processing wide file: ", basename(file))

  is_xlsx <- grepl("\\.xlsx$", file, ignore.case = TRUE)

  if (is_xlsx) {
    # --- Decrypt password-protected xlsx, then read via openxlsx2 ---
    if (!is.null(password) && nzchar(password)) {
      decrypted_file <- tempfile(fileext = ".xlsx")
      cmd <- sprintf(
        'python -m msoffcrypto -p "%s" "%s" "%s"',
        password, normalizePath(file, winslash = "/"), decrypted_file
      )
      status <- system(cmd)
      if (status != 0) stop("Failed to decrypt: ", file)
      load_file <- decrypted_file
    } else {
      load_file <- file
    }
    wb <- openxlsx2::wb_load(load_file)
    all_rows <- openxlsx2::wb_to_df(
      wb, sheet = 1, col_names = FALSE,
      skip_empty_rows = FALSE, skip_empty_cols = FALSE
    )
    if (exists("decrypted_file") && file.exists(decrypted_file)) {
      unlink(decrypted_file)
    }
    # Convert all columns to character; replace NA with ""
    all_rows <- as.data.frame(
      lapply(all_rows, function(x) {
        x <- as.character(x)
        x[is.na(x)] <- ""
        x
      }),
      stringsAsFactors = FALSE
    )

    # --- Extract metadata from header rows 1-8 ---
    meta_fields <- c(
      "Session Title", "Session ID", "Data Model", "Population Base",
      "Population Criteria Filters", "Session Date Range",
      "Export User", "Date of Export"
    )
    meta <- list(
      file = file,
      md5 = tools::md5sum(file),
      date_processed = as.character(Sys.time())
    )
    names(meta$md5) <- NULL
    for (i in seq_along(meta_fields)) {
      val <- trimws(as.character(all_rows[i, 2]))
      if (is.na(val) || val == "") val <- ""
      meta[[meta_fields[i]]] <- val
    }

    # Extract header rows (rows 12, 13, 14 in the original spreadsheet)
    row12 <- as.character(all_rows[12, ])
    row13 <- as.character(all_rows[13, ])
    row14 <- as.character(all_rows[14, ])

    # Extract data rows (row 16 onward)
    data_raw <- all_rows[16:nrow(all_rows), , drop = FALSE]
    rownames(data_raw) <- NULL

  } else {
    # --- Read CSV (legacy path) ---
    raw_lines <- readLines(file, warn = FALSE)
    raw_lines <- gsub("\x96", "-", raw_lines, useBytes = TRUE)

    parse_csv_row <- function(line) {
      row <- read.csv(
        text = line, header = FALSE,
        stringsAsFactors = FALSE, check.names = FALSE
      )
      sapply(row[1, ], function(x) {
        if (is.na(x)) "" else as.character(x)
      }, USE.NAMES = FALSE)
    }

    # --- Extract metadata from header rows 1-8 ---
    meta_fields <- c(
      "Session Title", "Session ID", "Data Model", "Population Base",
      "Population Criteria Filters", "Session Date Range",
      "Export User", "Date of Export"
    )
    meta <- list(
      file = file,
      md5 = tools::md5sum(file),
      date_processed = as.character(Sys.time())
    )
    names(meta$md5) <- NULL
    for (i in seq_along(meta_fields)) {
      parsed <- parse_csv_row(raw_lines[i])
      val <- if (length(parsed) >= 2) trimws(parsed[2]) else ""
      if (is.na(val)) val <- ""
      meta[[meta_fields[i]]] <- val
    }

    row12 <- parse_csv_row(raw_lines[12])
    row13 <- parse_csv_row(raw_lines[13])
    row14 <- parse_csv_row(raw_lines[14])

    data_raw <- read.csv(
      file, skip = 15, header = FALSE,
      stringsAsFactors = FALSE, check.names = FALSE,
      fileEncoding = "windows-1252"
    )
  }

  # =========================================================================
  # Common processing for both xlsx and csv
  # =========================================================================

  # Remove first 2 columns (state, age position labels) from headers
  n_cols <- min(length(row12), length(row13), length(row14)) - 2
  outcomes <- row12[-(1:2)][1:n_cols]
  years_raw <- row13[-(1:2)][1:n_cols]
  weeks_raw <- row14[-(1:2)][1:n_cols]

  # --- Fill outcome labels rightward ---
  outcomes <- trimws(outcomes)
  outcomes[outcomes == ""] <- NA
  outcomes <- zoo::na.locf(outcomes, na.rm = FALSE)

  # Map to standard column names
  outcome_std <- case_when(
    grepl("RSV", outcomes, ignore.case = TRUE)   ~ "n_rsv",
    grepl("COVID", outcomes, ignore.case = TRUE)  ~ "n_covid",
    grepl("flu", outcomes, ignore.case = TRUE)    ~ "n_flu",
    grepl("Total", outcomes)                      ~ "n_all_encounters_weekly",
    TRUE ~ NA_character_
  )

  # --- Fill year/season labels rightward ---
  years_raw <- trimws(years_raw)
  years_raw[years_raw == ""] <- NA
  years_raw <- zoo::na.locf(years_raw, na.rm = FALSE)

  # Parse first and last 4-digit year from season string
  # e.g. "2024 – 2025" -> year1=2024, year2=2025; "2025" -> year1=year2=2025
  year1 <- as.integer(stringr::str_extract(years_raw, "^\\d{4}"))
  year2 <- as.integer(stringr::str_extract(years_raw, "\\d{4}$"))
  is_cross_year <- year1 != year2

  # --- Parse end date from week ranges ---
  # Extract the LAST "Mon DD" pattern: "Dec 29 – Jan 4" -> "Jan" + "4"
  # This avoids needing to handle the en-dash character encoding
  end_match <- stringr::str_match(
    weeks_raw,
    "(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\s+(\\d+)\\s*$"
  )
  end_month_str <- end_match[, 2]
  end_day <- as.integer(end_match[, 3])
  end_month_num <- match(end_month_str, month.abb)

  # Determine year: for cross-year seasons, Dec end dates -> year1, else -> year2
  end_year <- ifelse(is_cross_year & end_month_num == 12, year1, year2)
  end_date <- as.Date(paste(end_year, end_month_num, end_day, sep = "-"))

  # Build column metadata
  col_meta <- data.frame(
    col_idx = seq_len(n_cols),
    outcome = outcome_std,
    end_date = end_date,
    stringsAsFactors = FALSE
  )

  # --- Clean data rows ---
  # Ensure all columns are character (xlsx may have mixed types)
  data_raw <- as.data.frame(
    lapply(data_raw, as.character),
    stringsAsFactors = FALSE
  )

  # Clean first two columns (state, age)
  data_raw[[1]] <- trimws(data_raw[[1]])
  data_raw[[2]] <- trimws(data_raw[[2]])

  # Fill state name down (blank = same state as above)
  data_raw[[1]][data_raw[[1]] == ""] <- NA
  colnames(data_raw)[1:2] <- c("state_name", "age")
  data_raw <- tidyr::fill(data_raw, state_name, .direction = "down")

  # --- Name data columns and trim to match metadata ---
  n_data <- min(ncol(data_raw) - 2, n_cols)
  dc <- paste0("d", seq_len(n_data))
  colnames(data_raw)[3:(n_data + 2)] <- dc
  data_raw <- data_raw[, c("state_name", "age", dc)]

  # --- Pivot to long format ---
  data_long <- data_raw %>%
    tidyr::pivot_longer(
      cols = all_of(dc),
      names_to = "col_name",
      values_to = "val"
    ) %>%
    mutate(col_idx = as.integer(sub("d", "", col_name))) %>%
    left_join(col_meta, by = "col_idx") %>%
    filter(!is.na(outcome), !is.na(end_date)) %>%
    mutate(
      val = trimws(val),
      suppressed = val == "10 or fewer",
      value = suppressWarnings(as.numeric(ifelse(suppressed, "5", val)))
    ) %>%
    select(state_name, age, outcome, time = end_date, value, suppressed)

  return(list(data = data_long, metadata = meta))
}


# =============================================================================
# 1. Process wide-format files from raw/staging_resp_infections_wide/
# =============================================================================

# Password for xlsx files from environment variable
xlsx_password <- Sys.getenv("EPIC_XLSX_PASSWORD")  # add password to enviornment using usethis::edit_r_environ()

wide_files <- list.files(
  "raw/staging_resp_infections_wide", "\\.(csv|xlsx)$", full.names = TRUE
)
wide_results <- lapply(wide_files, process_wide_epic_file, password = xlsx_password)
wide_combined <- bind_rows(lapply(wide_results, `[[`, "data"))

# Save metadata from wide-format file headers to JSON
wide_metadata <- lapply(wide_results, `[[`, "metadata")
jsonlite::write_json(
  wide_metadata,
  "raw/staging_resp_infections_wide.json",
  auto_unbox = TRUE,
  pretty = TRUE
)

# --- Filter to US states + DC + national total ---
valid_states <- c(state.name, "District of Columbia")

wide_combined <- wide_combined %>%
  mutate(
    # National total rows: state starts with "Total:" (no "bucket" = not per-state subtotal)
    is_national = grepl("^Total:", state_name),
    # Per-state age subtotal: age starts with "Total:"
    is_age_total = grepl("^Total:", age),
    # Clean state name
    state_clean = case_when(
      is_national ~ "Total",
      state_name %in% valid_states ~ state_name,
      TRUE ~ NA_character_
    ),
    # Clean age: "Total:..." -> "Total"
    age_clean = ifelse(is_age_total, "Total", age)
  ) %>%
  filter(!is.na(state_clean)) %>%
  select(state_name = state_clean, age = age_clean, outcome, time, value, suppressed)

# --- Standardize age groups ---
# Raw ages: "Less than 1 Years", "? 1 and < 5 Years" (≥ as ?), "65 Years or more"
# The raw data uses exclusive upper bounds (< 5, < 18, etc.) so we subtract 1
# to get inclusive ranges: 1-4, 5-17, 18-49, 50-64
wide_combined <- wide_combined %>%
  mutate(
    age = trimws(age),
    # "Less than 1 Years" -> "<1 Years"
    age = stringr::str_replace(age, "^Less than\\s+(\\d+).*$", "<\\1 Years"),
    # "65 Years or more" -> "65+ Years"
    age = stringr::str_replace(age, "^(\\d+)\\s+Years or more$", "\\1+ Years"),
    # "? X and < Y Years" -> "X-(Y-1) Years" (subtract 1 from exclusive upper bound)
    age = {
      m <- stringr::str_match(age, "^[^0-9]*?(\\d+)\\s+and\\s+<\\s*(\\d+)\\s*Years?$")
      lower <- m[, 2]
      upper <- as.character(as.integer(m[, 3]) - 1L)
      ifelse(!is.na(lower), paste0(lower, "-", upper, " Years"), age)
    }
  )

# --- Map state names to FIPS geography codes ---
wide_combined <- wide_combined %>%
  mutate(
    geography_name = ifelse(state_name == "Total", "United States", state_name)
  ) %>%
  left_join(state_fips_lookup, by = "geography_name") %>%
  filter(!is.na(geography)) %>%
  select(-state_name, -geography_name)

# --- Pivot wider: one row per (geography, age, time) ---
value_wide <- wide_combined %>%
  select(geography, age, time, outcome, value) %>%
  pivot_wider(names_from = outcome, values_from = value, values_fn = sum)

suppressed_wide <- wide_combined %>%
  mutate(
    supp_col = sub("^n_", "suppressed_flag_", outcome),
    suppressed = as.integer(suppressed)
  ) %>%
  select(geography, age, time, supp_col, suppressed) %>%
  pivot_wider(names_from = supp_col, values_from = suppressed, values_fn = max)

merged_weekly <- value_wide %>%
  left_join(suppressed_wide, by = c("geography", "age", "time"))


# =============================================================================
# 2. Compute percentages and apply filters
# =============================================================================

merged_weekly <- merged_weekly %>%
  filter(!is.na(age)) %>%
  mutate(
    pct_rsv   = 100 * n_rsv / n_all_encounters_weekly,
    pct_flu   = 100 * n_flu / n_all_encounters_weekly,
    pct_covid = 100 * n_covid / n_all_encounters_weekly
  ) %>%
  # Add epic_ prefix to all columns except geography, time, age
  rename_with(
    ~ paste0("epic_", .x),
    .cols = -c(geography, time, age)
  ) %>%
  arrange(geography, age, time) %>%
  group_by(geography, age) %>%
  mutate(
    time = as.Date(time),
    epic_n_all_encounters_lag1 = lag(epic_n_all_encounters_weekly, 1),
    remove = if_else(
      epic_n_all_encounters_weekly / epic_n_all_encounters_lag1 < 0.5 &
        time == max(time, na.rm = TRUE), 1, 0
    )
  ) %>%
  filter(remove != 1) %>%
  dplyr::select(-remove, -epic_n_all_encounters_lag1) %>%
  dplyr::select(
    geography, age, time,
    starts_with("epic_n"),
    starts_with("epic_pct"),
    starts_with("epic_suppressed")
  )


vroom::vroom_write(
  merged_weekly,
  "standard/weekly.csv.gz",
  ","
)


# =============================================================================
# 3. Process old-format staging data (long format, separate files per outcome)
# =============================================================================

raw1 <- dcf::dcf_process_epic_staging(cleanup=F, staging_dir = "raw/staging_resp_infections",)

process <- dcf::dcf_process_record()
meta_files <- list.files("raw", "json", full.names = TRUE)
raw_vintages <- lapply(
  structure(
    meta_files,
    names = gsub(".*/|\\..*", "", meta_files)
  ),
  function(meta_file) {
    meta <- jsonlite::read_json(meta_file)[[1L]]
    as.character(as.Date(meta[["Date of Export"]], "%m/%d/%Y"))
  }
)

# Load and standardize all raw .csv.xz files
files <- list.files("raw", "\\.csv\\.xz", full.names = TRUE)

data <- lapply(files, function(file) {

  d <- vroom::vroom(file, show_col_types = FALSE, guess_max = Inf)

  d2 <- dcf::dcf_standardize_epic(d)

  if('month' %in% names(d)){
    d2$time = paste0(d2$time, '-01')
  }

  if('Year' %in% names(d2) & !('time' %in% names(d2)) ){
    first_date <- mdy(strsplit(d2$Year, " - ")[[1]][1])
    d2$time = paste0(lubridate::year(first_date), '-01-01')
  }

  if ("geography" %in% names(d2)) {
    d2 <- d2 %>%
      mutate(geography = if_else(geography=='0','00', geography)
      )
  }

  if ("time" %in% names(d2)) {

    d2 <- d2 %>%
      mutate(time = as.Date(time),
             weekday = weekdays(time),
             time = if_else(weekday=='Sunday', time+6, time), #week end date
             time = as.character(time)
      ) %>%
      dplyr::select(-weekday)
  }

  if ("age" %in% names(d2)) {
    d2 <- d2 %>%
      mutate(age = stringr::str_replace(age, "^Less than\\s+(\\d+)", "<\\1 Years"),
             age = stringr::str_replace(age, "^(\\d+) or more$", "≥\\1 Years"),
             age = stringr::str_replace(age, "^≥\\s*(\\d+) and <\\s*(\\d+)$", "\\1-\\2 Years"),
             age = if_else(
               age=="1-5 Years", "1-4 Years",
               if_else( age=="5-18 Years", "5-17 Years",
                        if_else(age=="18-25 Years", "18-24 Years",
                                if_else(age=="18-50 Years", "18-49 Years",
                                        if_else( age=="25-35 Years" , "25-34 Years",
                                                 if_else( age=="35-45 Years", "35-44 Years",
                                                          if_else( age=="45-55 Years", "45-54 Years",
                                                                   if_else( age=="50-65 Years", "50-64 Years",
                                                                            if_else( age=="55-65 Years", "55-64 Years",
                                                                                     if_else( age=="≥65 Years", "65+ Years",
                                                                                              age
                                                                                     ))))))))))

      )
  }

  return(d2)
})
names(data) <- sub("\\..*", "", basename(files))

# -----------------------------------------------------------------------------
# 4. RSV test data (monthly)
# -----------------------------------------------------------------------------
monthly_tests <- data[["rsv_tests"]] %>%
  filter(!is.na(age))%>%
  rename(
    epic_pct_j12_j18_tested_rsv = rsv_tests ,
    epic_pct_rsv_pos_tests ='positive_rsv_tests_(%)',
    epic_n_ed_j12_j18 = n_rsv_tests

  ) %>%
  mutate(
    suppressed_rsv_test =     if_else(epic_n_ed_j12_j18 == 5, 1, 0),

    epic_pct_rsv_pos_tests = gsub('%','',epic_pct_rsv_pos_tests),
    epic_pct_rsv_pos_tests = as.numeric(epic_pct_rsv_pos_tests),

    epic_pct_j12_j18_tested_rsv = gsub('%','',epic_pct_j12_j18_tested_rsv),
    epic_pct_j12_j18_tested_rsv = as.numeric(epic_pct_j12_j18_tested_rsv)

  ) %>%
  dplyr::select(time, geography, age,epic_pct_rsv_pos_tests, epic_pct_j12_j18_tested_rsv,epic_n_ed_j12_j18, suppressed_rsv_test)


vroom::vroom_write(
  monthly_tests,
  "standard/monthly_tests.csv.gz",
  ","
)

vroom::vroom_write(data$rsv_tests, "standard/no_geo.csv.gz", ",")

# -----------------------------------------------------------------------------
# 5. Record processed state
# -----------------------------------------------------------------------------
process$vintages <- list(
  weekly.csv.gz = max(unlist(raw_vintages[c(
    "all_encounters",
    "covid",
    "flu",
    "rsv"
  )])),
  no_geo.csv.gz = raw_vintages$rsv_tests
)
dcf::dcf_process_record(updated = process)
