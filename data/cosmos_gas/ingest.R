# =============================================================================
# Epic Cosmos Group A Streptococcus (GAS) Data Ingestion
# Source: Epic Cosmos SlicerDicer exports (raw/staging/*.xlsx)
#         Quarterly patient counts with a strep throat diagnosis, by state and age
#
# SlicerDicer session 2809857, "Number of Patients by State of Residence and
# Diagnosis (All) and Age at Encounter in Years Range":
#   Data model      : Patients
#   Population base : All Patients  (NOT emergency department only)
#   Criteria        : Country of Care = United States of America; Has Any Encounters
#   Measure         : Number of Patients
#   Column groups   : "strep throat" (numerator) and "Total" (denominator)
#
# The numerator is Epic's "strep throat" diagnosis grouping, built around:
#   J02.0  - Streptococcal pharyngitis
#   J03.00 - Acute streptococcal tonsillitis, unspecified
#   J03.01 - Acute recurrent streptococcal tonsillitis
#
# Output (PopHIVE wide format, standard/data.csv.gz):
#   index   : geography (FIPS string, "00" = national), time, age
#   measures: epic_n_strep_throat, epic_n_patients, epic_pct_strep_throat
#             each immediately followed by its own *_suppressed_flag
#
# Conventions applied here:
#   - time is the LAST day of the quarter, formatted YYYY-mm-dd; partial
#     quarters at the edges of the exported range are dropped
#   - state_name is resolved to a FIPS `geography` and then dropped
#   - suppression is handled per measure (blank / "10 or fewer" -> 5, flag 1)
#   - the strep throat measure is a PERCENT of patients, not a rate per 100,000
# =============================================================================

library(dplyr)
library(tidyr)
library(stringr)
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
all_fips <- vroom::vroom(
  "../../resources/all_fips.csv.gz",
  show_col_types = FALSE
)

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
# 1. Read raw SlicerDicer staging files
# =============================================================================
# Place exported .xlsx files from Epic Cosmos SlicerDicer into raw/staging/
# Expected export layout (rows):
#   1-8:   Metadata header (Session Title, ID, Population Base, etc.)
#   9-10:  Blank
#   11:    Sub-header / column group labels
#   12:    Column names (Year, Quarter, State of Residence, Age, counts)
#   13+:   Data rows

staging_files <- list.files("raw/staging", pattern = "\\.(csv|xlsx)$", full.names = TRUE)

if (length(staging_files) == 0) {
  stop(
    "No staging files found in raw/staging/.\n",
    "Export data from Epic Cosmos SlicerDicer and place .xlsx files there."
  )
}

read_gas_staging <- function(file, password = NULL) {
  message("Reading: ", basename(file))

  is_xlsx <- grepl("\\.xlsx$", file, ignore.case = TRUE)

  if (is_xlsx) {
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

    wb <- openxlsx2::wb_load(decrypted)
    all_rows <- openxlsx2::wb_to_df(
      wb, sheet = 1, col_names = FALSE,
      skip_empty_rows = FALSE, skip_empty_cols = FALSE
    )
    all_rows <- as.data.frame(
      lapply(all_rows, function(x) { x <- as.character(x); x[is.na(x)] <- ""; x }),
      stringsAsFactors = FALSE
    )
  } else {
    all_rows <- read.csv(
      file, header = FALSE, stringsAsFactors = FALSE, check.names = FALSE,
      fileEncoding = "windows-1252"
    )
    all_rows <- as.data.frame(
      lapply(all_rows, function(x) { x <- as.character(x); x[is.na(x)] <- ""; x }),
      stringsAsFactors = FALSE
    )
  }

  # Echo the session metadata so the export stays traceable in the run log
  meta_field <- function(label) {
    hit <- which(trimws(all_rows[[1]]) == label)
    if (length(hit) == 0 || ncol(all_rows) < 2) return(NA_character_)
    trimws(all_rows[hit[1], 2])
  }
  for (lbl in c("Session ID", "Population Base", "Session Date Range")) {
    val <- meta_field(lbl)
    if (!is.na(val)) message("  ", lbl, ": ", val)
  }

  # Data starts at row 13 (0-indexed row 12 in R after skip)
  data_raw <- all_rows[14:nrow(all_rows), , drop = FALSE]
  rownames(data_raw) <- NULL

  # Assign column names: Year, Quarter, State, Age, strep count, patient count
  # Adjust col.names below to match your specific SlicerDicer export layout
  n_cols <- ncol(data_raw)
  base_names <- c("year", "quarter", "state_name", "age", "n_strep_throat", "n_patients")
  col_names <- c(base_names, paste0("extra_", seq_len(max(0, n_cols - length(base_names)))))
  colnames(data_raw) <- col_names[seq_len(n_cols)]

  data_raw
}

raw_list <- lapply(staging_files, read_gas_staging, password = xlsx_password)
data_raw <- bind_rows(raw_list)

# Track raw file state for change detection
current_state <- list(
  files = staging_files,
  hashes = unname(tools::md5sum(staging_files))
)

# =============================================================================
# 2. Helpers
# =============================================================================

# Epic suppresses counts of 10 or fewer as the literal string "10 or fewer";
# some cells arrive blank instead. Both mean "10 or fewer patients".
is_suppressed_count <- function(x) {
  x <- trimws(x)
  is.na(x) | x == "" | x == "-" | x == "10 or fewer"
}

# Imputes a suppressed count as 5 (half the bound), strips thousands separators
unsuppress_count <- function(x) {
  x <- trimws(x)
  suppressWarnings(as.numeric(ifelse(is_suppressed_count(x), "5", gsub(",", "", x))))
}

# Raw ages use exclusive upper bounds ("< 5", "< 18", ...), so subtract 1 from
# the upper bound to get inclusive ranges: <1, 1-4, 5-17, 18-49, 50-64, 65+
standardize_age_labels <- function(age) {
  age <- trimws(age)
  age <- str_replace(age, "^Less than\\s+(\\d+).*$", "<\\1 Years")
  age <- str_replace(age, "^(\\d+)\\s+Years or more$", "\\1+ Years")
  m <- str_match(age, "^[^0-9]*?(\\d+)\\s+and\\s+<\\s*(\\d+)\\s*Years?$")
  lower <- m[, 2]
  upper <- as.character(as.integer(m[, 3]) - 1L)
  age <- ifelse(!is.na(lower), paste0(lower, "-", upper, " Years"), age)
  ifelse(grepl("^Total", age, ignore.case = TRUE), "Total", age)
}

# Quarter labels look like "Jan 1 - Mar 31". Returns the calendar quarter the
# label falls in, the label's own end date, and whether the label covers the
# whole quarter (Epic emits partial buckets at the edges of the date range).
parse_quarter_range <- function(quarter_str, year_str) {
  quarter_str <- trimws(quarter_str)
  year <- suppressWarnings(as.integer(str_extract(trimws(year_str), "\\d{4}")))

  m_start <- str_match(quarter_str, "^([A-Za-z]{3})[a-z]*\\.?\\s+(\\d{1,2})")
  start_month <- match(m_start[, 2], month.abb)
  start_day <- suppressWarnings(as.integer(m_start[, 3]))

  # End of the label, only when the label is actually a range. The separator is
  # an en dash ("Jan 1 - Mar 31"), so count month tokens rather than matching the
  # dash itself - a non-ASCII pattern fails outright under a C locale.
  has_range <- str_count(
    quarter_str, paste0("(?i)\\b(", paste(month.abb, collapse = "|"), ")\\b")
  ) >= 2
  m_end <- str_match(quarter_str, "([A-Za-z]{3})[a-z]*\\.?\\s+(\\d{1,2})\\s*$")
  end_month <- ifelse(has_range, match(m_end[, 2], month.abb), NA_integer_)
  end_day <- ifelse(has_range, suppressWarnings(as.integer(m_end[, 3])), NA_integer_)

  if (any(is.na(year) | is.na(start_month) | is.na(start_day))) {
    bad <- unique(paste(year_str, quarter_str)[is.na(year) | is.na(start_month) | is.na(start_day)])
    stop(
      "Unparseable year/quarter labels in the export: ",
      paste(utils::head(bad, 5), collapse = " | "),
      "\nUpdate parse_quarter_range() for the new label format."
    )
  }

  start_date <- as.Date(sprintf("%d-%02d-%02d", year, start_month, start_day))
  quarter_end <- ceiling_date(start_date, "quarter") - days(1)

  label_end <- as.Date(ifelse(
    is.na(end_month) | is.na(end_day),
    quarter_end,
    as.Date(sprintf("%d-%02d-%02d", year, end_month, end_day))
  ), origin = "1970-01-01")

  list(
    quarter_end = quarter_end,
    label_end = label_end,
    is_full = start_date == floor_date(start_date, "quarter") & label_end >= quarter_end
  )
}

# =============================================================================
# 3. Clean and standardize
# =============================================================================

valid_states <- c(state.name, "District of Columbia")

data_filled <- data_raw %>%
  mutate(
    # Strip non-UTF-8 bytes (common in SlicerDicer exports) before trimws/na_if
    quarter    = iconv(quarter,    to = "UTF-8", sub = ""),
    state_name = iconv(state_name, to = "UTF-8", sub = ""),
    age        = iconv(age,        to = "UTF-8", sub = ""),
    state_name = if_else(trimws(state_name) == "", NA_character_, trimws(state_name)),
    state_name = if_else(grepl("^Total", state_name), "Total", state_name),
    year       = na_if(trimws(year),    ""),
    quarter    = na_if(trimws(quarter), ""),
    age        = na_if(trimws(age),     "")
  ) %>%
  # Merged cells: grouping columns are only filled on the first row of a group
  fill(year, quarter, state_name, age, .direction = "down") %>%
  filter(!(is.na(year) | is.na(quarter) | is.na(state_name) | is.na(age)))

# --- Time: last day of the quarter, YYYY-mm-dd; drop partial quarters --------
quarter_parsed <- parse_quarter_range(data_filled$quarter, data_filled$year)

data_timed <- data_filled %>%
  mutate(
    quarter_end = quarter_parsed$quarter_end,
    is_full_quarter = quarter_parsed$is_full
  )

partial_labels <- data_timed %>%
  filter(!is_full_quarter) %>%
  distinct(year, quarter) %>%
  mutate(label = paste(year, quarter))

if (nrow(partial_labels) > 0) {
  n_partial <- sum(!data_timed$is_full_quarter)
  message(
    "Dropping ", n_partial, " rows in ", nrow(partial_labels),
    " partial quarter(s): ", paste(partial_labels$label, collapse = ", ")
  )
  if (n_partial == nrow(data_timed)) {
    stop(
      "Every quarter was classified as partial - the quarter label format ",
      "probably changed. Check parse_quarter_range()."
    )
  }
}

data_timed <- data_timed %>%
  filter(is_full_quarter) %>%
  mutate(time = format(quarter_end, "%Y-%m-%d"))

# --- Geography: state name -> FIPS, then drop the state name -----------------
n_before_geo <- nrow(data_timed)

data_geo <- data_timed %>%
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

dropped_states <- setdiff(unique(data_timed$state_name), c(valid_states, "Total"))
if (length(dropped_states) > 0) {
  message(
    "Dropped ", n_before_geo - nrow(data_geo), " non-US / catch-all rows: ",
    paste(utils::head(sort(dropped_states), 10), collapse = ", "),
    if (length(dropped_states) > 10) ", ..." else ""
  )
}

# --- Measures + per-measure suppression --------------------------------------
# Flags are computed BEFORE imputation, so they record what Epic withheld.
data_measures <- data_geo %>%
  mutate(
    age = standardize_age_labels(age),

    epic_n_strep_throat_suppressed_flag = as.integer(is_suppressed_count(n_strep_throat)),
    epic_n_patients_suppressed_flag     = as.integer(is_suppressed_count(n_patients)),

    epic_n_strep_throat = unsuppress_count(n_strep_throat),
    epic_n_patients     = unsuppress_count(n_patients),

    # Percent of patients with a strep throat diagnosis (NOT per 100,000).
    # When the denominator itself was suppressed it has been imputed to 5, so
    # 5 / 5 * 100 would assert a meaningless 100% - leave those cells NA, as in
    # cosmos_vaccines, and keep the flag at 1.
    epic_pct_strep_throat_suppressed_flag = as.integer(
      epic_n_strep_throat_suppressed_flag == 1L | epic_n_patients_suppressed_flag == 1L
    ),
    epic_pct_strep_throat = if_else(
      epic_n_patients_suppressed_flag == 1L | epic_n_patients == 0,
      NA_real_,
      epic_n_strep_throat / epic_n_patients * 100
    )
  )

data_clean <- data_measures %>%
  select(
    geography, time, age,
    epic_n_strep_throat, epic_n_strep_throat_suppressed_flag,
    epic_n_patients, epic_n_patients_suppressed_flag,
    epic_pct_strep_throat, epic_pct_strep_throat_suppressed_flag
  ) %>%
  arrange(geography, age, time)

# =============================================================================
# 4. Validate
# =============================================================================

dupes <- data_clean %>%
  count(geography, time, age) %>%
  filter(n > 1)
if (nrow(dupes) > 0) {
  stop(
    "Duplicate rows per geography/time/age (", nrow(dupes), " combinations). ",
    "Check for overlapping staging files in raw/staging/."
  )
}

stopifnot(
  # Geography: FIPS strings, national is "00"
  all(nchar(data_clean$geography) == 2),
  "00" %in% data_clean$geography,
  # Time: YYYY-mm-dd, always the last day of a quarter
  all(grepl("^\\d{4}-\\d{2}-\\d{2}$", data_clean$time)),
  all(as.Date(data_clean$time) == ceiling_date(as.Date(data_clean$time), "quarter") - days(1)),
  # No state name column survives
  !("state_name" %in% names(data_clean)),
  # Counts are present and non-negative
  !any(is.na(data_clean$epic_n_strep_throat)),
  !any(is.na(data_clean$epic_n_patients)),
  all(data_clean$epic_n_strep_throat >= 0),
  all(data_clean$epic_n_patients >= 0),
  # Percent is a percent, in [0, 100]
  all(data_clean$epic_pct_strep_throat >= 0 & data_clean$epic_pct_strep_throat <= 100,
      na.rm = TRUE),
  # Flags are 0/1, and every imputed value carries a flag of 1
  all(data_clean$epic_n_strep_throat_suppressed_flag %in% c(0L, 1L)),
  all(data_clean$epic_n_patients_suppressed_flag %in% c(0L, 1L)),
  all(data_clean$epic_pct_strep_throat_suppressed_flag %in% c(0L, 1L)),
  all(data_clean$epic_n_strep_throat[data_clean$epic_n_strep_throat_suppressed_flag == 1L] == 5),
  all(data_clean$epic_n_patients[data_clean$epic_n_patients_suppressed_flag == 1L] == 5),
  all(is.na(data_clean$epic_pct_strep_throat[data_clean$epic_pct_strep_throat_suppressed_flag == 0L]) == FALSE)
)

message(
  "Standardized ", nrow(data_clean), " rows | ",
  length(unique(data_clean$geography)), " geographies | ",
  min(data_clean$time), " to ", max(data_clean$time)
)
for (m in c("epic_n_strep_throat", "epic_n_patients", "epic_pct_strep_throat")) {
  message(
    "  ", m, ": ", sum(data_clean[[paste0(m, "_suppressed_flag")]]), " suppressed/imputed",
    ", ", sum(is.na(data_clean[[m]])), " left NA"
  )
}

# =============================================================================
# 5. Write standardized output
# =============================================================================

if (!dir.exists("standard")) dir.create("standard")
vroom::vroom_write(data_clean, "standard/data.csv.gz", delim = ",")

# =============================================================================
# 6. Record processed state
# =============================================================================

process$raw_state <- current_state
dcf::dcf_process_record(updated = process)
