# =============================================================================
# Epic Cosmos Group A Streptococcus (GAS) Data Ingestion
# Source: Epic Cosmos SlicerDicer exports
#         Quarterly ED encounters with GAS-related diagnoses by state and age
#
# Relevant ICD-10 codes:
#   J02.0  - Streptococcal pharyngitis
#   J03.00 - Acute streptococcal tonsillitis, unspecified
#   J03.01 - Acute recurrent streptococcal tonsillitis
# =============================================================================

library(dplyr)
library(tidyr)
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
#   1-8:   Metadata header (Session Title, ID, etc.)
#   9-10:  Blank
#   11:    Sub-header / column group labels
#   12:    Column names (Year, Month, State of Residence, n_patients, pct)
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
    if (!is.null(password) && nzchar(password)) {
      decrypted <- tempfile(fileext = ".xlsx")
      cmd <- sprintf(
        'python -m msoffcrypto -p "%s" "%s" "%s"',
        password, normalizePath(file, winslash = "/"), decrypted
      )
      if (system(cmd) != 0) stop("Decryption failed: ", file)
      load_file <- decrypted
      on.exit(unlink(decrypted))
    } else {
      load_file <- file
    }

    wb <- openxlsx2::wb_load(load_file)
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

  # Data starts at row 13 (0-indexed row 12 in R after skip)
  data_raw <- all_rows[14:nrow(all_rows), , drop = FALSE]
  rownames(data_raw) <- NULL

  # Assign column names: Year, Month, State, n_patients, pct
  # Adjust col.names below to match your specific SlicerDicer export layout
  n_cols <- ncol(data_raw)
  base_names <- c("year", "quarter", "state_name",'age', "n_strep_throat", "n_patients")
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
# 2. Clean and standardize
# =============================================================================

valid_states <- c(state.name, "District of Columbia")

data_clean <- data_raw %>%
  mutate(
    # Strip non-UTF-8 bytes (common in SlicerDicer exports)
    quarter    = iconv(quarter,    to = "UTF-8", sub = ""),
    state_name = iconv(state_name, to = "UTF-8", sub = ""),
    state_name = if_else(state_name=='',NA_character_,state_name),
    state_name = if_else(grepl('Total',state_name), 'Total', state_name),
    year       = na_if(trimws(year),    ""),
    quarter    = na_if(trimws(quarter), ""),
    age    = na_if(trimws(age), "")


  ) %>%
  tidyr::fill(year, quarter, age, state_name, .direction = "down") %>%
  #filter(trimws(state_name) %in% c(valid_states, "Total")) %>%
  mutate(
    # Standardize age group labels
    age = trimws(age),
    age = stringr::str_replace(age, "^Less than\\s+(\\d+).*$", "<\\1 Years"),
    age = stringr::str_replace(age, "^(\\d+)\\s+Years or more$", "\\1+ Years"),
    age = {
      m <- stringr::str_match(age, "^[^0-9]*?(\\d+)\\s+and\\s+<\\s*(\\d+)\\s*Years?$")
      lower <- m[, 2]
      upper <- as.character(as.integer(m[, 3]) - 1L)
      ifelse(!is.na(lower), paste0(lower, "-", upper, " Years"), age)
    },
    age = if_else(grepl("^Total", age, ignore.case = TRUE), "Total", age)
  ) %>%
  mutate(
    # Suppression: counts <= 10 reported as "10 or fewer"
    suppressed_flag = if_else(trimws(n_strep_throat) == "10 or fewer", 1L, 0L),
    n_strep_throat  = as.numeric(if_else(
      trimws(n_strep_throat) == "10 or fewer", "5", trimws(n_strep_throat)
    )),
    n_patients = as.numeric(trimws(n_patients)),

    # Rate per 100,000 patients
    rate_strep_throat = n_strep_throat / n_patients * 100000,

    # Time: first day of quarter, parsed from "Jan 1 - Mar 31" format
    quarter_start_month = stringr::str_extract(trimws(quarter), "^[A-Za-z]+"),
    quarter_start_day   = stringr::str_extract(trimws(quarter), "(?<=^[A-Za-z]{3}\\s)\\d+"),
    time = format(
      as.Date(paste(trimws(year), quarter_start_month, quarter_start_day), format = "%Y %b %d"),
      "%m-%d-%Y"
    ),

    # Geography name for FIPS join
    geography_name = if_else(trimws(state_name) == "Total", "United States", trimws(state_name))
  ) %>%
  select(-quarter_start_month, -quarter_start_day) %>%
  left_join(state_fips_lookup, by = "geography_name") %>%
  filter(!is.na(geography)) %>%
  select(geography,age, time, state_name, n_strep_throat, n_patients, rate_strep_throat, suppressed_flag) %>%
  arrange(geography,age,  time)

# =============================================================================
# 3. Write standardized output
# =============================================================================

vroom::vroom_write(data_clean, "standard/data.csv.gz", delim = ",")

# =============================================================================
# 4. Record processed state
# =============================================================================

process$raw_state <- current_state
dcf::dcf_process_record(updated = process)
