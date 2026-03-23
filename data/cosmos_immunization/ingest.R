# =============================================================================
# Epic Cosmos Immunization Data Ingestion
# Source: Epic SlicerDicer export (Patients Crosstab)
#         "rsv and Percentage with Has Any Encounters? preceded by Immunizations"
# Geography: ZIP code (Postal Code of Residence)
# Dimensions: Age at Encounter x ZIP code
# Columns (rows 11-12 of export):
#   col 1: Age at Encounter in Years
#   col 2: Postal Code of Residence
#   col 3: rsv (%)
#   col 4: Percentage ... PCV within 4 years (%)
#   col 5: Percentage ... Zoster within 4 years (%)
#   col 6: Number of Patients
#   col 7: influenza (%)
# =============================================================================

library(dplyr)
library(tidyr)
library(openxlsx2)

# Check that msoffcrypto-tool is available
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

# All xlsx files in raw/staging/ (one per state export)
raw_files <- list.files("raw/staging", pattern = "\\.xlsx$", full.names = TRUE)
if (length(raw_files) == 0) stop("No xlsx files found in raw/staging/")

# Hash all files together for change detection
all_hashes <- as.list(tools::md5sum(raw_files))
names(all_hashes) <- basename(raw_files)
current_hash <- all_hashes

if (!identical(process$raw_state, current_hash)) {

  # ---------------------------------------------------------------------------
  # 1. Helper: decrypt + parse one xlsx file
  # ---------------------------------------------------------------------------
  xlsx_password <- Sys.getenv("EPIC_XLSX_PASSWORD")
  if (!nzchar(xlsx_password)) {
    stop("EPIC_XLSX_PASSWORD not set. Add it to ~/.Renviron via usethis::edit_r_environ()")
  }

  col_names <- c("age", "zip_code", "pct_rsv", "pct_pcv", "pct_zoster",
                 "n_patients", "pct_flu")

  parse_one_file <- function(raw_file, password) {
    # Copy to temp first (avoids permission errors when file is open in Excel/OneDrive)
    temp_input     <- tempfile(fileext = ".xlsx")
    decrypted_file <- tempfile(fileext = ".xlsx")
    file.copy(raw_file, temp_input, overwrite = TRUE)

    cmd <- sprintf(
      'python -m msoffcrypto -p "%s" "%s" "%s"',
      password,
      normalizePath(temp_input, winslash = "/"),
      decrypted_file
    )
    status <- system(cmd)
    unlink(temp_input)
    if (status != 0) stop("Failed to decrypt: ", raw_file)

    wb       <- openxlsx2::wb_load(decrypted_file)
    all_rows <- openxlsx2::wb_to_df(
      wb, sheet = 1, col_names = FALSE,
      skip_empty_rows = FALSE, skip_empty_cols = FALSE
    )
    unlink(decrypted_file)

    all_rows <- as.data.frame(
      lapply(all_rows, function(x) { x <- as.character(x); x[is.na(x)] <- ""; x }),
      stringsAsFactors = FALSE
    )

    # Row 6: "Session Date Range" -> use end date as time
    session_end_raw <- trimws(gsub(".*-\\s*", "", trimws(as.character(all_rows[6, 2]))))
    time_val <- format(as.Date(session_end_raw, "%m/%d/%Y"), "%m-%d-%Y")

    data_raw <- all_rows[13:nrow(all_rows), 1:length(col_names), drop = FALSE]
    rownames(data_raw) <- NULL
    colnames(data_raw) <- col_names

    data_raw$age[data_raw$age == ""] <- NA
    data_raw <- tidyr::fill(data_raw, age, .direction = "down")
    data_raw$time_val <- time_val
    data_raw
  }

  # Process all files and combine
  data_raw <- bind_rows(lapply(raw_files, parse_one_file, password = xlsx_password))

  # Use the latest session end date across all files
  time_val <- max(data_raw$time_val)
  data_raw  <- data_raw %>% select(-time_val)

  # ---------------------------------------------------------------------------
  # 2. Clean and filter
  # ---------------------------------------------------------------------------
  # Keep only valid 5-digit US ZIP codes (excludes "Total", blanks, etc.)
  data_raw <- data_raw %>%
    filter(grepl("^\\d{5}$", trimws(zip_code))) %>%
    distinct(zip_code, age, .keep_all = TRUE)  # drop any duplicate zip/age across files

  # ---------------------------------------------------------------------------
  # 3. Parse values: "10 or fewer" -> 5 (suppressed), "-" -> NA, strip "%"
  # ---------------------------------------------------------------------------
  parse_pct <- function(x) {
    x <- trimws(gsub("%", "", x))
    suppressed <- x == "10 or fewer"
    value <- suppressWarnings(as.numeric(ifelse(suppressed, "5", ifelse(x %in% c("-", ""), NA, x))))
    list(value = value, suppressed = as.integer(suppressed))
  }

  parse_count <- function(x) {
    x <- trimws(x)
    suppressed <- x == "10 or fewer"
    value <- suppressWarnings(as.numeric(ifelse(suppressed, "5", ifelse(x %in% c("-", ""), NA, x))))
    list(value = value, suppressed = as.integer(suppressed))
  }

  rsv     <- parse_pct(data_raw$pct_rsv)
  pcv     <- parse_pct(data_raw$pct_pcv)
  zoster  <- parse_pct(data_raw$pct_zoster)
  flu     <- parse_pct(data_raw$pct_flu)
  npt     <- parse_count(data_raw$n_patients)

  # ---------------------------------------------------------------------------
  # 4. Assemble standardized output
  # ---------------------------------------------------------------------------
  data_standard <- data_raw %>%
    mutate(
      geography               = zip_code,
      time                    = time_val,
      age                     = trimws(age),
      pct_rsv                 = rsv$value,
      suppressed_flag_pct_rsv = rsv$suppressed,
      pct_pcv                 = pcv$value,
      suppressed_flag_pct_pcv = pcv$suppressed,
      pct_zoster              = zoster$value,
      suppressed_flag_pct_zoster = zoster$suppressed,
      pct_flu                 = flu$value,
      suppressed_flag_pct_flu = flu$suppressed,
      n_patients              = npt$value,
      suppressed_flag_n_patients = npt$suppressed
    ) %>%
    select(
      geography, time, age,
      pct_rsv,    suppressed_flag_pct_rsv,
      pct_pcv,    suppressed_flag_pct_pcv,
      pct_zoster, suppressed_flag_pct_zoster,
      pct_flu,    suppressed_flag_pct_flu,
      n_patients, suppressed_flag_n_patients
    ) %>%
    arrange(geography, age)

  # ---------------------------------------------------------------------------
  # 5. Write standardized output
  # ---------------------------------------------------------------------------
  if (!dir.exists("standard")) dir.create("standard")

  vroom::vroom_write(data_standard, "standard/data.csv.gz", delim = ",")

  # ---------------------------------------------------------------------------
  # 6. Update process state
  # ---------------------------------------------------------------------------
  process$raw_state <- current_hash
  dcf::dcf_process_record(updated = process)
}
