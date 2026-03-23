# =============================================================================
# Epic Cosmos Immunization Data Ingestion
# Source: Epic SlicerDicer export (Patients Crosstab)
#         "rsv and Percentage with Has Any Encounters?
#          preceded by Immunizations"
# Geography:
#   raw/staging/       -> standard/data_zip.csv.gz     (ZIP code)
#   raw/staging_county -> standard/data_county.csv.gz  (county FIPS)
# Dimensions: Age at Encounter x geography
# Columns (rows 11-12 of export):
#   col 1: Age at Encounter in Years
#   col 2: Postal Code / County of Residence
#   col 3: rsv (%)
#   col 4: Percentage ... PCV within 4 years (%)
#   col 5: Percentage ... Zoster within 4 years (%)
#   col 6: Number of Patients
#   col 7: influenza (%)
# =============================================================================

library(dplyr)
library(tidyr)
library(openxlsx2)
library(vroom)

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
  process <- list(raw_state = NULL, raw_state_county = NULL)
} else {
  process <- dcf::dcf_process_record()
}

if (!dir.exists("standard")) dir.create("standard")

# Migrate old data.csv.gz -> data_zip.csv.gz if not yet renamed
if (
  file.exists("standard/data.csv.gz") &&
  !file.exists("standard/data_zip.csv.gz")
) {
  file.rename("standard/data.csv.gz", "standard/data_zip.csv.gz")
}

xlsx_password <- Sys.getenv("EPIC_XLSX_PASSWORD")
if (!nzchar(xlsx_password)) {
  stop(
    "EPIC_XLSX_PASSWORD not set. ",
    "Add it to ~/.Renviron via usethis::edit_r_environ()"
  )
}

# ---------------------------------------------------------------------------
# Helper: decrypt one xlsx and return a data.frame with named columns
# ---------------------------------------------------------------------------
parse_one_file <- function(raw_file, password, col_names) {
  # Copy to temp first (avoids permission errors with OneDrive-synced files)
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
    lapply(all_rows, function(x) {
      x <- as.character(x)
      x[is.na(x)] <- ""
      x
    }),
    stringsAsFactors = FALSE
  )

  # Row 6: "Session Date Range" -> use end date as time
  session_end_raw <- trimws(
    gsub(".*-\\s*", "", trimws(as.character(all_rows[6, 2])))
  )
  time_val <- format(as.Date(session_end_raw, "%m/%d/%Y"), "%m-%d-%Y")

  data_raw <- all_rows[13:nrow(all_rows), seq_along(col_names), drop = FALSE]
  rownames(data_raw) <- NULL
  colnames(data_raw) <- col_names

  data_raw$age[data_raw$age == ""] <- NA
  data_raw <- tidyr::fill(data_raw, age, .direction = "down")
  data_raw$time_val <- time_val
  data_raw
}

# ---------------------------------------------------------------------------
# Helper: parse value columns (suppression, strip %, NA handling)
# ---------------------------------------------------------------------------
parse_pct <- function(x) {
  x <- trimws(gsub("%", "", x))
  suppressed <- x == "10 or fewer"
  value <- suppressWarnings(
    as.numeric(ifelse(suppressed, "5", ifelse(x %in% c("-", ""), NA, x)))
  )
  list(value = value, suppressed = as.integer(suppressed))
}

parse_count <- function(x) {
  x <- trimws(x)
  suppressed <- x == "10 or fewer"
  value <- suppressWarnings(
    as.numeric(ifelse(suppressed, "5", ifelse(x %in% c("-", ""), NA, x)))
  )
  list(value = value, suppressed = as.integer(suppressed))
}

# ---------------------------------------------------------------------------
# Helper: assemble the standard value columns from a parsed data.frame
# ---------------------------------------------------------------------------
assemble_standard <- function(df, geography_col, time_val) {
  rsv    <- parse_pct(df$pct_rsv)
  pcv    <- parse_pct(df$pct_pcv)
  zoster <- parse_pct(df$pct_zoster)
  flu    <- parse_pct(df$pct_flu)
  npt    <- parse_count(df$n_patients)

  df |>
    mutate(
      geography                  = .data[[geography_col]],
      time                       = time_val,
      age                        = trimws(age),
      pct_rsv                    = rsv$value,
      suppressed_flag_pct_rsv    = rsv$suppressed,
      pct_pcv                    = pcv$value,
      suppressed_flag_pct_pcv    = pcv$suppressed,
      pct_zoster                 = zoster$value,
      suppressed_flag_pct_zoster = zoster$suppressed,
      pct_flu                    = flu$value,
      suppressed_flag_pct_flu    = flu$suppressed,
      n_patients                 = npt$value,
      suppressed_flag_n_patients = npt$suppressed
    ) |>
    select(
      geography, time, age,
      pct_rsv,    suppressed_flag_pct_rsv,
      pct_pcv,    suppressed_flag_pct_pcv,
      pct_zoster, suppressed_flag_pct_zoster,
      pct_flu,    suppressed_flag_pct_flu,
      n_patients, suppressed_flag_n_patients
    ) |>
    arrange(geography, age)
}

# =============================================================================
# BLOCK 1: ZIP-level data  (raw/staging/ -> standard/data_zip.csv.gz)
# =============================================================================

zip_col_names <- c(
  "age", "zip_code", "pct_rsv", "pct_pcv",
  "pct_zoster", "n_patients", "pct_flu"
)

raw_files_zip <- list.files(
  "raw/staging", pattern = "\\.xlsx$", full.names = TRUE
)
if (length(raw_files_zip) == 0) {
  stop("No xlsx files found in raw/staging/")
}

zip_hashes <- as.list(tools::md5sum(raw_files_zip))
names(zip_hashes) <- basename(raw_files_zip)

if (!identical(process$raw_state, zip_hashes)) {

  data_raw_zip <- bind_rows(
    lapply(raw_files_zip, parse_one_file,
      password  = xlsx_password,
      col_names = zip_col_names
    )
  )

  time_val_zip  <- max(data_raw_zip$time_val)
  data_raw_zip  <- data_raw_zip |> select(-time_val)

  # Keep only valid 5-digit US ZIP codes
  data_raw_zip <- data_raw_zip |>
    filter(grepl("^\\d{5}$", trimws(zip_code))) |>
    distinct(zip_code, age, .keep_all = TRUE)

  data_zip <- assemble_standard(data_raw_zip, "zip_code", time_val_zip)

  vroom::vroom_write(data_zip, "standard/data_zip.csv.gz", delim = ",")

  process$raw_state <- zip_hashes
  dcf::dcf_process_record(updated = process)
}

# =============================================================================
# BLOCK 2: County-level data  (raw/staging_county/ -> standard/data_county.csv.gz)
# =============================================================================

county_col_names <- c(
  "age", "county_state", "pct_rsv", "pct_pcv",
  "pct_zoster", "n_patients", "pct_flu"
)

raw_files_county <- list.files(
  "raw/staging_county", pattern = "\\.xlsx$", full.names = TRUE
)

if (length(raw_files_county) > 0) {

  county_hashes <- as.list(tools::md5sum(raw_files_county))
  names(county_hashes) <- basename(raw_files_county)

  if (!identical(process$raw_state_county, county_hashes)) {

    # -------------------------------------------------------------------------
    # Load FIPS lookup (county level)
    # -------------------------------------------------------------------------
    all_fips <- vroom::vroom(
      "../../resources/all_fips.csv.gz",
      show_col_types = FALSE
    )

    # Normalize county names to uppercase with suffix stripped for joining.
    # Apply fixes for apostrophes/prefixes that differ between Epic and FIPS.
    normalize_county <- function(x) {
      x <- toupper(trimws(x))
      x <- sub(
        " (COUNTY|PARISH|BOROUGH|CENSUS AREA|MUNICIPALITY|CITY AND BOROUGH)$",
        "", x
      )
      x <- gsub("^SAINTE ", "STE. ", x)
      x <- gsub("^SAINT ",  "ST. ",  x)
      # Add period after ST if missing (e.g. "ST JOHN" -> "ST. JOHN")
      x <- gsub("^ST ([^.].*)$", "ST. \\1", x)
      x <- gsub("PRINCE GEORGES",  "PRINCE GEORGE'S", x)
      x <- gsub("QUEEN ANNES",     "QUEEN ANNE'S",    x)
      x <- gsub("ST\\. MARYS",     "ST. MARY'S",      x)
      x <- gsub("^OBRIEN$",        "O'BRIEN",         x)
      x <- gsub("^LA SALLE$",      "LASALLE",         x)
      x <- gsub("^LA PORTE$",      "LAPORTE",         x)
      x
    }

    all_fips_county <- all_fips |>
      filter(nchar(geography) == 5) |>
      select(geography, geography_name, state) |>
      mutate(county_key = normalize_county(geography_name))

    # -------------------------------------------------------------------------
    # Parse county xlsx files
    # -------------------------------------------------------------------------
    data_raw_county <- bind_rows(
      lapply(raw_files_county, parse_one_file,
        password  = xlsx_password,
        col_names = county_col_names
      )
    )

    time_val_county  <- max(data_raw_county$time_val)
    data_raw_county  <- data_raw_county |> select(-time_val)

    # Filter out non-county rows ("Total", "None of the above", blanks)
    # county_state format: "COUNTY NAME, ST"
    data_raw_county <- data_raw_county |>
      filter(grepl(",", trimws(county_state))) |>
      distinct(county_state, age, .keep_all = TRUE)

    # Split "MOFFAT, CO" -> county_key = "MOFFAT", state_abbr = "CO"
    data_raw_county <- data_raw_county |>
      mutate(
        county_raw  = trimws(sub(",.*$", "", county_state)),
        state_abbr  = trimws(sub("^.*,", "", county_state)),
        county_key  = normalize_county(county_raw)
      )

    # Join to FIPS
    data_raw_county <- data_raw_county |>
      left_join(
        all_fips_county,
        by = c("county_key" = "county_key", "state_abbr" = "state")
      )

    n_unmatched <- sum(is.na(data_raw_county$geography))
    if (n_unmatched > 0) {
      unmatched_names <- unique(
        data_raw_county$county_state[is.na(data_raw_county$geography)]
      )
      warning(
        n_unmatched, " rows could not be matched to a FIPS code.\n",
        "Unmatched county names: ",
        paste(head(unmatched_names, 20), collapse = ", ")
      )
    }

    # Drop rows with no FIPS match
    data_raw_county <- data_raw_county |>
      filter(!is.na(geography))

    data_county <- assemble_standard(
      data_raw_county, "geography", time_val_county
    )

    vroom::vroom_write(
      data_county, "standard/data_county.csv.gz", delim = ","
    )

    process$raw_state_county <- county_hashes
    dcf::dcf_process_record(updated = process)
  }
}
