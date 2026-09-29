# =============================================================================
# Epic Cosmos Diabetes Indicators by County Ingestion
# Source: Epic Cosmos SlicerDicer exports (raw/staging/*.xlsx)
#         Diabetes indicators for one month (Jan 2024), by county of residence
#         crossed with two further stratifiers, exported as two sessions.
#
# SlicerDicer sessions "DM in ICD10 by State Age 2023":
#   2860367 : County of Residence x Patient RUCA (Primary) x Age at Encounter
#   2860371 : County of Residence x Age at Encounter x Patient Race
#   Data model      : Patients
#   Population base : All Patients
#   Criteria        : State of Residence = Illinois, Florida, Ohio, Wyoming;
#                     Country of Care = United States of America;
#                     Has Any Encounters; Base Patient
#   Measures        : A1c 6.5+ (%), Diabetes CCW definition (%), Number of Patients
#
# Output (PopHIVE wide format), one file per export layout:
#   standard/data_ruca_age.csv.gz : geography, time, ruca, age, measures
#   standard/data_age_race.csv.gz : geography, time, age, race_ethnicity, measures
#   measures: epic_pct_a1c_6_5_plus, epic_pct_diabetes_ccw, epic_n_patients,
#             each followed by its own _suppressed_flag
#
# Conventions applied here:
#   - every "Total: ..." subtotal label is normalized to "Total", so each output
#     carries all marginals (a Total level of each stratifier)
#   - the county "Total" row covers ONLY the four exported states, not the
#     nation, so it is dropped (it must not be labelled "00"); the county
#     "None of the above" row (unknown county) is dropped as well
#   - suppression is handled per measure (see below), flags computed first
# =============================================================================

library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)
library(openxlsx2)

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

xlsx_password <- Sys.getenv("EPIC_XLSX_PASSWORD")

# --- Label maps: the contract with the export -------------------------------
# Stratification header label -> column name. Unrecognized labels stop the run.
DIM_LABELS <- c(
  "County of Residence"                          = "county",
  "Patient Rural-Urban Commuting Area (Primary)" = "ruca",
  "Age at Encounter in Years"                    = "age",
  "Patient Race"                                 = "race_ethnicity"
)
# Measure label regex -> output name. Each label must match exactly one pattern.
MEASURE_PATTERNS <- c(
  epic_pct_a1c_6_5_plus = "^A1c 6\\.5\\+",
  epic_pct_diabetes_ccw = "^Diabetes CCW",
  epic_n_patients       = "^Number of Patients"
)
PCT_COLS <- c("epic_pct_a1c_6_5_plus", "epic_pct_diabetes_ccw")
# Recognized dimension sets (sorted) -> output file
LAYOUTS <- list(
  "age|county|ruca"           = "data_ruca_age",
  "age|county|race_ethnicity" = "data_age_race"
)

# =============================================================================
# 1. Locate staging files and detect change
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

# =============================================================================
# 2. Helpers
# =============================================================================
read_slicerdicer_grid <- function(file, password) {
  if (!nzchar(password)) {
    stop(
      "EPIC_XLSX_PASSWORD is not set, but ", basename(file), " is a ",
      "password-protected SlicerDicer export.\n",
      "Set it in .Renviron via usethis::edit_r_environ()."
    )
  }
  decrypted <- tempfile(fileext = ".xlsx")
  on.exit(unlink(decrypted))
  rc <- system2("python", c(
    "-m", "msoffcrypto", "-p", shQuote(password),
    shQuote(normalizePath(file, winslash = "/")), shQuote(decrypted)
  ))
  if (rc != 0) stop("Decryption failed: ", file)
  grid <- suppressWarnings(openxlsx2::wb_to_df(
    openxlsx2::wb_load(decrypted), sheet = 1, col_names = FALSE,
    skip_empty_rows = FALSE, skip_empty_cols = FALSE
  ))
  grid <- as.data.frame(
    lapply(grid, function(x) { x <- as.character(x); x[is.na(x)] <- ""; iconv(x, to = "UTF-8", sub = "") }),
    stringsAsFactors = FALSE
  )
  grid[] <- lapply(grid, trimws)
  grid
}

# Epic suppresses counts of 10 or fewer as "10 or fewer"; some cells arrive blank
is_suppressed_count <- function(x) x == "" | x == "-" | x == "10 or fewer"

# Percent cells: blank / "-" -> NA; bounded "<0.01%" -> half the bound
parse_pct <- function(x) {
  bounded <- grepl("^<", x)
  v <- suppressWarnings(as.numeric(gsub("[%<, ]", "", x)))
  v[bounded] <- v[bounded] / 2
  v
}

# Raw ages use exclusive upper bounds ("< 25"): subtract 1 for inclusive ranges
standardize_age_labels <- function(age) {
  age <- str_replace(age, "^Less than\\s+(\\d+).*$", "<\\1 Years")
  age <- str_replace(age, "^(\\d+)\\s+Years or more$", "\\1+ Years")
  m <- str_match(age, "^[^0-9]*?(\\d+)\\s+and\\s+<\\s*(\\d+)\\s*Years?$")
  age <- ifelse(!is.na(m[, 2]), paste0(m[, 2], "-", as.integer(m[, 3]) - 1L, " Years"), age)
  age
}

# Session date range "1/1/2024 - 1/31/2024" -> last day of the range
parse_range_end <- function(grid) {
  hit <- which(grid[[1]] == "Session Date Range")
  rng <- grid[hit[1], 2]
  end <- as.Date(str_match(rng, "-\\s*(\\d{1,2}/\\d{1,2}/\\d{4})\\s*$")[, 2], "%m/%d/%Y")
  if (is.na(end)) stop("Cannot parse Session Date Range: '", rng, "'")
  end
}

# Match key for county names: upper case, no County/Parish/... suffix, no punctuation
county_key <- function(x) {
  x <- toupper(x)
  x <- gsub(" (COUNTY|PARISH|BOROUGH|CENSUS AREA|MUNICIPALITY|CITY AND BOROUGH)$", "", x)
  x <- gsub("^SAINT ", "ST ", x)
  gsub("[^A-Z0-9]", "", x)
}

# =============================================================================
# 3. FIPS lookup (county name + state abbreviation)
# =============================================================================
all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
county_lookup <- all_fips %>%
  filter(nchar(geography) == 5) %>%
  transmute(geography, state, key = county_key(geography_name)) %>%
  distinct()
stopifnot(!anyDuplicated(county_lookup[c("state", "key")]))

# =============================================================================
# 4. Read one export into a tidy long-by-index frame
# =============================================================================
read_export <- function(file) {
  message("Reading: ", basename(file))
  grid <- read_slicerdicer_grid(file, xlsx_password)
  for (lbl in c("Session ID", "Population Base", "Session Date Range")) {
    hit <- which(grid[[1]] == lbl)
    if (length(hit) > 0) message("  ", lbl, ": ", grid[hit[1], 2])
  }
  time <- format(parse_range_end(grid), "%Y-%m-%d")

  # Locate the header rows by content: measure labels sit in the row holding
  # "Measures", stratification labels in the row directly below.
  m_row <- which(apply(grid, 1, function(r) "Measures" %in% r))[1]
  if (is.na(m_row)) stop("No 'Measures' row found in ", basename(file))
  h_row <- m_row + 1L
  m_col <- which(unlist(grid[m_row, ]) == "Measures")[1]

  dim_idx <- which(unlist(grid[h_row, ]) != "")
  dim_idx <- dim_idx[dim_idx < m_col + 1L]
  dim_lbls <- unlist(grid[h_row, dim_idx])
  unknown <- setdiff(dim_lbls, names(DIM_LABELS))
  if (length(unknown) > 0) {
    stop("Unrecognized stratification label(s) in ", basename(file), ": ",
         paste(unknown, collapse = ", "), "\nExtend DIM_LABELS.")
  }
  dims <- unname(DIM_LABELS[dim_lbls])

  meas_idx <- (m_col + 1L):ncol(grid)
  meas_idx <- meas_idx[unlist(grid[m_row, meas_idx]) != ""]
  meas_lbls <- unlist(grid[m_row, meas_idx])
  meas_names <- vapply(meas_lbls, function(l) {
    hit <- names(MEASURE_PATTERNS)[vapply(MEASURE_PATTERNS, grepl, logical(1), x = l)]
    if (length(hit) != 1) {
      stop("Measure label '", l, "' in ", basename(file), " matched ", length(hit),
           " patterns. Extend MEASURE_PATTERNS.")
    }
    hit
  }, character(1))
  if (anyDuplicated(meas_names)) stop("Duplicate measure mapping in ", basename(file))

  body <- grid[(h_row + 1L):nrow(grid), c(dim_idx, meas_idx), drop = FALSE]
  names(body) <- c(dims, meas_names)
  rownames(body) <- NULL

  # Merged cells: only the outer stratifiers are blank; the last one never is
  outer <- dims[-length(dims)]
  last_dim <- dims[length(dims)]
  body[outer] <- lapply(body[outer], function(x) ifelse(x == "", NA_character_, x))
  body <- fill(body, all_of(outer), .direction = "down")
  body <- body[!is.na(body[[outer[1]]]) & body[[last_dim]] != "", ]

  # Subtotal labels ("Total: Total includes all data under the X bucket...")
  body[dims] <- lapply(body[dims], function(x) ifelse(grepl("^Total", x), "Total", x))

  list(data = body, dims = dims, time = time, file = basename(file))
}

# =============================================================================
# 5. Standardize one export
# =============================================================================
standardize <- function(ex) {
  d <- ex$data
  n_raw <- nrow(d)

  # --- Geography: "VAN WERT, OH" -> county FIPS -------------------------------
  dropped_total <- sum(d$county == "Total")
  dropped_unknown <- sum(d$county == "None of the above")
  d <- d %>% filter(!county %in% c("Total", "None of the above"))
  parts <- str_match(d$county, "^(.*),\\s*([A-Z]{2})$")
  d$key <- county_key(parts[, 2])
  d$state <- parts[, 3]
  d <- d %>% left_join(county_lookup, by = c("state", "key"))
  if (any(is.na(d$geography))) {
    bad <- unique(d$county[is.na(d$geography)])
    stop("Unmatched counties in ", ex$file, ": ", paste(utils::head(bad, 10), collapse = " | "),
         "\nExtend county_key().")
  }
  message("  Dropped ", dropped_total, " county 'Total' rows (four-state total, not national) and ",
          dropped_unknown, " 'None of the above' (unknown county) rows")

  # --- Label cleanup ----------------------------------------------------------
  d <- d %>% mutate(age = standardize_age_labels(age))
  if ("ruca" %in% names(d)) d <- d %>% mutate(ruca = if_else(ruca == "None of the above", "Unknown", ruca))
  if ("race_ethnicity" %in% names(d)) {
    d <- d %>% mutate(race_ethnicity = if_else(race_ethnicity == "None of the above", "Unknown", race_ethnicity))
  }

  # --- Measures + per-measure flags (computed BEFORE imputation) --------------
  d <- d %>%
    mutate(
      time = ex$time,
      epic_n_patients_suppressed_flag = as.integer(is_suppressed_count(epic_n_patients)),
      epic_n_patients = if_else(
        epic_n_patients_suppressed_flag == 1L, 5,
        suppressWarnings(as.numeric(gsub(",", "", epic_n_patients)))
      )
    )
  for (pc in PCT_COLS) {
    v <- parse_pct(d[[pc]])
    fl <- as.integer(is.na(v))
    # Rule 2: missing percent = 5 / denominator. If the denominator was itself
    # suppressed (imputed to 5), 5/5 would assert a meaningless 100%: leave NA.
    imputed <- if_else(d$epic_n_patients_suppressed_flag == 1L, NA_real_, 5 / d$epic_n_patients * 100)
    d[[pc]] <- if_else(fl == 1L, imputed, v)
    d[[paste0(pc, "_suppressed_flag")]] <- fl
  }

  index <- c("geography", "time", intersect(c("ruca", "age", "race_ethnicity"), names(d)))
  d <- d %>%
    select(all_of(index),
           epic_pct_a1c_6_5_plus, epic_pct_a1c_6_5_plus_suppressed_flag,
           epic_pct_diabetes_ccw, epic_pct_diabetes_ccw_suppressed_flag,
           epic_n_patients, epic_n_patients_suppressed_flag) %>%
    arrange(across(all_of(index)))
  attr(d, "index") <- index
  d
}

# =============================================================================
# 6. Process, validate, and write
# =============================================================================
if (!identical(process$raw_state, current_state)) {
  outputs <- list()
  for (f in staging_files) {
    ex <- read_export(f)
    key <- paste(sort(ex$dims), collapse = "|")
    out_name <- LAYOUTS[[key]]
    if (is.null(out_name)) {
      stop("Unrecognized stratification layout '", key, "' in ", ex$file, ". Extend LAYOUTS.")
    }
    if (!is.null(outputs[[out_name]])) stop("Two staging files map to ", out_name, ".")
    outputs[[out_name]] <- standardize(ex)
  }

  for (nm in names(outputs)) {
    d <- outputs[[nm]]
    index <- attr(d, "index")

    dupes <- d %>% count(across(all_of(index))) %>% filter(n > 1)
    if (nrow(dupes) > 0) stop("Duplicate index rows in ", nm, " (", nrow(dupes), ").")

    stopifnot(
      all(nchar(d$geography) == 5),
      all(grepl("^\\d{4}-\\d{2}-\\d{2}$", d$time)),
      !any(is.na(d$epic_n_patients)), all(d$epic_n_patients >= 0),
      all(d$epic_pct_a1c_6_5_plus %in% NA | (d$epic_pct_a1c_6_5_plus >= 0 & d$epic_pct_a1c_6_5_plus <= 100)),
      all(d$epic_pct_diabetes_ccw %in% NA | (d$epic_pct_diabetes_ccw >= 0 & d$epic_pct_diabetes_ccw <= 100)),
      all(d$epic_n_patients[d$epic_n_patients_suppressed_flag == 1L] == 5),
      # A percent is missing only where it was suppressed AND its denominator was too
      identical(is.na(d$epic_pct_a1c_6_5_plus),
                d$epic_pct_a1c_6_5_plus_suppressed_flag == 1L & d$epic_n_patients_suppressed_flag == 1L),
      identical(is.na(d$epic_pct_diabetes_ccw),
                d$epic_pct_diabetes_ccw_suppressed_flag == 1L & d$epic_n_patients_suppressed_flag == 1L)
    )

    message(
      nm, ": ", nrow(d), " rows | ", length(unique(d$geography)), " counties | time ",
      paste(unique(d$time), collapse = ", ")
    )
    for (fl in grep("_suppressed_flag$", names(d), value = TRUE)) {
      message("  ", fl, ": ", sum(d[[fl]]), " suppressed/imputed")
    }
    message("  percent cells left NA (denominator also suppressed): ",
            sum(is.na(d$epic_pct_a1c_6_5_plus)), " (a1c), ",
            sum(is.na(d$epic_pct_diabetes_ccw)), " (ccw)")

    if (!dir.exists("standard")) dir.create("standard")
    vroom::vroom_write(d, file.path("standard", paste0(nm, ".csv.gz")), delim = ",")
  }

  process$raw_state <- current_state
  dcf::dcf_process_record(updated = process)
}
