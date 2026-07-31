# =============================================================================
# Epic Cosmos Infant Vaccination Data Ingestion
# Source: Epic Cosmos SlicerDicer exports (raw/staging/*.xlsx)
#         Monthly RSV immunization (nirsevimab), hepatitis B, and PCV coverage
#         in the newborn birth cohort, by state of residence.
#
# Population base: patients with a newborn / birth-hospitalization billed
# procedure (CPT 99460-99465, 99468, 99477), i.e. the birth cohort.
#
# The staging layout is read from the export's own header rows rather than
# hard-coded, because SlicerDicer sessions vary in both their stratification
# (an earlier session included an age-at-encounter split) and their measure
# labels. Unrecognized dimensions or measures raise an error rather than being
# silently mapped onto the wrong column.
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

# Initialize process record
if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

# Password for the encrypted xlsx exports (set in .Renviron via usethis::edit_r_environ())
xlsx_password <- Sys.getenv("EPIC_XLSX_PASSWORD")

# Recognized stratification columns, keyed by their SlicerDicer header label
DIM_LABELS <- c(
  "Year"                        = "year",
  "Month"                       = "month",
  "State of Residence"          = "state_name",
  "Age at Encounter in Years"   = "age"
)

# Recognized measures, keyed by output column name. Values are regexes matched
# against the measure label in the export. Labels drift between sessions (e.g.
# "RSV immunization after birth (%)" was relabeled "Immunization after birth
# (%)"), so match on the stable part of each label.
MEASURE_PATTERNS <- c(
  epic_n_patients           = "^Number of Patients",
  epic_pct_rsv_immunization = "[Ii]mmunization after birth",
  epic_pct_hepb_vaccination = "[Hh]ep\\s*B",
  epic_pct_pcv_182d         = "PCV"
)

# =============================================================================
# 1. Locate raw SlicerDicer staging files
# =============================================================================
# Place exported .xlsx files from Epic Cosmos SlicerDicer into raw/staging/.
# Expected export layout (rows):
#   1-8:   Metadata header (Session Title, ID, Population Criteria, etc.)
#   9-10:  Blank
#   11:    "Measures" label, then one label per measure column
#   12:    Stratification column names (Year, Month, State of Residence, ...)
#   13+:   Data rows

staging_files <- list.files("raw/staging", pattern = "\\.(csv|xlsx)$", full.names = TRUE)

if (length(staging_files) == 0) {
  stop(
    "No staging files found in raw/staging/.\n",
    "Export data from Epic Cosmos SlicerDicer and place .xlsx files there."
  )
}

# Track raw file state for change detection
current_state <- list(
  files  = staging_files,
  hashes = unname(tools::md5sum(staging_files))
)

if (!identical(process$raw_state, current_state)) {

  # ===========================================================================
  # 2. Read raw data
  # ===========================================================================

  # Decrypt a password-protected SlicerDicer xlsx (or read a plain csv) and
  # return the sheet as an all-character grid with no header interpretation.
  read_slicerdicer_grid <- function(file, password = NULL) {
    message("Reading: ", basename(file))

    if (grepl("\\.xlsx$", file, ignore.case = TRUE)) {
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
      grid <- openxlsx2::wb_to_df(
        wb, sheet = 1, col_names = FALSE,
        skip_empty_rows = FALSE, skip_empty_cols = FALSE
      )
    } else {
      grid <- read.csv(
        file, header = FALSE, stringsAsFactors = FALSE, check.names = FALSE,
        fileEncoding = "windows-1252"
      )
    }

    as.data.frame(
      lapply(grid, function(x) { x <- as.character(x); x[is.na(x)] <- ""; x }),
      stringsAsFactors = FALSE
    )
  }

  # Resolve the column layout from the export's header rows, then return the
  # data rows named with standard output column names.
  extract_staging_data <- function(grid, file) {
    nm <- basename(file)

    # The stratification header row is the one starting with "Year"
    header_row <- which(trimws(grid[[1]]) == "Year")[1]
    if (is.na(header_row) || header_row < 2) {
      stop("Could not locate the 'Year' header row in: ", nm)
    }

    # Stratification columns occupy the leftmost cells of the header row
    dim_row  <- trimws(as.character(unlist(grid[header_row, ])))
    n_dims   <- sum(dim_row != "")
    if (any(dim_row[seq_len(n_dims)] == "")) {
      stop("Stratification columns are not contiguous from the left in: ", nm)
    }

    dim_raw <- dim_row[seq_len(n_dims)]
    unknown_dims <- setdiff(dim_raw, names(DIM_LABELS))
    if (length(unknown_dims) > 0) {
      stop(
        "Unrecognized stratification column(s) in ", nm, ": ",
        paste(sQuote(unknown_dims), collapse = ", "),
        ".\nAdd them to DIM_LABELS and handle them in the transform below."
      )
    }

    # Measure columns follow the stratification columns, labeled one row above
    measure_raw <- trimws(as.character(unlist(grid[header_row - 1, ])))[-seq_len(n_dims)]
    if (length(measure_raw) == 0 || any(measure_raw == "")) {
      stop("Blank or missing measure label(s) in: ", nm)
    }

    # Each label must match exactly one known measure
    measure_names <- vapply(measure_raw, function(lbl) {
      hit <- names(MEASURE_PATTERNS)[vapply(
        MEASURE_PATTERNS, function(p) grepl(p, lbl), logical(1)
      )]
      if (length(hit) != 1) {
        stop(
          "Measure label ", sQuote(lbl), " in ", nm, " matched ", length(hit),
          " known measures (expected exactly 1).",
          "\nUpdate MEASURE_PATTERNS to cover it unambiguously."
        )
      }
      hit
    }, character(1), USE.NAMES = FALSE)

    if (anyDuplicated(measure_names)) {
      stop("Two measure columns mapped to the same output column in: ", nm)
    }

    data_raw <- grid[(header_row + 1):nrow(grid), , drop = FALSE]
    rownames(data_raw) <- NULL
    colnames(data_raw) <- c(unname(DIM_LABELS[dim_raw]), measure_names)

    message(
      "  layout: ", paste(unname(DIM_LABELS[dim_raw]), collapse = " x "),
      " | measures: ", paste(measure_names, collapse = ", ")
    )

    data_raw
  }

  raw_list <- lapply(staging_files, function(f) {
    extract_staging_data(read_slicerdicer_grid(f, xlsx_password), f)
  })
  data_raw <- bind_rows(raw_list)

  # ===========================================================================
  # 3. Load FIPS lookup
  # ===========================================================================
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    select(geography, geography_name)

  # ===========================================================================
  # 4. Clean and standardize
  # ===========================================================================

  # Strip non-UTF-8 bytes (common in SlicerDicer exports) and blank out empties
  clean_chr <- function(x) na_if(trimws(iconv(x, to = "UTF-8", sub = "")), "")

  # Percentages: "-" and blanks are suppressed -> NA. Bounded values such as
  # "<0.01%" are imputed at half the bound, matching the "10 or fewer" -> 5
  # convention used elsewhere for Epic Cosmos suppression.
  parse_pct <- function(x) {
    x[!is.na(x) & x == "-"] <- NA_character_
    bounded <- !is.na(x) & startsWith(x, "<")
    v <- suppressWarnings(as.numeric(gsub("[<>%,]", "", x)))
    v[bounded] <- v[bounded] / 2
    v
  }

  valid_months <- c(
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
  )
  valid_states <- c(state.name, "District of Columbia")

  pct_cols <- intersect(
    c("epic_pct_rsv_immunization", "epic_pct_hepb_vaccination", "epic_pct_pcv_182d"),
    names(data_raw)
  )

  data_filled <- data_raw %>%
    mutate(across(everything(), clean_chr)) %>%
    # Grouping columns are merged cells: only filled on the first row of a group
    fill(any_of(c("year", "month", "state_name")), .direction = "down")

  # Partial leading/trailing periods (e.g. "Jun 23 - Jun 30", "Jun 1 - Jun 22")
  # cover only part of a month and would understate counts, so they are dropped.
  dropped_periods <- data_filled %>%
    filter(!month %in% valid_months) %>%
    distinct(year, month)
  if (nrow(dropped_periods) > 0) {
    message(
      "Dropping ", nrow(dropped_periods), " incomplete period(s): ",
      paste(dropped_periods$year, dropped_periods$month, collapse = "; ")
    )
  }

  data_standard <- data_filled %>%
    filter(month %in% valid_months) %>%
    # Keep US states, DC, and the national total; drop "None of the above",
    # which collects patients with an unknown state of residence
    filter(state_name %in% c(valid_states, "Total")) %>%
    mutate(
      # -- Suppression, denominator --------------------------------------------
      # Patient counts of 10 or fewer are reported as "10 or fewer", imputed as 5
      epic_n_patients_suppressed_flag = if_else(
        epic_n_patients == "10 or fewer", 1L, 0L, missing = 0L
      ),
      epic_n_patients = if_else(
        epic_n_patients_suppressed_flag == 1L, "5", gsub(",", "", epic_n_patients)
      ),
      epic_n_patients = suppressWarnings(as.numeric(epic_n_patients)),

      across(all_of(pct_cols), parse_pct),

      # -- Suppression, numerators ---------------------------------------------
      # Each percentage is suppressed independently: Epic blanks the cell when
      # the numerator is 10 or fewer, so a missing percentage against an
      # observed denominator means a suppressed numerator rather than a gap in
      # reporting. (An exact 0% is never emitted, so a blank covers 0-10.)
      # Flags are computed before imputation so they reflect the source state.
      across(
        all_of(pct_cols),
        ~ as.integer(is.na(.x)),
        .names = "{.col}_suppressed_flag"
      ),

      # Impute the suppressed numerator as 5, matching the "10 or fewer" -> 5
      # convention, and re-express it on the percentage scale as 5 / n * 100.
      # Skipped where the denominator is itself suppressed: n has already been
      # imputed to 5 there, so 5/5 would assert a meaningless 100%.
      across(
        all_of(pct_cols),
        ~ if_else(
          is.na(.x) & epic_n_patients_suppressed_flag == 0L,
          5 / epic_n_patients * 100,
          .x
        )
      ),

      # Time: last day of the reporting month
      date = as.Date(paste(year, month, "01"), format = "%Y %b %d"),
      time = format(ceiling_date(date, "month") - days(1), "%Y-%m-%d"),

      # Geography: "Total" -> national ("00"), states -> 2-digit FIPS
      geography_name = if_else(state_name == "Total", "United States", state_name)
    ) %>%
    left_join(state_fips_lookup, by = "geography_name") %>%
    filter(!is.na(geography), !is.na(time))

  # Standardize age labels when the export carries an age stratification:
  # "Less than 1 Years" -> "<1 Years", "1 Years or more" -> "1+ Years",
  # "Total: ..." -> "Total"
  if ("age" %in% names(data_standard)) {
    data_standard <- data_standard %>%
      mutate(
        age = stringr::str_replace(age, "^Less than\\s+(\\d+).*$", "<\\1 Years"),
        age = stringr::str_replace(age, "^(\\d+)\\s+Years or more$", "\\1+ Years"),
        age = if_else(grepl("^Total", age, ignore.case = TRUE), "Total", age)
      )
  }

  index_cols <- intersect(c("geography", "time", "age"), names(data_standard))

  # Each measure is followed by its own suppression flag
  measure_cols <- c("epic_n_patients", pct_cols)
  value_cols   <- as.vector(rbind(measure_cols, paste0(measure_cols, "_suppressed_flag")))

  data_standard <- data_standard %>%
    select(all_of(c(index_cols, value_cols))) %>%
    arrange(across(all_of(index_cols)))

  # ===========================================================================
  # 5. Validate
  # ===========================================================================
  dupes <- data_standard %>%
    count(across(all_of(index_cols))) %>%
    filter(n > 1)
  if (nrow(dupes) > 0) {
    stop(
      "Duplicate ", paste(index_cols, collapse = "/"), " combinations found: ",
      nrow(dupes), ". Check for overlapping staging exports."
    )
  }

  out_of_range <- data_standard %>%
    summarize(across(all_of(pct_cols), ~ sum(.x < 0 | .x > 100, na.rm = TRUE))) %>%
    unlist()
  if (any(out_of_range > 0)) {
    stop(
      "Percentage values outside [0, 100]: ",
      paste(names(out_of_range)[out_of_range > 0], collapse = ", ")
    )
  }

  # A flag of 1 must mean either an imputed value or, where the denominator was
  # itself suppressed, a value left missing. It must never coexist with an
  # unimputed value that the source actually reported.
  for (mc in measure_cols) {
    flagged_unimputed <- sum(
      data_standard[[paste0(mc, "_suppressed_flag")]] == 1L &
        is.na(data_standard[[mc]]) &
        data_standard$epic_n_patients_suppressed_flag == 0L
    )
    if (flagged_unimputed > 0) {
      stop("Suppressed but unimputed values remain in ", mc, ": ", flagged_unimputed)
    }
  }

  message(
    "Standardized ", nrow(data_standard), " rows | ",
    length(unique(data_standard$geography)), " geographies | ",
    min(data_standard$time), " to ", max(data_standard$time)
  )
  for (mc in measure_cols) {
    message(
      "  ", format(mc, width = 26), " suppressed: ",
      sum(data_standard[[paste0(mc, "_suppressed_flag")]]),
      " | still NA: ", sum(is.na(data_standard[[mc]]))
    )
  }

  # ===========================================================================
  # 6. Write standardized output
  # ===========================================================================
  if (!dir.exists("standard")) dir.create("standard")

  vroom::vroom_write(data_standard, "standard/data.csv.gz", delim = ",")

  # ===========================================================================
  # 7. Record processed state
  # ===========================================================================
  process$raw_state <- current_state
  dcf::dcf_process_record(updated = process)
}
