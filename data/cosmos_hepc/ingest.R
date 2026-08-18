# =============================================================================
# Epic Cosmos Hepatitis C Data Ingestion
# Source: Epic Cosmos SlicerDicer exports
#         raw/staging/*.xlsx             -- hepatitis C diagnosis counts
#         raw/staging_medication/*.xlsx  -- HCV medication uptake
#
# Two crosstabs, merged into one standard/data.csv.gz keyed by
# (geography, time, age):
#
# 1. raw/staging/ (Session ID 2850163; predecessors 2850139/2847109/2847096)
#    -- annual counts of patients with a viral hepatitis C diagnosis, and the
#    all-patients denominator, by state of residence and age at encounter.
#    Population base: All Patients; Country of Care = United States of
#    America; Has Any Encounters?  (no diagnosis filter on the population).
#    Diagnosis is a ROW dimension with two buckets per state/year/age:
#      "hepc"       -> patients with a viral hepatitis C diagnosis (numerator)
#      "Total: ..." -> all patients in that state/year/age cell (denominator)
#    epic_pct_hepc = numerator / denominator * 100.
#
# 2. raw/staging_medication/ (Session ID 2850012) -- among patients with an
#    HCV medication on record (itself among patients meeting the SAME viral
#    hepatitis C diagnosis criteria as crosstab 1, applied here as a
#    POPULATION filter rather than a row bucket), the rate of a named Epic
#    Cosmos quality measure ("hcv medication measure"), by state of
#    residence and age at encounter. "All Medications" is a ROW dimension
#    with two buckets:
#      "has HCV medication" -> the bucket actually used (see below)
#      "Total"               -> all HCV-diagnosed patients in that cell,
#                                unused here but CONFIRMED by inspection to
#                                match epic_n_patients_hepc exactly at every
#                                (geography, time, age) cell checked as of
#                                the 2026-08-18 (10:48) export
#    The "has HCV medication" bucket carries two columns: "Number of
#    Patients" (this bucket's own population -- patients with an HCV
#    medication on record) and "hcv medication measure (%)" (this
#    population's own rate for the named measure, e.g. ~70-95% in the
#    2026-08-18 export -- CONFIRMED against user domain knowledge to be the
#    intended reading, not the ~5-35% that would result from treating the
#    "Total" bucket's population as the denominator instead). The measure
#    does NOT provide a numerator count directly:
#      epic_n_patients_hepc_medication_pop = "has HCV medication" bucket's
#        own population (the denominator)
#      epic_pct_hepc_medication = "has HCV medication" bucket's own rate
#        (reported directly, or imputed via the standard 5 / denominator
#        rule when blank)
#      epic_n_patients_hepc_medication = round(population * rate / 100),
#        the DERIVED numerator -- not a value the source reports anywhere
#
# CAVEAT on the numerator's code set (both crosstabs): Session 2850012's own
# Population Criteria Filters read: "Chronic viral hepatitis C (ICD-10-CM:
# B18.2), Acute hepatitis E (ICD-10-CM: B17.2), Unspecified viral hepatitis
# C". B17.2 is NOT a hepatitis C code. This matches the predecessor session
# (2847096) used to define crosstab 1's "hepc" bucket (see
# epic_n_patients_hepc's long_description), so BOTH crosstabs' HCV-diagnosed
# populations include a small number of acute hepatitis E patients. Fix the
# bucket definition in SlicerDicer and re-export both sessions to remove it.
#
# CAVEAT on cross-crosstab comparison: epic_n_patients_hepc (crosstab 1) and
# crosstab 2's own "Total" bucket (unused, not the same as
# epic_n_patients_hepc_medication_pop, which is the "has HCV medication"
# bucket's smaller population) count the same conceptual population --
# HCV-diagnosed patients -- and were confirmed to match cell-for-cell in the
# 2026-08-18 (10:48) exports checked,
# but come from separately exported SlicerDicer sessions with independently
# set session date ranges (crosstab 1 ends 2026-07-28; crosstab 2 ends
# 2026-06-30 as of this update), so their trailing partial periods do NOT
# align and future exports are not guaranteed to match as closely. Treat
# them as related but distinct measures; do not substitute one for the
# other without re-checking.
#
# Layout notes (resolved from each export's own header rows, not
# hard-coded):
#   Rows 1-9   session metadata (Session Title, ID, Data Model, Population
#              Base, Population Criteria Filters, Session Date Range,
#              Measure [crosstab 1 only], Export User, Date of Export)
#   Rows 10-11 blank
#   Crosstab 1: Row 12 is the column axis label in the last row-index column,
#     then one age-bucket label per value column; Row 13 is the row index
#     header (Year, State of Residence, Diagnosis (All)); Row 14+ is data,
#     with the first two index columns merged/filled down and the third
#     (Diagnosis (All)) repeating every row.
#   Crosstab 2: Row 11 is a "Measures" header over two named value columns
#     (Number of Patients, hcv medication measure (%)); Row 12 is the row
#     index header (Year, State of Residence, All Medications, Age at
#     Encounter in Years -- FOUR dimensions); Row 13+ is data, with the
#     first three index columns merged/filled down and the fourth (Age at
#     Encounter in Years) repeating every row.
#
# Dropped rows (each reported via message()):
#   - "None of the above" (unknown state of residence) and non-state
#     geographies (Puerto Rico and other territories)
#
# Partial periods are RETAINED per crosstab (not dropped), each carrying its
# own true period-end date in `time` rather than a whole year. `time` is the
# period END date, so a whole year is YYYY-12-31 while a trailing partial
# period keeps its real end date. The two crosstabs' partial periods do NOT
# necessarily align (see the cross-crosstab caveat above) -- validation
# therefore checks "at most one partial period, and it is the most recent"
# separately per crosstab, not on the merged file as a whole.
#
# Suppression: counts of 10 or fewer arrive as "10 or fewer" (or blank); they
# are imputed as 5 and flagged. Flags are computed before imputation. A
# derived percent inherits a flag of 1 when either its numerator or its
# denominator was suppressed; when the DENOMINATOR was suppressed the percent
# is left NA rather than asserting a meaningless 5/5 = 100% (the
# cosmos_vaccines precedent).
#
# Age note: the export's overall-age column/row is carried through as
# age = "Total". It is the source's own total and may include patients whose
# age at encounter is unknown, so it can exceed the sum of the six age
# buckets slightly -- use the "Total" row for overall figures rather than
# summing the buckets. Both crosstabs have the age "Total" enabled as of the
# 2026-08-18 (10:28) update to crosstab 1.
#
# Crosstab 1's history: session 2850163 covers 2020-2025 plus a 2026 partial
# period only (its predecessor, 2850139, covered 2018-2025 but lacked the age
# "Total" column -- the update traded history depth for the Total row). Rows
# for 2018 and 2019 therefore have NA for crosstab 1's measures
# (epic_n_patients_hepc, epic_n_patients, epic_pct_hepc) but are still
# populated for crosstab 2's medication measures.
# =============================================================================

library(dplyr)
library(tidyr)
library(openxlsx2)

# Check that msoffcrypto-tool is available (required to decrypt SlicerDicer xlsx)
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
  process <- list(raw_state = NULL, medication_raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

# Password for xlsx files (set in .Renviron via usethis::edit_r_environ())
xlsx_password <- Sys.getenv("EPIC_XLSX_PASSWORD")

# -----------------------------------------------------------------------------
# Label maps: the contract with each export. Extend these when the
# SlicerDicer session changes; anything unrecognized stops the run rather
# than landing on the wrong column.
# -----------------------------------------------------------------------------

# --- Crosstab 1: hepatitis C diagnosis counts (raw/staging/) ---------------
DIM_LABELS <- c(
  "Year"               = "year",
  "State of Residence" = "state_name",
  "Diagnosis (All)"    = "dx"
)
COL_AXIS_LABEL <- "Age at Encounter in Years"
AGE_EXPECTED <- c(
  "<5 Years", "5-17 Years", "18-24 Years", "25-44 Years", "45-64 Years",
  "65+ Years", "Total"
)
DX_LABELS <- c("hepc" = "hepc")
DENOM_COL <- "epic_n_patients"

# --- Crosstab 2: HCV medication uptake (raw/staging_medication/) -----------
# Layout differs from crosstab 1: FOUR row dimensions (Year, State of
# Residence, All Medications, Age at Encounter in Years) followed by two
# NAMED measure columns under a "Measures" header ("Number of Patients",
# "hcv medication measure (%)") rather than one dimension's buckets spread
# across columns -- see extract_medication_data() below.
DIM_LABELS_MED <- c(
  "Year"                      = "year",
  "State of Residence"        = "state_name",
  "All Medications"           = "med_status",
  "Age at Encounter in Years" = "age_raw"
)
MED_EXPECTED  <- c("has HCV medication", "Total")
MED_NUM_COL   <- "epic_n_patients_hepc_medication"
MED_DENOM_COL <- "epic_n_patients_hepc_medication_pop"

# The measure both sessions report; asserted against each export's metadata
MEASURE_PATTERN <- "^Number of Patients"

# Convert SlicerDicer age bucket text to the repo's age labels (shared by
# both crosstabs -- crosstab 1 applies it to column headers, crosstab 2 to a
# row-dimension value). Matching is on the digits rather than the ">=" glyph,
# which is a non-ASCII character that varies between exports.
standardize_age_label <- function(x) {
  a <- sub("^Total.*$", "Total", x)
  a <- sub("^Less than\\s+([0-9]+)\\s+Years?$", "<\\1 Years", a)
  a <- sub("^([0-9]+)\\s+Years or more$", "\\1+ Years", a)

  # "<glyph> L and < U Years" -> "L-(U-1) Years"
  g <- regmatches(a, regexec("^[^0-9]*([0-9]+)\\s+and\\s+<\\s*([0-9]+)\\s*Years?$", a))
  for (i in seq_along(a)) {
    if (length(g[[i]]) == 3L) {
      a[i] <- paste0(g[[i]][2], "-", as.integer(g[[i]][3]) - 1L, " Years")
    }
  }
  a
}

# "Total: Total includes ..." -> "Total"; "has HCV medication" unchanged
standardize_med_label <- function(x) sub("^Total.*$", "Total", x)

# Convert a SlicerDicer Year row label to its period END date, which is what
# `time` carries. A whole year ("2020") ends 2020-12-31. A partial period
# ("Jan 1 - Jul 22 2026", with any dash glyph) ends on the date after the
# dash, so the truncated coverage is visible in `time` itself rather than
# being disguised as a full year.
parse_period_end <- function(x) {
  labels <- unique(x)
  ends   <- as.Date(rep(NA, length(labels)))
  partial <- !grepl("^[0-9]{4}$", labels)

  ends[!partial] <- as.Date(paste0(labels[!partial], "-12-31"))

  for (i in which(partial)) {
    lbl  <- labels[i]
    year <- regmatches(lbl, regexpr("[0-9]{4}", lbl))
    # Take the text after the final dash (hyphen, en dash or em dash)
    seg <- trimws(tail(strsplit(lbl, "[-–—]")[[1]], 1))
    if (!grepl("[0-9]{4}", seg)) seg <- paste(seg, year)
    d <- as.Date(seg, format = "%b %d %Y")
    if (is.na(d)) d <- as.Date(seg, format = "%B %d %Y")
    ends[i] <- d
  }

  if (any(is.na(ends))) {
    stop(
      "Could not parse a period end date from Year label(s): ",
      paste(labels[is.na(ends)], collapse = " | "), "\n",
      "Extend parse_period_end() in ingest.R."
    )
  }

  list(
    date    = ends[match(x, labels)],
    partial = partial[match(x, labels)]
  )
}

# -----------------------------------------------------------------------------
# 1. Locate staging files and detect change (each crosstab tracked
#    independently, since either can be refreshed on its own schedule)
# -----------------------------------------------------------------------------
staging_files <- list.files(
  "raw/staging",
  pattern = "\\.(csv|xlsx)$", full.names = TRUE
)
if (length(staging_files) == 0) {
  stop(
    "No staging files found in raw/staging/.\n",
    "Export the hepatitis C diagnosis crosstab from Epic Cosmos SlicerDicer ",
    "and place the .xlsx file there."
  )
}

med_staging_files <- list.files(
  "raw/staging_medication",
  pattern = "\\.(csv|xlsx)$", full.names = TRUE
)
if (length(med_staging_files) == 0) {
  stop(
    "No staging files found in raw/staging_medication/.\n",
    "Export the HCV medication crosstab from Epic Cosmos SlicerDicer and ",
    "place the .xlsx file there."
  )
}

current_state <- list(
  files  = staging_files,
  hashes = unname(tools::md5sum(staging_files))
)
current_med_state <- list(
  files  = med_staging_files,
  hashes = unname(tools::md5sum(med_staging_files))
)

if (!identical(process$raw_state, current_state) ||
    !identical(process$medication_raw_state, current_med_state)) {

  # ---------------------------------------------------------------------------
  # 2. Read the raw grids
  # ---------------------------------------------------------------------------

  # Read a SlicerDicer export as a character grid, with no header interpretation
  read_slicerdicer_grid <- function(file, password = NULL) {
    message("Reading: ", basename(file))

    if (grepl("\\.xlsx$", file, ignore.case = TRUE)) {
      load_file <- file
      if (!is.null(password) && nzchar(password)) {
        decrypted <- tempfile(fileext = ".xlsx")
        cmd <- sprintf(
          'python -m msoffcrypto -p "%s" "%s" "%s"',
          password, normalizePath(file, winslash = "/"), decrypted
        )
        if (system(cmd) != 0) stop("Decryption failed: ", file)
        load_file <- decrypted
        on.exit(unlink(decrypted), add = TRUE)
      }
      grid <- suppressWarnings(openxlsx2::wb_to_df(
        openxlsx2::wb_load(load_file), sheet = 1, col_names = FALSE,
        skip_empty_rows = FALSE, skip_empty_cols = FALSE
      ))
    } else {
      grid <- read.csv(
        file, header = FALSE, stringsAsFactors = FALSE, check.names = FALSE,
        fileEncoding = "windows-1252"
      )
    }

    # Everything to character, blanks as "", non-UTF-8 bytes stripped
    as.data.frame(
      lapply(grid, function(x) {
        x <- iconv(as.character(x), to = "UTF-8", sub = "")
        x[is.na(x)] <- ""
        trimws(x)
      }),
      stringsAsFactors = FALSE
    )
  }

  # Generic long-format extraction for a 3-row-dimension x N-column-value
  # SlicerDicer crosstab. The first two row dimensions (Year, State of
  # Residence) are merged cells and filled down; the third repeats on every
  # row already. Column labels are standardized and validated against
  # `expected_col_labels`, so an unrecognized layout stops the run rather
  # than landing on the wrong column.
  extract_staging_data <- function(grid, file_label, dim_labels, col_axis_label,
                                    col_axis_name, expected_col_labels,
                                    standardize_col_label = identity) {
    dim_names <- names(dim_labels)
    n_dim <- length(dim_names)

    # The row index header is the first row whose leading cells are exactly
    # the declared dimension labels
    hdr <- NA_integer_
    for (i in seq_len(nrow(grid))) {
      if (identical(as.character(grid[i, seq_along(dim_names)]), dim_names)) {
        hdr <- i
        break
      }
    }
    if (is.na(hdr)) {
      stop(
        file_label, ": could not find the row index header.\n",
        "Expected the leading cells of a row to be: ",
        paste(dim_names, collapse = " | "), "\n",
        "Update the DIM_LABELS constant in ingest.R to match the export."
      )
    }

    # Any extra non-empty cell in the header row is an undeclared dimension
    extra_dims <- setdiff(as.character(grid[hdr, ])[-seq_len(n_dim)], "")
    if (length(extra_dims) > 0) {
      stop(
        file_label, ": unrecognized row dimension(s) in the header row: ",
        paste(extra_dims, collapse = ", "), "\n",
        "Add them to the DIM_LABELS constant in ingest.R."
      )
    }

    # The row above carries the column axis label in the last index column,
    # then one bucket label per value column
    axis_row <- as.character(grid[hdr - 1L, ])
    if (!identical(axis_row[n_dim], col_axis_label)) {
      stop(
        file_label, ": expected column axis label '", col_axis_label,
        "' in column ", n_dim, " of row ", hdr - 1L,
        " but found '", axis_row[n_dim], "'.\n",
        "Update the column axis label constant in ingest.R."
      )
    }

    val_cols   <- (n_dim + 1L):ncol(grid)
    col_labels <- axis_row[val_cols]
    if (any(col_labels == "")) {
      stop(
        file_label, ": blank column label(s) at position(s) ",
        paste(val_cols[col_labels == ""], collapse = ", "),
        " of row ", hdr - 1L, "."
      )
    }

    col_std <- standardize_col_label(col_labels)
    if (!all(col_std %in% expected_col_labels)) {
      stop(
        file_label, ": unrecognized column label(s): ",
        paste(col_labels[!col_std %in% expected_col_labels], collapse = " | "), "\n",
        "Extend the expected-label constant / standardize function in ingest.R."
      )
    }
    if (anyDuplicated(col_std) > 0) {
      stop(
        file_label, ": column labels map to duplicate standard labels: ",
        paste(col_std[duplicated(col_std)], collapse = ", ")
      )
    }

    # Assert the measure recorded in the metadata block
    meta_measure <- grid[grid[[1]] == "Measure", 2]
    if (length(meta_measure) == 1 && !grepl(MEASURE_PATTERN, meta_measure)) {
      stop(
        file_label, ": metadata reports measure '", meta_measure,
        "' which does not match MEASURE_PATTERN ('", MEASURE_PATTERN, "').\n",
        "Update MEASURE_PATTERN in ingest.R, or re-export the session."
      )
    }

    body <- grid[(hdr + 1L):nrow(grid), , drop = FALSE]
    rownames(body) <- NULL
    names(body) <- c(unname(dim_labels), col_std)

    fill_cols <- unname(dim_labels)[1:2]   # Year, State of Residence: merged, filled down
    last_dim  <- unname(dim_labels)[n_dim] # populated on every data row already

    # Merged index cells: blanks to NA, fill the first two down; the third
    # dimension is expected on every row, so a blank there (e.g. a trailing
    # padding row in the sheet) is dropped rather than filled
    body %>%
      mutate(across(all_of(unname(dim_labels)), ~ na_if(.x, ""))) %>%
      tidyr::fill(all_of(fill_cols), .direction = "down") %>%
      filter(!is.na(.data[[last_dim]])) %>%
      tidyr::pivot_longer(
        cols = all_of(col_std), names_to = col_axis_name, values_to = "n_raw"
      ) %>%
      mutate(source_file = file_label)
  }

  # Crosstab 2's layout is FOUR row dimensions followed by two NAMED measure
  # columns under a "Measures" header, rather than one dimension's buckets
  # spread across columns -- extract_staging_data()'s column-axis-pivot shape
  # does not fit, so this is a dedicated parser. Both "Number of Patients"
  # (the measure's own population/denominator) and "hcv medication measure
  # (%)" (the measure's own rate) are kept -- see the CAVEAT at the top of
  # this file: the source does not provide a numerator count directly, so it
  # is derived downstream as round(population * rate / 100).
  extract_medication_data <- function(grid, file_label, dim_labels, expected_med_labels) {
    dim_names <- names(dim_labels)
    n_dim <- length(dim_names)

    hdr <- NA_integer_
    for (i in seq_len(nrow(grid))) {
      if (identical(as.character(grid[i, seq_along(dim_names)]), dim_names)) {
        hdr <- i
        break
      }
    }
    if (is.na(hdr)) {
      stop(
        file_label, ": could not find the row index header.\n",
        "Expected the leading cells of a row to be: ",
        paste(dim_names, collapse = " | "), "\n",
        "Update DIM_LABELS_MED in ingest.R to match the export."
      )
    }

    measures_row <- as.character(grid[hdr - 1L, ])
    if (!identical(measures_row[n_dim], "Measures")) {
      stop(
        file_label, ": expected 'Measures' label in column ", n_dim,
        " of row ", hdr - 1L, " but found '", measures_row[n_dim], "'.\n",
        "Update extract_medication_data() in ingest.R."
      )
    }
    measure_labels <- measures_row[(n_dim + 1L):ncol(grid)]
    if (!identical(measure_labels[1], "Number of Patients")) {
      stop(
        file_label, ": expected 'Number of Patients' as the first measure ",
        "column, found '", measure_labels[1], "'.\n",
        "Update extract_medication_data() in ingest.R."
      )
    }

    body <- grid[(hdr + 1L):nrow(grid), , drop = FALSE]
    rownames(body) <- NULL
    names(body) <- c(unname(dim_labels), "n_raw", "pct_raw")

    fill_cols <- unname(dim_labels)[1:3]   # Year, State, All Medications: merged, filled down
    last_dim  <- unname(dim_labels)[n_dim] # Age at Encounter: populated on every row

    body <- body %>%
      mutate(across(all_of(unname(dim_labels)), ~ na_if(.x, ""))) %>%
      tidyr::fill(all_of(fill_cols), .direction = "down") %>%
      filter(!is.na(.data[[last_dim]]))

    med_std <- standardize_med_label(body$med_status)
    if (!all(med_std %in% expected_med_labels)) {
      stop(
        file_label, ": unrecognized 'All Medications' bucket label(s): ",
        paste(unique(body$med_status[!med_std %in% expected_med_labels]), collapse = " | "), "\n",
        "Extend MED_EXPECTED / standardize_med_label() in ingest.R."
      )
    }

    body %>%
      mutate(med_status = med_std, source_file = file_label)
  }

  data_raw_hepc <- bind_rows(lapply(staging_files, function(f) {
    extract_staging_data(
      read_slicerdicer_grid(f, xlsx_password), basename(f),
      DIM_LABELS, COL_AXIS_LABEL, "age", AGE_EXPECTED, standardize_age_label
    )
  }))

  data_raw_med <- bind_rows(lapply(med_staging_files, function(f) {
    extract_medication_data(
      read_slicerdicer_grid(f, xlsx_password), basename(f),
      DIM_LABELS_MED, MED_EXPECTED
    )
  }))

  # ---------------------------------------------------------------------------
  # 3. Load FIPS lookup (shared by both crosstabs)
  # ---------------------------------------------------------------------------
  all_fips <- vroom::vroom(
    "../../resources/all_fips.csv.gz",
    show_col_types = FALSE
  )

  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    select(geography, geography_name)

  valid_states <- c(state.name, "District of Columbia", "Total")

  # Shared validation, parameterized per crosstab so each one's own partial
  # period (which need not align with the other crosstab's) is checked
  # independently rather than on the merged file as a whole.
  validate_crosstab_standard <- function(df, num_cols, denom_col, pct_cols, label) {
    index_cols <- c("geography", "time", "age")

    if (anyDuplicated(df[, index_cols]) > 0) {
      stop(label, ": duplicate rows for the same geography / time / age combination.")
    }
    if (!all(grepl("^[0-9]{2}$", df$geography))) {
      stop(label, ": non-2-digit-FIPS geography values found.")
    }
    if (!"00" %in% df$geography) {
      stop(label, ": national row ('00') is missing from the output.")
    }
    if (!all(grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", df$time))) {
      stop(label, ": time is not formatted as YYYY-mm-dd for all rows.")
    }
    partial_times <- sort(unique(df$time[!grepl("-12-31$", df$time)]))
    if (length(partial_times) > 1) {
      stop(
        label, ": more than one partial period in the output: ",
        paste(partial_times, collapse = " | "),
        "\nOnly a trailing partial period is expected."
      )
    }
    if (length(partial_times) == 1 && partial_times != max(df$time)) {
      stop(
        label, ": partial period ", partial_times, " is not the most recent ",
        "period; a partial period in the middle of the series would ",
        "silently understate it."
      )
    }

    denom_flag   <- paste0(denom_col, "_suppressed_flag")
    measure_cols <- c(num_cols, denom_col, pct_cols)

    for (mc in measure_cols) {
      fc <- paste0(mc, "_suppressed_flag")
      if (!fc %in% names(df)) stop(label, ": measure ", mc, " has no ", fc, " column.")
      if (any(is.na(df[[fc]])) || !all(df[[fc]] %in% c(0L, 1L))) {
        stop(label, ": ", fc, " must be 0 or 1 with no missing values.")
      }
      if (any(df[[mc]] < 0, na.rm = TRUE)) stop(label, ": ", mc, " has negative values.")
    }

    for (mc in c(num_cols, denom_col)) {
      fc <- paste0(mc, "_suppressed_flag")
      if (any(is.na(df[[mc]]))) {
        stop(
          label, ": ", mc, " has ", sum(is.na(df[[mc]])),
          " missing values; every suppressed count should be imputed to 5."
        )
      }
      if (any(df[[fc]] == 1L & df[[mc]] != 5)) {
        stop(label, ": a suppressed cell in ", mc, " does not carry the imputed value 5.")
      }
    }

    for (pc in pct_cols) {
      v <- df[[pc]]
      if (any(v < 0 | v > 100, na.rm = TRUE)) {
        stop(label, ": ", pc, " has values outside [0, 100].")
      }
      if (any(is.na(v) & df[[denom_flag]] == 0L)) {
        stop(label, ": ", pc, " is NA where the denominator was not suppressed.")
      }
      if (any(df[[paste0(pc, "_suppressed_flag")]] == 0L & is.na(v))) {
        stop(label, ": ", pc, " is NA on a cell flagged as not suppressed.")
      }
    }

    for (nc in num_cols) {
      over <- which(df[[nc]] > df[[denom_col]])
      if (length(over) > 0) {
        stop(
          label, ": ", nc, " exceeds ", denom_col, " in ", length(over),
          " cell(s), e.g. ", df$geography[over[1]], " / ", df$time[over[1]],
          " / ", df$age[over[1]]
        )
      }
    }

    invisible(TRUE)
  }

  # ---------------------------------------------------------------------------
  # 4a. Crosstab 1 -- hepatitis C diagnosis counts
  # ---------------------------------------------------------------------------
  data_raw <- data_raw_hepc %>%
    mutate(
      measure_col = if_else(
        grepl("^Total", dx),
        DENOM_COL,
        paste0("epic_n_patients_", unname(DX_LABELS[tolower(dx)]))
      )
    )

  unknown_dx <- unique(data_raw$dx[is.na(data_raw$measure_col)])
  if (length(unknown_dx) > 0) {
    stop(
      "Crosstab 1 (hepc): unrecognized diagnosis bucket(s): ",
      paste(unknown_dx, collapse = " | "),
      "\nAdd them to DX_LABELS in ingest.R."
    )
  }
  if (!DENOM_COL %in% data_raw$measure_col) {
    stop(
      "Crosstab 1 (hepc): no per-state 'Total:' denominator row found in the export.\n",
      "Enable the diagnosis Total row in SlicerDicer and re-export, or set ",
      "DENOM_COL handling in ingest.R accordingly."
    )
  }

  periods <- parse_period_end(data_raw$year)
  if (any(periods$partial)) {
    for (lbl in unique(data_raw$year[periods$partial])) {
      message(
        "Crosstab 1 (hepc): retaining partial period '", lbl, "' as time = ",
        format(periods$date[match(lbl, data_raw$year)], "%Y-%m-%d"),
        " - its counts cover only part of the year and are NOT comparable to a ",
        "full year."
      )
    }
  }

  dropped_geo <- setdiff(unique(data_raw$state_name), valid_states)
  if (length(dropped_geo) > 0) {
    message(
      "Crosstab 1 (hepc): dropping non-state / unknown geography rows: ",
      paste(dropped_geo, collapse = " | ")
    )
    data_raw <- data_raw %>% filter(state_name %in% valid_states)
  }

  data_long <- data_raw %>%
    mutate(
      geography_name = if_else(state_name == "Total", "United States", state_name)
    ) %>%
    left_join(state_fips_lookup, by = "geography_name") %>%
    mutate(
      time = format(parse_period_end(year)$date, "%Y-%m-%d"),
      suppressed_flag = if_else(
        is.na(n_raw) | n_raw == "" | n_raw == "10 or fewer", 1L, 0L,
        missing = 1L
      ),
      n_value = if_else(suppressed_flag == 1L, "5", gsub(",", "", n_raw)),
      n_value = as.numeric(n_value)
    )

  if (any(is.na(data_long$geography))) {
    stop(
      "Crosstab 1 (hepc): unmatched geography name(s): ",
      paste(unique(data_long$geography_name[is.na(data_long$geography)]), collapse = " | ")
    )
  }
  if (any(is.na(data_long$time))) {
    stop("Crosstab 1 (hepc): failed to parse year for ", sum(is.na(data_long$time)), " rows.")
  }
  if (any(is.na(data_long$n_value))) {
    stop(
      "Crosstab 1 (hepc): unparseable count value(s): ",
      paste(unique(data_long$n_raw[is.na(data_long$n_value)]), collapse = " | "),
      "\nAdd the new suppression marker to the flag logic in ingest.R."
    )
  }

  index_cols <- c("geography", "time", "age")

  if (anyDuplicated(data_long[, c(index_cols, "measure_col")]) > 0) {
    stop("Crosstab 1 (hepc): duplicate cells for the same geography / time / age / measure.")
  }

  wide_values <- data_long %>%
    select(all_of(index_cols), measure_col, n_value) %>%
    tidyr::pivot_wider(names_from = measure_col, values_from = n_value)

  wide_flags <- data_long %>%
    select(all_of(index_cols), measure_col, suppressed_flag) %>%
    mutate(measure_col = paste0(measure_col, "_suppressed_flag")) %>%
    tidyr::pivot_wider(names_from = measure_col, values_from = suppressed_flag)

  num_cols   <- paste0("epic_n_patients_", unname(DX_LABELS))
  pct_cols   <- paste0("epic_pct_", unname(DX_LABELS))
  denom_flag <- paste0(DENOM_COL, "_suppressed_flag")

  hepc_standard <- wide_values %>%
    left_join(wide_flags, by = index_cols)

  for (i in seq_along(num_cols)) {
    nc <- num_cols[i]
    pc <- pct_cols[i]
    nf <- paste0(nc, "_suppressed_flag")

    hepc_standard[[pc]] <- if_else(
      hepc_standard[[denom_flag]] == 1L,
      NA_real_,
      hepc_standard[[nc]] / hepc_standard[[DENOM_COL]] * 100
    )
    hepc_standard[[paste0(pc, "_suppressed_flag")]] <- as.integer(
      hepc_standard[[nf]] == 1L | hepc_standard[[denom_flag]] == 1L
    )
  }

  hepc_measure_cols <- c(num_cols, DENOM_COL, pct_cols)
  hepc_flag_cols    <- paste0(hepc_measure_cols, "_suppressed_flag")

  hepc_standard <- hepc_standard %>%
    select(all_of(index_cols), all_of(as.vector(rbind(hepc_measure_cols, hepc_flag_cols)))) %>%
    arrange(geography, time, age)

  validate_crosstab_standard(
    hepc_standard, num_cols = num_cols, denom_col = DENOM_COL,
    pct_cols = pct_cols, label = "Crosstab 1 (hepc)"
  )

  # ---------------------------------------------------------------------------
  # 4b. Crosstab 2 -- HCV medication measure
  #
  # Only the "has HCV medication" bucket is used (the "Total" bucket, which
  # matches epic_n_patients_hepc, is validated as present for structural
  # sanity but not otherwise used here -- see the CAVEAT at the top of this
  # file). The source gives this bucket's own population ("Number of
  # Patients") and its own rate ("hcv medication measure (%)") directly, but
  # NOT the count of patients meeting the measure -- that numerator is
  # derived below as round(population * rate / 100).
  # ---------------------------------------------------------------------------
  bad_age <- unique(
    data_raw_med$age_raw[!standardize_age_label(data_raw_med$age_raw) %in% AGE_EXPECTED]
  )
  if (length(bad_age) > 0) {
    stop(
      "Crosstab 2 (medication): unrecognized age bucket label(s): ",
      paste(bad_age, collapse = " | "),
      "\nExtend AGE_EXPECTED / standardize_age_label() in ingest.R."
    )
  }
  present_med <- unique(data_raw_med$med_status)
  if (!all(MED_EXPECTED %in% present_med)) {
    stop(
      "Crosstab 2 (medication): expected buckets ",
      paste(MED_EXPECTED, collapse = " and "), " in the export; found: ",
      paste(present_med, collapse = ", ")
    )
  }

  data_raw_med <- data_raw_med %>%
    filter(med_status == "has HCV medication") %>%
    mutate(age = standardize_age_label(age_raw))

  periods_med <- parse_period_end(data_raw_med$year)
  if (any(periods_med$partial)) {
    for (lbl in unique(data_raw_med$year[periods_med$partial])) {
      message(
        "Crosstab 2 (medication): retaining partial period '", lbl, "' as time = ",
        format(periods_med$date[match(lbl, data_raw_med$year)], "%Y-%m-%d"),
        " - its counts cover only part of the year and are NOT comparable to a ",
        "full year."
      )
    }
  }

  dropped_geo_med <- setdiff(unique(data_raw_med$state_name), valid_states)
  if (length(dropped_geo_med) > 0) {
    message(
      "Crosstab 2 (medication): dropping non-state / unknown geography rows: ",
      paste(dropped_geo_med, collapse = " | ")
    )
    data_raw_med <- data_raw_med %>% filter(state_name %in% valid_states)
  }

  medication_standard <- data_raw_med %>%
    mutate(
      geography_name = if_else(state_name == "Total", "United States", state_name)
    ) %>%
    left_join(state_fips_lookup, by = "geography_name") %>%
    mutate(
      time = format(parse_period_end(year)$date, "%Y-%m-%d"),
      # Rule 1: the population/denominator count
      denom_suppressed = if_else(
        is.na(n_raw) | n_raw == "" | n_raw == "10 or fewer", 1L, 0L, missing = 1L
      ),
      denom_value = as.numeric(if_else(denom_suppressed == 1L, "5", gsub(",", "", n_raw))),
      # A blank rate means the underlying numerator was suppressed. Confirmed
      # against the 2026-08-18 export: every row with a suppressed
      # population also had a blank rate, and a further 60 rows had a normal
      # population but a blank rate (a numerator small enough to withhold on
      # its own) -- no row had a suppressed population with a rate shown.
      pct_parsed = as.numeric(gsub("%", "", pct_raw)),
      pct_suppressed = as.integer(is.na(pct_parsed))
    )

  if (any(is.na(medication_standard$geography))) {
    stop(
      "Crosstab 2 (medication): unmatched geography name(s): ",
      paste(unique(medication_standard$geography_name[is.na(medication_standard$geography)]), collapse = " | ")
    )
  }
  if (any(is.na(medication_standard$time))) {
    stop("Crosstab 2 (medication): failed to parse year for ", sum(is.na(medication_standard$time)), " rows.")
  }
  if (any(is.na(medication_standard$denom_value))) {
    stop(
      "Crosstab 2 (medication): unparseable count value(s): ",
      paste(unique(medication_standard$n_raw[is.na(medication_standard$denom_value)]), collapse = " | "),
      "\nAdd the new suppression marker to the flag logic in ingest.R."
    )
  }
  if (anyDuplicated(medication_standard[, index_cols]) > 0) {
    stop("Crosstab 2 (medication): duplicate cells for the same geography / time / age.")
  }

  medication_standard[[MED_DENOM_COL]] <- medication_standard$denom_value
  medication_standard[[paste0(MED_DENOM_COL, "_suppressed_flag")]] <- medication_standard$denom_suppressed

  # The rate: reported directly when present; imputed via 5 / denominator
  # when blank and the denominator itself was not suppressed; left NA when
  # the denominator WAS suppressed (cosmos_vaccines precedent -- 5 / 5 * 100
  # would assert a meaningless 100%).
  medication_standard$epic_pct_hepc_medication <- with(medication_standard, case_when(
    !is.na(pct_parsed) ~ pct_parsed,
    denom_suppressed == 1L ~ NA_real_,
    TRUE ~ 5 / denom_value * 100
  ))
  medication_standard$epic_pct_hepc_medication_suppressed_flag <- medication_standard$pct_suppressed

  # The numerator: not reported by the source at all, so it is derived from
  # the population and the (possibly imputed) rate. A suppressed cell is
  # imputed to 5 directly (matching Rule 1 for counts) rather than via
  # round(denom * pct / 100), which would just round-trip back to 5 anyway
  # but is more fragile to floating-point noise.
  medication_standard[[MED_NUM_COL]] <- with(medication_standard, if_else(
    pct_suppressed == 1L, 5, round(denom_value * epic_pct_hepc_medication / 100)
  ))
  medication_standard[[paste0(MED_NUM_COL, "_suppressed_flag")]] <- medication_standard$pct_suppressed

  med_measure_cols <- c(MED_NUM_COL, MED_DENOM_COL, "epic_pct_hepc_medication")
  med_flag_cols    <- paste0(med_measure_cols, "_suppressed_flag")

  medication_standard <- medication_standard %>%
    select(all_of(index_cols), all_of(as.vector(rbind(med_measure_cols, med_flag_cols)))) %>%
    arrange(geography, time, age)

  validate_crosstab_standard(
    medication_standard, num_cols = MED_NUM_COL, denom_col = MED_DENOM_COL,
    pct_cols = "epic_pct_hepc_medication", label = "Crosstab 2 (medication)"
  )

  # ---------------------------------------------------------------------------
  # 5. Merge the two crosstabs. A full join on (geography, time, age) merges
  #    them without truncating either source to the other's max date/grain --
  #    cells present in only one crosstab (e.g. each crosstab's own trailing
  #    partial period, or age = "Total" which crosstab 1 does not currently
  #    export) get NA for the other crosstab's columns rather than being
  #    silently dropped.
  # ---------------------------------------------------------------------------
  data_standard <- full_join(hepc_standard, medication_standard, by = index_cols) %>%
    arrange(geography, time, age)

  # ---------------------------------------------------------------------------
  # 6. Write standardized output
  # ---------------------------------------------------------------------------
  if (!dir.exists("standard")) dir.create("standard")
  vroom::vroom_write(data_standard, "standard/data.csv.gz", delim = ",")

  message(
    "Wrote standard/data.csv.gz: ", nrow(data_standard), " rows, ",
    length(unique(data_standard$geography)), " geographies, ",
    length(unique(data_standard$age)), " age groups (",
    paste(unique(data_standard$age), collapse = ", "), "), ",
    min(data_standard$time), " to ", max(data_standard$time)
  )
  for (mc in c(hepc_measure_cols, med_measure_cols)) {
    fc <- paste0(mc, "_suppressed_flag")
    message(
      "  ", mc, ": ", sum(data_standard[[fc]], na.rm = TRUE), " of ",
      sum(!is.na(data_standard[[fc]])), " cells suppressed (",
      round(100 * mean(data_standard[[fc]], na.rm = TRUE), 1), "%)",
      if (mc %in% c(pct_cols, "epic_pct_hepc_medication")) {
        paste0(", ", sum(is.na(data_standard[[mc]])), " left NA")
      } else ""
    )
  }

  # ---------------------------------------------------------------------------
  # 7. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- current_state
  process$medication_raw_state <- current_med_state
  dcf::dcf_process_record(updated = process)
}
