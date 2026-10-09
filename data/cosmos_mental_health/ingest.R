# =============================================================================
# Epic Cosmos Mental Health ED Length of Stay Ingestion
# Source: Epic Cosmos SlicerDicer crosstab (raw/staging/*.xlsx), session 2865212
#         ED length of stay (Q1, median, Q3) and number of ED encounters, for
#         suicidal behavior, other ED diagnoses, and all ED diagnoses, by state
#         of residence and age band, by calendar quarter of arrival.
#
# Population base: all ED encounters with country of residence = United States.
#
# Layout (one two-level crosstab, no flat blocks):
#   rows    Year > Quarter > Age at Time of Visit > State of Residence
#           (the outer labels are written on the first row of each group only)
#   columns ED Diagnoses group (written on the first column of the group) over
#           the measures Q3 / Q1 / Median length of stay and Number of ED
#           Encounters. The last group, "Total", is every ED diagnosis.
#   A 'Total' age row is the all-ages stratum; a 'Total' state row is the nation.
#
# Things about this source that need care:
#
# 1. Epic suppresses a cell built on 10 or fewer encounters. The count shows
#    "10 or fewer" and the length-of-stay statistics are blank. Counts are
#    imputed as 5 and flagged; length-of-stay statistics are not counts, so they
#    stay missing and are flagged.
#
# 2. The "Less than 25 Years" band overlaps the narrower bands below it, and the
#    age bands do not cover every encounter (some have no recorded age), so the
#    bands do not add up to the all-ages row. Both are published as exported.
#
# 3. Only whole calendar quarters are comparable. The ingest stops on a period
#    that is not one.
#
# 4. Every session must be filtered to Country of Residence = United States; a
#    Country of Care or Sneak Peek (sample) export is rejected on read.
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

# --- Label maps: the contract with the export --------------------------------
# Row dimensions, as labeled in the header row
DIM_LABELS <- c(
  "Year"                 = "year",
  "Quarter"              = "quarter",
  "Age at Time of Visit" = "age",
  "State of Residence"   = "state"
)

# ED Diagnoses groups, keyed by the output column suffix. Regexes match the
# group label in the export; "Total" is every ED diagnosis.
DX_PATTERNS <- c(
  suicidal_behavior = "^Suicidal behavio",
  other             = "^Other",
  all               = "^Total"
)

# Measures, keyed by the output column prefix. The all-diagnoses visit count is
# published as plain epic_ed_n_visits.
MEASURE_PATTERNS <- c(
  epic_ed_los_q1     = "^Q1\\b",
  epic_ed_los_q3     = "^Q3\\b",
  epic_ed_los_median = "^Median",
  epic_ed_n_visits   = "^Number of ED Encounters"
)

# Age bands, mapped to the repository's age labels. Epic has written the bands
# both as ">= 10 and < 15 Years" and as "10 Years or more and less than 15
# Years"; both match. Regexes avoid the non-ASCII comparison signs.
AGE_PATTERNS <- c(
  "0-9"   = "^Less than 10 Years",
  "10-14" = "^(\\S+ 10 and \\S+ 15|10 Years or more and less than 15) Years",
  "15-19" = "^(\\S+ 15 and \\S+ 20|15 Years or more and less than 20) Years",
  "20-24" = "^(\\S+ 20 and \\S+ 25|20 Years or more and less than 25) Years",
  "0-24"  = "^Less than 25 Years",
  "25-44" = "^(\\S+ 25 and \\S+ 45|25 Years or more and less than 45) Years",
  "45-64" = "^(\\S+ 45 and \\S+ 65|45 Years or more and less than 65) Years",
  "65+"   = "^65 Years or more",
  "Overall" = "^Total"
)

INDEX_COLS <- c("geography", "time", "age")

# Match each label against a set of regexes; NA unless exactly one matches
match_patterns <- function(labels, patterns) {
  vapply(labels, function(lbl) {
    hit <- names(patterns)[vapply(patterns, function(p) grepl(p, lbl), logical(1))]
    if (length(hit) != 1) NA_character_ else hit
  }, character(1), USE.NAMES = FALSE)
}

# =============================================================================
# 1. Locate raw files
# =============================================================================
# Place the exported .xlsx from Epic Cosmos SlicerDicer in raw/staging/.
# Superseded exports live in raw/archive/ and are not read.
staging_files <- list.files("raw/staging", pattern = "\\.xlsx$", full.names = TRUE)

if (length(staging_files) == 0) {
  stop(
    "No staging files found in raw/staging/.\n",
    "Export data from Epic Cosmos SlicerDicer and place the .xlsx file there."
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

  # Decrypt a password-protected SlicerDicer xlsx and return the sheet as an
  # all-character grid with no header interpretation.
  read_slicerdicer_grid <- function(file, password = NULL) {
    message("Reading: ", basename(file))

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

    as.data.frame(
      lapply(grid, function(x) { x <- as.character(x); x[is.na(x)] <- ""; x }),
      stringsAsFactors = FALSE
    )
  }

  # Every session must be filtered to Country of Residence, from the criteria
  # line of the metadata header
  require_residence <- function(grid, nm) {
    head_cells <- unlist(grid[seq_len(min(12, nrow(grid))), ])
    crit <- grid[[2]][which(grepl("^Population Criteria", trimws(grid[[1]][seq_len(12)])))[1]]
    if (length(crit) == 1 && !is.na(crit) && grepl("Country of Care", crit)) {
      stop(nm, " is filtered on Country of Care; re-export it filtered on Country of Residence.")
    }
    if (any(grepl("Sneak Peek", head_cells))) {
      stop(nm, " is a Sneak Peek sample export; re-export it on the full data.")
    }
    invisible(TRUE)
  }

  # Return one crosstab in long form: one row per (quarter x age x state x
  # diagnosis group x measure).
  extract_crosstab <- function(grid, file) {
    nm      <- basename(file)
    hdr_row <- which(trimws(grid[[1]]) == "Year")[1]
    if (is.na(hdr_row) || hdr_row < 3) stop(nm, " has no 'Year' header row.")
    hdr <- trimws(unlist(grid[hdr_row, ]))

    # Row dimensions run contiguously from column 1; values follow them
    dim_cols <- which(hdr %in% names(DIM_LABELS))
    if (!identical(dim_cols, seq_along(dim_cols)) || length(dim_cols) != length(DIM_LABELS) ||
        !setequal(hdr[dim_cols], names(DIM_LABELS))) {
      stop(
        "Unexpected row dimensions in ", nm, ": ", paste(sQuote(hdr[hdr != ""]), collapse = ", "),
        "\nExtend DIM_LABELS as needed."
      )
    }
    val_cols <- seq_along(hdr)[-dim_cols]

    # The diagnosis group sits two rows above the header, the measure one row
    # above; the group label is written on its first column only
    group   <- trimws(unlist(grid[hdr_row - 2, ]))
    measure <- trimws(unlist(grid[hdr_row - 1, ]))
    for (i in val_cols) if (group[i] == "" && i > min(val_cols)) group[i] <- group[i - 1]
    val_cols <- val_cols[measure[val_cols] != ""]

    dx   <- match_patterns(group[val_cols],   DX_PATTERNS)
    meas <- match_patterns(measure[val_cols], MEASURE_PATTERNS)
    if (anyNA(dx) || anyNA(meas) || anyDuplicated(paste(dx, meas))) {
      stop(
        "Could not classify the columns of ", nm, ": ",
        paste(sQuote(paste(group[val_cols], "/", measure[val_cols])[is.na(dx) | is.na(meas) | duplicated(paste(dx, meas))]), collapse = "; "),
        "\nExtend DX_PATTERNS / MEASURE_PATTERNS as needed."
      )
    }

    rows <- (hdr_row + 1):nrow(grid)
    clean <- function(x) na_if(trimws(iconv(x, to = "UTF-8", sub = "")), "")
    keys <- tibble::tibble(.rows = length(rows))
    for (k in dim_cols) keys[[DIM_LABELS[[hdr[k]]]]] <- clean(grid[[k]][rows])

    # Blank rows below the table carry no state; merged cells are filled down
    keep <- !is.na(keys$state)
    keys <- keys[keep, ]
    raw  <- grid[rows[keep], val_cols, drop = FALSE]
    keys <- tidyr::fill(keys, year, quarter, age, .direction = "down")

    message("  ", nrow(keys), " rows x ", length(val_cols), " value columns | groups: ",
            paste(unique(dx), collapse = ", "))
    bind_rows(lapply(seq_along(val_cols), function(k) {
      mutate(keys, dx = dx[k], meas = meas[k], raw = trimws(raw[[k]]))
    }))
  }

  long <- bind_rows(lapply(staging_files, function(f) {
    grid <- read_slicerdicer_grid(f, xlsx_password)
    require_residence(grid, basename(f))
    extract_crosstab(grid, f)
  }))

  # ===========================================================================
  # 3. Standardize periods, ages and geographies
  # ===========================================================================
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

  # 2-digit FIPS rows include the national entry ("00", "United States")
  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    select(geography, geography_name) %>%
    filter(!is.na(geography_name)) %>%   # territories have no name in all_fips
    distinct()
  stopifnot(!anyDuplicated(state_fips_lookup$geography_name))
  valid_states <- c(state.name, "District of Columbia")

  # Quarter label ("Jul 1 - Sep 30", any dash) -> quarter end date. Only whole
  # calendar quarters are accepted.
  quarter_ends <- c("Jan 1" = "Mar 31", "Apr 1" = "Jun 30", "Jul 1" = "Sep 30", "Oct 1" = "Dec 31")
  q_start <- sub("^(\\w{3} \\d+).*$", "\\1", long$quarter)
  q_end   <- sub("^.*?(\\w{3} \\d+)$", "\\1", long$quarter)
  bad_q   <- !(q_start %in% names(quarter_ends)) | unname(quarter_ends[q_start]) != q_end
  if (any(bad_q, na.rm = TRUE) || anyNA(bad_q)) {
    stop(
      "Period(s) that are not whole calendar quarters: ",
      paste(sQuote(unique(long$quarter[bad_q | is.na(bad_q)])), collapse = ", ")
    )
  }
  long$time <- format(
    as.Date(paste(long$year, q_end), format = "%Y %b %d"), "%Y-%m-%d"
  )
  if (anyNA(long$time)) stop("Could not parse the Year / Quarter labels.")

  long$age_label <- match_patterns(long$age, AGE_PATTERNS)
  if (anyNA(long$age_label)) {
    stop(
      "Unrecognized age band(s): ", paste(sQuote(unique(long$age[is.na(long$age_label)])), collapse = ", "),
      "\nExtend AGE_PATTERNS as needed."
    )
  }

  # Territories, Puerto Rico, Canadian provinces, Mexican states, 'Armed Forces'
  # and 'None of the above' have no all_fips name and are dropped
  dropped_geos <- setdiff(unique(long$state), c(valid_states, "Total"))
  if (length(dropped_geos) > 0) {
    message("Dropping non-state geograph(ies): ", paste(sort(dropped_geos), collapse = ", "))
  }

  long <- long %>%
    filter(state %in% c(valid_states, "Total")) %>%
    mutate(geography_name = if_else(state == "Total", "United States", state)) %>%
    left_join(state_fips_lookup, by = "geography_name")
  stopifnot(!anyNA(long$geography))

  # Suppression: a count of "10 or fewer" (or a blank) is imputed as 5; the
  # length-of-stay statistics are not counts and stay missing. Either way the
  # flag records what Epic withheld, computed before imputation.
  is_count <- long$meas == "epic_ed_n_visits"
  value    <- suppressWarnings(as.numeric(gsub(",", "", long$raw)))
  unparsed <- !is.na(value) | long$raw %in% c("", "-", "10 or fewer")
  if (any(!unparsed)) {
    stop("Unrecognized cell value(s): ", paste(sQuote(unique(long$raw[!unparsed])), collapse = ", "))
  }
  long$flag  <- as.integer(is.na(value))
  long$value <- if_else(is_count & is.na(value), 5, value)
  # The all-diagnoses visit count is plain epic_ed_n_visits
  long$column <- if_else(
    is_count & long$dx == "all", long$meas, paste0(long$meas, "_", long$dx)
  )

  # ===========================================================================
  # 4. Build the wide table
  # ===========================================================================
  value_wide <- long %>%
    transmute(geography, time, age = age_label, column, value) %>%
    pivot_wider(id_cols = all_of(INDEX_COLS), names_from = column, values_from = value)
  flag_wide <- long %>%
    transmute(geography, time, age = age_label, column = paste0(column, "_suppressed_flag"), flag) %>%
    pivot_wider(id_cols = all_of(INDEX_COLS), names_from = column, values_from = flag)
  wide <- left_join(value_wide, flag_wide, by = INDEX_COLS)

  # Order: index, then each measure family across diagnosis groups, each value
  # followed by its flag
  dx_order <- c("all", "suicidal_behavior", "other")
  ordered <- unlist(lapply(
    c("epic_ed_los_median", "epic_ed_los_q1", "epic_ed_los_q3", "epic_ed_n_visits"),
    function(m) ifelse(dx_order == "all" & m == "epic_ed_n_visits", m, paste0(m, "_", dx_order))
  ))
  stopifnot(setequal(ordered, setdiff(names(value_wide), INDEX_COLS)))
  data_quarterly <- wide %>%
    select(all_of(c(INDEX_COLS, as.vector(rbind(ordered, paste0(ordered, "_suppressed_flag")))))) %>%
    arrange(across(all_of(INDEX_COLS)))

  # ===========================================================================
  # 5. Validate
  # ===========================================================================
  dupes <- data_quarterly %>% count(across(all_of(INDEX_COLS))) %>% filter(n > 1)
  if (nrow(dupes) > 0) {
    stop("Duplicate ", paste(INDEX_COLS, collapse = "/"), " rows: ", nrow(dupes))
  }

  # A length-of-stay value must carry flag 0 and a missing one flag 1. A count is
  # always present (imputed as 5 when suppressed).
  for (mc in ordered) {
    v <- data_quarterly[[mc]]
    f <- data_quarterly[[paste0(mc, "_suppressed_flag")]]
    if (any(is.na(f))) stop("Missing suppression flag in ", mc)
    if (grepl("^epic_ed_n_visits", mc)) {
      if (anyNA(v) || any(f == 1L & v != 5)) {
        stop("Count and suppression flag disagree in ", mc)
      }
    } else if (any(!is.na(v) & f != 0L) || any(is.na(v) & f != 1L)) {
      stop("Value and suppression flag disagree in ", mc)
    }
    message(mc, ": ", sum(f == 1L), " suppressed cell(s) of ", length(f))
  }

  # Quartile ordering over cells where all three statistics are published
  ord_total <- 0L
  ord_viol  <- 0L
  for (dxs in dx_order) {
    qcols <- paste0("epic_ed_los_", c("q1", "median", "q3"), "_", dxs)
    tri <- data_quarterly[, qcols]
    ok  <- stats::complete.cases(tri)
    ord_total <- ord_total + sum(ok)
    ord_viol  <- ord_viol + sum(tri[ok, 1] > tri[ok, 2] | tri[ok, 2] > tri[ok, 3])
  }
  message(
    "Quartile check: ", ord_viol, " of ", ord_total,
    " triples violate Q1 <= median <= Q3 (", sprintf("%.2f%%", 100 * ord_viol / ord_total), ")"
  )
  if (ord_viol / ord_total > 0.01) {
    stop("Q1 <= median <= Q3 fails in more than 1% of cells; check the measure mapping.")
  }

  if (any(unlist(data_quarterly[, grep("^epic_ed_los", ordered, value = TRUE)]) <= 0, na.rm = TRUE)) {
    stop("Non-positive length of stay.")
  }

  # The diagnosis groups are subsets of all ED encounters; imputed 5s can push a
  # small cell over, so only observed cells are compared
  for (dxs in c("suicidal_behavior", "other")) {
    n_dx <- data_quarterly[[paste0("epic_ed_n_visits_", dxs)]]
    f_dx <- data_quarterly[[paste0("epic_ed_n_visits_", dxs, "_suppressed_flag")]]
    if (any(f_dx == 0L & n_dx > data_quarterly$epic_ed_n_visits &
            data_quarterly$epic_ed_n_visits_suppressed_flag == 0L)) {
      stop("A diagnosis group exceeds all ED encounters (", dxs, ").")
    }
  }

  # The national count should be close to the sum of the states (territories and
  # unknown state of residence are in the national figure only)
  ratio <- data_quarterly %>% filter(age == "Overall") %>% group_by(time) %>%
    summarize(r = epic_ed_n_visits[geography == "00"] /
                sum(epic_ed_n_visits[geography != "00"]), .groups = "drop")
  message("ED visits, national / sum of states: median ", round(median(ratio$r), 3),
          ", range ", paste(round(range(ratio$r), 3), collapse = "-"))
  if (any(ratio$r < 0.9 | ratio$r > 1.2)) stop("National ED visit count is far from the sum of the states.")

  message(
    "Quarterly: ", nrow(data_quarterly), " rows x ", ncol(data_quarterly), " cols | ",
    length(unique(data_quarterly$geography)), " geographies | ages: ",
    paste(sort(unique(data_quarterly$age)), collapse = ", "), " | ",
    min(data_quarterly$time), " to ", max(data_quarterly$time)
  )

  # ===========================================================================
  # 6. Write standardized output
  # ===========================================================================
  if (!dir.exists("standard")) dir.create("standard")

  vroom::vroom_write(data_quarterly, "standard/data.csv.gz", delim = ",")

  # ===========================================================================
  # 7. Record processed state
  # ===========================================================================
  process$raw_state <- current_state
  dcf::dcf_process_record(updated = process)
}
