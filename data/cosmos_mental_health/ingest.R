# =============================================================================
# Epic Cosmos Mental Health ED Length of Stay Ingestion
# Source: Epic Cosmos SlicerDicer export (raw/staging/*.xlsx)
#         ED length of stay quartiles and median, and diagnosis share, by mental
#         health ED diagnosis grouper, state of residence, and age band, by
#         calendar quarter of arrival.
#
# Population base: all ED encounters with country of residence = United States.
#
# The export is one flat SlicerDicer session: rows are
#   Start Date x End Date x ED Diagnoses x State of Residence x Age band
# with one column per measure (Q1, median, Q3, Percentage of Sliced Population).
# There are no merged cells. Which slicing columns are filled identifies the block:
#
#   diagnosis  state  age   block
#   ---------  -----  ----  ------------------------------------------------
#   blank      blank  blank national, all ED encounters (all-cause reference)
#   filled     blank  blank national, one diagnosis
#   filled     filled blank state, one diagnosis, all ages
#   filled     filled filled state, one diagnosis, one age band
#
# Things about this source that need care:
#
# 1. The blank-diagnosis row covers every ED encounter, including the
#    non-mental-health diagnoses, so it is emitted as an all-cause reference
#    series (*_all_cause), published nationally for all ages only.
#
# 2. "Percentage of Sliced Population" is the share of the nearest enclosing
#    block, so it means something different in each block:
#      national, one diagnosis : diagnosis share of all ED encounters
#                                -> epic_ed_pct_visits_*   (national only)
#      state, all ages         : state share of the national diagnosis volume
#                                (not published)
#      state, age band         : age band share of the state's diagnosis volume
#                                -> epic_ed_pct_age_*
#    Only values Epic reports are published; nothing is derived from them. The
#    state share of the national diagnosis volume is not published.
#
# 3. Epic omits a suppressed row from the export outright instead of blanking it,
#    so an absent row and a blank cell both mean "not published". These statistics
#    are not counts, so they cannot be imputed: the value is left missing and
#    the suppression flag is set.
#
# 4. The final period of an export is usually a partial quarter (the export date
#    cuts it off). Partial quarters are not comparable and are dropped.
#
# 5. Every session must be filtered to Country of Residence = United States; a
#    Country of Care or Sneak Peek (sample) export is rejected on read.
#
# 6. ED visit counts (epic_ed_n_visits_all_cause) come from a second export, a
#    crosstab (State of Residence x Age at Time of Visit x Month) of the Number of
#    ED Encounters, for all diagnoses. The only step applied is summing the three
#    months of each whole quarter. Counts of "10 or fewer" are imputed as 5 and
#    flagged. The crosstab's age rows use the same bands as the LOS export; its
#    'No value' age rows are disregarded. The age rows do not add up to the
#    state's 'Total' row (the 'Overall' count); both are published as exported.
#    The 0-24 row is the sum of the four exported bands below 25 (0-9 to 20-24).
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

# Recognized ED Diagnoses buckets, keyed by the output column suffix. Values are
# regexes matched against the bucket label in the export. A blank label is the
# all-ED-encounters total and is mapped to all_cause separately.
DX_PATTERNS <- c(
  suicidal_behavior = "^Suicidal behavio",
  mood              = "^Mood",
  behavioral        = "^Behavioral",
  substance_use     = "^Substance use",
  psychosis         = "^Psychosis",
  eating_disorders  = "^Eating disorder",
  other             = "^Other",
  all_cause         = "^Total"
)

# Header row 10 reads
#   Start Date | End Date | <slicing columns, any order> | <measure> | ...
FLAT_DIM_MAP <- c(
  "Slices by ED Diagnoses"                 = "dx_label",
  "Slices by State of Residence"           = "state",
  "Slices by Age at Time of Visit (Years)" = "age"
)
FLAT_MEASURE_PATTERNS <- c(
  q1     = "^Q1\\b",
  q3     = "^Q3\\b",
  median = "^Median",
  pct    = "^Percentage of Sliced Population"
)

# Age bands, mapped to the repository's age labels. "Overall" (all ages) is the
# unstratified row.
AGE_LABELS <- c(
  "Less than 10 Years"                       = "0-9",
  "10 Years or more and less than 15 Years"  = "10-14",
  "15 Years or more and less than 20 Years"  = "15-19",
  "20 Years or more and less than 25 Years"  = "20-24",
  "25 Years or more and less than 45 Years"  = "25-44",
  "45 Years or more and less than 65 Years"  = "45-64",
  "65 Years or more"                         = "65+"
)

INDEX_COLS <- c("geography", "time", "age")

# Published output column families, in output order
FAMILIES <- c(
  "epic_ed_los_median", "epic_ed_los_q1", "epic_ed_los_q3",
  "epic_ed_pct_visits", "epic_ed_pct_age"
)

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
# Place exported .xlsx files from Epic Cosmos SlicerDicer into raw/staging/.
# Superseded exports live in raw/archive/ and are not read.
staging_files <- list.files("raw/staging", pattern = "\\.xlsx$", full.names = TRUE)

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

  # Return one file's cells in long form: one row per (period x diagnosis x
  # state x age band x measure).
  extract_flat_data <- function(grid, file) {
    nm      <- basename(file)
    hdr_row <- which(trimws(grid[[1]]) == "Start Date")[1]
    if (is.na(hdr_row)) stop(nm, " is not a flat SlicerDicer export (no 'Start Date' header).")
    hdr <- trimws(unlist(grid[hdr_row, ]))
    if (!identical(unname(hdr[1:2]), c("Start Date", "End Date"))) {
      stop("Expected 'Start Date', 'End Date' as the first two columns of ", nm)
    }

    # Slicing columns run contiguously from column 3; measures follow them
    j <- 3
    while (j <= length(hdr) && hdr[j] %in% names(FLAT_DIM_MAP)) j <- j + 1
    dim_cols  <- seq_len(j - 1)[-(1:2)]
    meas_cols <- which(hdr != "" & seq_along(hdr) >= j)
    meas      <- match_patterns(hdr[meas_cols], FLAT_MEASURE_PATTERNS)
    if (length(dim_cols) == 0 || anyNA(meas) || anyDuplicated(meas)) {
      stop(
        "Could not classify the columns of ", nm, ": ",
        paste(sQuote(hdr[meas_cols][is.na(meas) | duplicated(meas)]), collapse = ", "),
        "\nExtend FLAT_MEASURE_PATTERNS / FLAT_DIM_MAP as needed."
      )
    }

    # Data rows are those with an ISO start date; blank spacer rows and the
    # "Slices by ..." block labels carry none
    data_rows <- which(grepl("^\\d{4}-\\d{2}-\\d{2}$", trimws(grid[[1]])))
    data_rows <- data_rows[data_rows > hdr_row]
    d     <- grid[data_rows, , drop = FALSE]
    clean <- function(x) na_if(trimws(iconv(x, to = "UTF-8", sub = "")), "")

    keys <- tibble::tibble(
      start    = as.Date(trimws(d[[1]])),
      end      = as.Date(trimws(d[[2]])),
      dx_label = NA_character_,
      state    = NA_character_,
      age      = NA_character_
    )
    for (k in dim_cols) keys[[FLAT_DIM_MAP[[hdr[k]]]]] <- clean(d[[k]])

    message("  ", nrow(keys), " rows | measures: ", paste(meas, collapse = ", "))
    bind_rows(lapply(seq_along(meas_cols), function(k) {
      raw   <- trimws(d[[meas_cols[k]]])
      value <- suppressWarnings(as.numeric(gsub("[%,]", "", raw)))
      mutate(keys, meas = meas[k], value = value)
    }))
  }

  # Crosstab of the visit count by month: rows are State of Residence (plus a
  # 'Total' row for the nation) with a second 'Age at Time of Visit' column
  # holding the age bands and an all-ages 'Total: ...' row per state; columns are
  # Year over Month, the year sitting on the first month of each year. The state
  # is written on the first row of its group only.
  is_state_month_crosstab <- function(grid) {
    top <- trimws(grid[[1]][seq_len(min(30, nrow(grid)))])
    "State of Residence" %in% top && !("Start Date" %in% top)
  }

  extract_state_month_counts <- function(grid, file) {
    nm      <- basename(file)
    lab_row <- which(trimws(grid[[1]]) == "State of Residence")[1]
    measure <- trimws(grid[which(trimws(grid[[1]]) == "Measure")[1], 2])
    if (is.na(measure) || !grepl("^Number of ED Encounters", measure)) {
      stop("Expected the 'Number of ED Encounters' measure in ", nm, ", found: ", measure)
    }
    if (trimws(grid[lab_row, 2]) != "Age at Time of Visit") {
      stop(nm, " must be sliced by Age at Time of Visit as well as State of Residence.")
    }

    years  <- trimws(unlist(grid[lab_row - 2, ]))
    months <- trimws(unlist(grid[lab_row - 1, ]))
    cols   <- which(months != "" & seq_along(months) >= 3)
    for (i in seq_along(years)) if (i > 1 && years[i] == "") years[i] <- years[i - 1]
    start <- as.Date(paste(years[cols], months[cols], "01"), format = "%Y %b %d")
    if (anyNA(start)) stop("Could not parse the Year / Month header of ", nm)

    rows  <- (lab_row + 1):nrow(grid)
    state <- trimws(iconv(grid[[1]][rows], to = "UTF-8", sub = ""))
    age   <- trimws(grid[[2]][rows])
    keep  <- state != "" | age != ""
    rows <- rows[keep]; state <- state[keep]; age <- age[keep]
    for (i in seq_along(state)) if (i > 1 && state[i] == "") state[i] <- state[i - 1]
    age[grepl("^Total", age)] <- ""   # the all-ages row

    raw     <- trimws(unlist(grid[rows, cols, drop = FALSE], use.names = FALSE))
    imputed <- grepl("^10 or fewer$", raw, ignore.case = TRUE)
    value   <- suppressWarnings(as.numeric(gsub(",", "", raw)))
    value[imputed] <- 5

    message("  ", length(rows), " rows x ", length(cols), " months | measure: count")
    tibble::tibble(
      start   = rep(start, each = length(rows)),
      state   = rep(state, times = length(cols)),
      age_raw = na_if(rep(age, times = length(cols)), ""),
      value   = value,
      imputed = imputed
    ) %>%
      mutate(state = if_else(state == "Total", NA_character_, state))
  }

  parsed <- lapply(staging_files, function(f) {
    grid <- read_slicerdicer_grid(f, xlsx_password)
    require_residence(grid, basename(f))
    if (is_state_month_crosstab(grid)) {
      list(type = "counts", data = extract_state_month_counts(grid, f))
    } else {
      list(type = "flat", data = extract_flat_data(grid, f))
    }
  })
  of_type <- function(t) bind_rows(lapply(Filter(function(p) p$type == t, parsed), `[[`, "data"))
  flat_long    <- of_type("flat")
  month_counts <- of_type("counts")
  if (nrow(flat_long) == 0) stop("No length-of-stay export (flat SlicerDicer layout) in raw/staging/.")
  if (nrow(month_counts) == 0) stop("No visit count crosstab (State x Age x Month) in raw/staging/.")

  # ===========================================================================
  # 3. Standardize periods, diagnoses, ages and geographies
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

  # Only whole calendar quarters are comparable; anything else is dropped
  whole_quarter <- flat_long$start == floor_date(flat_long$start, "quarter") &
    flat_long$end == ceiling_date(flat_long$start, "quarter") - days(1)
  if (any(!whole_quarter)) {
    message(
      "Dropping ", sum(!whole_quarter), " cell(s) whose period is not a whole quarter: ",
      paste(unique(paste(flat_long$start, "to", flat_long$end)[!whole_quarter]), collapse = ", ")
    )
  }

  flat_long <- flat_long %>%
    filter(whole_quarter) %>%
    mutate(
      time = format(end, "%Y-%m-%d"),
      dx   = if_else(is.na(dx_label), "all_cause", match_patterns(dx_label, DX_PATTERNS)),
      level = case_when(
        !is.na(age)       ~ "age",
        !is.na(state)     ~ "state",
        dx != "all_cause" ~ "national_dx",
        TRUE              ~ "national_all"
      )
    )

  if (anyNA(flat_long$dx)) {
    stop(
      "Unrecognized ED Diagnoses label(s): ",
      paste(sQuote(unique(flat_long$dx_label[is.na(flat_long$dx)])), collapse = ", "),
      "\nExtend DX_PATTERNS as needed."
    )
  }
  unknown_age <- setdiff(unique(na.omit(flat_long$age)), names(AGE_LABELS))
  if (length(unknown_age) > 0) {
    stop(
      "Unrecognized age band(s): ", paste(sQuote(unknown_age), collapse = ", "),
      "\nExtend AGE_LABELS as needed."
    )
  }

  # Territories, Puerto Rico, Canadian provinces, Mexican states and 'None of
  # the above' have no all_fips name and are dropped
  dropped_geos <- setdiff(unique(na.omit(flat_long$state)), valid_states)
  if (length(dropped_geos) > 0) {
    message("Dropping non-state geograph(ies): ", paste(dropped_geos, collapse = ", "))
  }

  flat_long <- flat_long %>%
    filter(is.na(state) | state %in% valid_states) %>%
    mutate(
      geography_name = coalesce(state, "United States"),
      age_label      = if_else(is.na(age), "Overall", unname(AGE_LABELS[age])),
      # Which column a cell feeds. The state share of the national diagnosis
      # volume (meas == "pct", level == "state") is not published.
      column = case_when(
        meas == "median"                       ~ paste0("epic_ed_los_median_", dx),
        meas == "q1"                           ~ paste0("epic_ed_los_q1_", dx),
        meas == "q3"                           ~ paste0("epic_ed_los_q3_", dx),
        meas == "pct" & level == "national_dx" ~ paste0("epic_ed_pct_visits_", dx),
        meas == "pct" & level == "age"         ~ paste0("epic_ed_pct_age_", dx),
        TRUE                                   ~ NA_character_
      )
    ) %>%
    filter(!is.na(column)) %>%
    left_join(state_fips_lookup, by = "geography_name") %>%
    filter(!is.na(geography))

  # ---------------------------------------------------------------------------
  # ED visit counts: months -> whole quarters
  # ---------------------------------------------------------------------------
  # 'No value' age rows are disregarded
  month_counts <- month_counts[!(month_counts$age_raw %in% "No value"), ]
  unknown_ct_age <- setdiff(unique(na.omit(month_counts$age_raw)), names(AGE_LABELS))
  if (length(unknown_ct_age) > 0) {
    stop("Unrecognized age row(s) in the count crosstab: ",
         paste(sQuote(unknown_ct_age), collapse = ", "), "\nExtend AGE_LABELS as needed.")
  }

  counts_age <- month_counts %>%
    filter(is.na(state) | state %in% valid_states) %>%
    mutate(
      geography_name = coalesce(state, "United States"),
      age    = if_else(is.na(age_raw), "Overall", unname(AGE_LABELS[age_raw])),
      qstart = floor_date(start, "quarter")
    ) %>%
    left_join(state_fips_lookup, by = "geography_name") %>%
    filter(!is.na(geography)) %>%
    group_by(geography, qstart, age) %>%
    summarize(
      n_months = n(),
      flag     = as.integer(any(imputed)),
      value    = if (anyNA(value)) NA_real_ else sum(value),
      .groups  = "drop"
    ) %>%
    filter(n_months == 3, !is.na(value)) %>%     # whole quarters only
    mutate(time = format(ceiling_date(qstart, "quarter") - days(1), "%Y-%m-%d")) %>%
    select(geography, time, age, value, flag)

  # Under 25: the sum of the four exported bands below 25. It is only published
  # where all four are, and is flagged if any was imputed.
  UNDER_25 <- unname(AGE_LABELS[1:4])
  under_25 <- counts_age %>%
    filter(age %in% UNDER_25) %>%
    group_by(geography, time) %>%
    filter(n() == length(UNDER_25)) %>%
    summarize(age = "0-24", value = sum(value), flag = max(flag), .groups = "drop") %>%
    select(geography, time, age, value, flag)
  counts_age <- bind_rows(counts_age, under_25)

  # ===========================================================================
  # 4. Build the wide table
  # ===========================================================================
  # One row per geography x time x age, one column per statistic x diagnosis,
  # each value followed by its suppression flag. The flag is 1 where the
  # statistic is missing for a row it should describe (suppressed, or the row was
  # not returned), 0 where it was published, and NA where the column does not
  # describe that stratum at all.
  wide <- flat_long %>%
    transmute(geography, time, age = age_label, column, value) %>%
    group_by(geography, time, age, column) %>%
    summarize(
      value = if (all(is.na(value))) NA_real_ else value[!is.na(value)][1],
      .groups = "drop"
    ) %>%
    pivot_wider(id_cols = all_of(INDEX_COLS), names_from = column, values_from = value)

  cols <- setdiff(names(wide), INDEX_COLS)

  # A row exists only if the export published something for it
  wide <- wide[rowSums(!is.na(wide[cols])) > 0, ]

  # Is the row a stratum the column describes?
  in_scope <- function(col) {
    switch(
      sub("^(epic_ed_los_median|epic_ed_los_q1|epic_ed_los_q3|epic_ed_pct_visits|epic_ed_pct_age)_.*$", "\\1", col),
      # National all ages only: Epic reports no state-level case mix and no national age bands
      # (the cells at state level hold other shares, which are not published)
      epic_ed_pct_visits = wide$geography == "00" & wide$age == "Overall",
      epic_ed_pct_age    = wide$geography != "00" & wide$age != "Overall",
      # Length of stay: all-cause is published nationally for all ages only
      if (grepl("_all_cause$", col)) {
        wide$geography == "00" & wide$age == "Overall"
      } else {
        rep(TRUE, nrow(wide))
      }
    )
  }
  for (col in cols) {
    wide[[paste0(col, "_suppressed_flag")]] <- if_else(
      in_scope(col), as.integer(is.na(wide[[col]])), NA_integer_
    )
  }

  # All-ED-encounter visit counts, as exported. Unlike the length-of-stay
  # statistics a suppressed count is imputed (as 5) rather than left missing, so
  # its flag may be 1 on an observed value. Quarters before the LOS export
  # begins get a row with only the count.
  counts_out <- counts_age %>%
    transmute(
      geography, time, age,
      epic_ed_n_visits_all_cause = value,
      epic_ed_n_visits_all_cause_suppressed_flag = flag
    )
  wide <- full_join(wide, counts_out, by = INDEX_COLS)

  # Order: index, each family's diagnoses in a stable order with each value
  # followed by its flag
  ordered <- intersect(
    as.vector(outer(names(DX_PATTERNS), FAMILIES, function(dx, fam) paste0(fam, "_", dx))),
    names(wide)
  )
  data_quarterly <- wide %>%
    select(all_of(c(
      INDEX_COLS,
      as.vector(rbind(ordered, paste0(ordered, "_suppressed_flag"))),
      "epic_ed_n_visits_all_cause", "epic_ed_n_visits_all_cause_suppressed_flag"
    ))) %>%
    arrange(across(all_of(INDEX_COLS)))

  # ===========================================================================
  # 5. Validate
  # ===========================================================================
  dupes <- data_quarterly %>% count(across(all_of(INDEX_COLS))) %>% filter(n > 1)
  if (nrow(dupes) > 0) {
    stop("Duplicate ", paste(INDEX_COLS, collapse = "/"), " rows: ", nrow(dupes))
  }

  # A value must carry flag 0 and a missing value must carry flag 1 or NA
  measure_cols <- setdiff(
    grep("^epic_", names(data_quarterly), value = TRUE),
    grep("_suppressed_flag$", names(data_quarterly), value = TRUE)
  )
  for (mc in setdiff(measure_cols, "epic_ed_n_visits_all_cause")) {
    v <- data_quarterly[[mc]]
    f <- data_quarterly[[paste0(mc, "_suppressed_flag")]]
    if (any(!is.na(v) & !(f %in% 0L)) || any(is.na(v) & f %in% 0L)) {
      stop("Value and suppression flag disagree in ", mc)
    }
  }

  # Quartile ordering over cells where all three statistics are published
  ord_total <- 0L
  ord_viol  <- 0L
  for (dxs in names(DX_PATTERNS)) {
    qcols <- paste0("epic_ed_los_", c("q1", "median", "q3"), "_", dxs)
    if (!all(qcols %in% names(data_quarterly))) next
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

  pct_cols <- grep("^epic_ed_pct_(visits|age)_[a-z_]+$", measure_cols, value = TRUE)
  pct_vals <- unlist(data_quarterly[, pct_cols])
  if (any(pct_vals < 0 | pct_vals > 100, na.rm = TRUE)) {
    stop("Percentages outside [0, 100]")
  }

  # The age bands of one state-quarter-diagnosis are shares of the same total
  # (every age), so they cannot sum past 100 beyond rounding
  pa_cols <- grep("^epic_ed_pct_age_[a-z_]+$", measure_cols, value = TRUE)
  age_sums <- data_quarterly %>%
    filter(age != "Overall", geography != "00") %>%
    group_by(geography, time) %>%
    summarize(across(all_of(pa_cols), ~ sum(.x, na.rm = TRUE)), .groups = "drop")
  max_sum <- max(unlist(age_sums[, pa_cols]))
  message("Age-share check: largest sum of age bands within a state-quarter-diagnosis is ",
          round(max_sum, 1), "%")
  if (max_sum > 101) stop("Age shares sum past 100 within a state-quarter-diagnosis.")

  # Visit counts: the national count should be close to the sum of the states
  # (territories and unknown state of residence are in the national figure only)
  nv <- data_quarterly %>% filter(!is.na(epic_ed_n_visits_all_cause))
  ratio <- nv %>% filter(age == "Overall") %>% group_by(time) %>%
    summarize(r = epic_ed_n_visits_all_cause[geography == "00"] /
                sum(epic_ed_n_visits_all_cause[geography != "00"]), .groups = "drop")
  message("ED visits, national / sum of states: median ", round(median(ratio$r), 3),
          ", range ", paste(round(range(ratio$r), 3), collapse = "-"))
  if (any(ratio$r < 0.9 | ratio$r > 1.2)) stop("National ED visit count is far from the sum of the states.")

  if (nrow(filter(data_quarterly, epic_ed_n_visits_all_cause < 0)) > 0) stop("Negative visit count.")
  # The age rows of the crosstab do not cover every visit (note 6), so the bands
  # add up to less than the all-ages count, never more
  band_ratio <- nv %>% filter(geography != "00") %>%
    group_by(geography, time) %>%
    summarize(r = sum(epic_ed_n_visits_all_cause[!age %in% c("Overall", "0-24")]) /
                epic_ed_n_visits_all_cause[age == "Overall"], .groups = "drop")
  message("Age-band visits / state all-ages visits: median ", round(median(band_ratio$r), 3),
          ", range ", paste(round(range(band_ratio$r), 3), collapse = "-"))
  if (any(band_ratio$r > 1.001)) stop("State age-band visits exceed the state's all-ages count.")

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
