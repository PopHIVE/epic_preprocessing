# =============================================================================
# Epic Cosmos Mental Health ED Length of Stay Ingestion
# Source: Epic Cosmos SlicerDicer exports (raw/staging/*.xlsx)
#         ED length of stay quartiles and median, diagnosis case mix, and ED
#         visit counts, by mental health ED diagnosis grouper, state of
#         residence, and age band.
#
# Population base: all ED encounters with country of residence = United States.
#
# Every export is a flat SlicerDicer session: rows are
#   Start Date x [ED Diagnoses] x [State of Residence] x [Age band]
# laid out in blocks, one column per measure. There are no merged cells. Each
# file is one diagnosis and measure set, and overlapping files must agree. The
# dimensions may appear in a different order (or be absent) in different files,
# so they are read from each file's own header.
#
# Four things about this source need care:
#
# 1. The diagnosis grouper's "Total" bucket (blank diagnosis in the export)
#    covers every ED encounter, including the non-mental-health diagnoses, so it
#    is emitted as an all-cause reference series (*_all_cause), published
#    nationally for all ages only.
#
# 2. The "Percentage of Sliced Population" field is the share of the nearest
#    enclosing block, so it means something different in each block:
#      national, one diagnosis : diagnosis share of all ED encounters   (nat)
#      state, all ages         : state share of the national diagnosis volume (r)
#      state, age band         : age band share of the state's diagnosis volume (pa)
#    None is a case mix at state level, so the state-level case mix
#    (epic_ed_pct_visits_*) is derived from them and from visit counts (section 5).
#
# 3. Epic omits a suppressed row from these exports outright instead of blanking
#    it, so an absent row and a blank cell both mean "not published". Visit
#    counts of 10 or fewer are published as the text "10 or fewer" and are
#    imputed as 5 with the suppression flag set.
#
# 4. Visit counts come from one crosstab (State of Residence x Age at Time of
#    Visit x Month): the age rows are the age-band denominators, the per-state
#    'Total' row the all-ages count, and the 'Total' state the national figures.
#    Every session must be filtered to Country of Residence = United States; a
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
MH_DX <- setdiff(names(DX_PATTERNS), "all_cause")

# Header row 10 reads
#   Start Date | End Date | <slicing columns, any order> | <measure> | ...
# Slicing columns are recognized by label and mapped to an internal name.
FLAT_DIM_MAP <- c(
  "Slices by ED Diagnoses"                 = "dx_label",
  "Slices by State of Residence"           = "state",
  "Slices by Age at Time of Visit (Years)" = "age"
)
FLAT_MEASURE_PATTERNS <- c(
  q1     = "^Q1\\b",
  q3     = "^Q3\\b",
  median = "^Median",
  pct    = "^Percentage of Sliced Population",
  count  = "^Number of ED Encounters"
)

# Age bands, mapped to the repository's age labels. The exports stop at age 25;
# "Overall" (all ages) is the unstratified row.
AGE_LABELS <- c(
  "Less than 5 Years"                        = "0-4",
  "5 Years or more and less than 10 Years"   = "5-9",
  "10 Years or more and less than 15 Years"  = "10-14",
  "15 Years or more and less than 20 Years"  = "15-19",
  "20 Years or more and less than 25 Years"  = "20-24"
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

  # Return one file's cells in long form: one row per (period x diagnosis x
  # state x age band x measure). Which slicing columns are filled identifies the
  # block:
  #
  #   diagnosis  state  age   block
  #   ---------  -----  ----  ------------------------------------------------
  #   blank      blank  blank national, all ED encounters (all-cause)
  #   filled     blank  blank national, one diagnosis
  #   filled     filled blank state, one diagnosis, all ages
  #   filled     filled filled state, one diagnosis, one age band
  #   (a file with no diagnosis column is all-cause throughout, and may also
  #    carry national-by-age rows: blank state, filled age)
  extract_flat_data <- function(grid, file) {
    nm      <- basename(file)
    hdr_row <- which(trimws(grid[[1]]) == "Start Date")[1]
    if (is.na(hdr_row)) {
      stop(
        nm, " is not a flat SlicerDicer export (no 'Start Date' header). ",
        "The older state x year x month crosstab layout is no longer read."
      )
    }
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
    out <- bind_rows(lapply(seq_along(meas_cols), function(k) {
      raw     <- trimws(d[[meas_cols[k]]])
      imputed <- grepl("^10 or fewer$", raw, ignore.case = TRUE)
      value   <- suppressWarnings(as.numeric(gsub("[%,]", "", raw)))
      # A suppressed count is imputed as 5; other statistics cannot be imputed
      if (meas[k] == "count") value[imputed] <- 5
      mutate(keys, meas = meas[k], value = value, imputed = imputed & meas[k] == "count")
    }))
    out
  }

  # The population filter the session was run with (Country of Residence or of
  # Care), from the criteria line of the metadata header
  population_filter <- function(grid) {
    hdr <- trimws(grid[[1]][seq_len(min(12, nrow(grid)))])
    row <- which(grepl("^Population Criteria", hdr))[1]
    if (is.na(row)) return(NA_character_)
    grid[row, 2]
  }

  # Every session must be filtered to Country of Residence. A Country of Care
  # session counts non-US residents treated in the US (nationally ~1.3% more
  # visits) and disagrees with the residence sessions in small state cells, so it
  # is rejected rather than mixed in.
  require_residence <- function(grid, nm) {
    crit <- population_filter(grid)
    if (!is.na(crit) && grepl("Country of Care", crit)) {
      stop(nm, " is filtered on Country of Care; re-export it filtered on Country of Residence.")
    }
    if (any(grepl("Sneak Peek", unlist(grid[seq_len(min(12, nrow(grid))), ])))) {
      stop(nm, " is a Sneak Peek sample export; re-export it on the full data.")
    }
    invisible(TRUE)
  }

  # Crosstab of one measure by month: rows are State of Residence (plus a
  # 'Total' row for the nation), optionally with a second 'Age at Time of Visit'
  # column holding the age bands and an all-ages 'Total: ...' row per state;
  # columns are Year over Month. The year sits on the first month of each year.
  # The state is written on the first row of its group only.
  is_state_month_crosstab <- function(grid) {
    top <- trimws(grid[[1]][seq_len(min(30, nrow(grid)))])
    "State of Residence" %in% top && !("Start Date" %in% top)
  }

  extract_state_month_counts <- function(grid, file) {
    nm      <- basename(file)
    lab_row <- which(trimws(grid[[1]]) == "State of Residence")[1]
    measure <- trimws(grid[which(trimws(grid[[1]]) == "Measure")[1], 2])
    if (is.na(measure) || !grepl(FLAT_MEASURE_PATTERNS[["count"]], measure)) {
      stop("Expected the 'Number of ED Encounters' measure in ", nm, ", found: ", measure)
    }
    has_age   <- trimws(grid[lab_row, 2]) == "Age at Time of Visit"
    first_col <- if (has_age) 3 else 2

    years  <- trimws(unlist(grid[lab_row - 2, ]))
    months <- trimws(unlist(grid[lab_row - 1, ]))
    cols   <- which(months != "" & seq_along(months) >= first_col)
    for (i in seq_along(years)) if (i > 1 && years[i] == "") years[i] <- years[i - 1]

    start <- as.Date(paste(years[cols], months[cols], "01"), format = "%Y %b %d")
    if (anyNA(start)) stop("Could not parse the Year / Month header of ", nm)

    # Row labels: carry the state down over its age rows; an age row labelled
    # 'Total: ...' is the all-ages count
    rows  <- (lab_row + 1):nrow(grid)
    state <- trimws(iconv(grid[[1]][rows], to = "UTF-8", sub = ""))
    age   <- if (has_age) trimws(grid[[2]][rows]) else rep("", length(rows))
    keep  <- state != "" | age != ""
    rows <- rows[keep]; state <- state[keep]; age <- age[keep]
    for (i in seq_along(state)) if (i > 1 && state[i] == "") state[i] <- state[i - 1]
    age[grepl("^Total", age)] <- ""

    raw     <- trimws(unlist(grid[rows, cols, drop = FALSE], use.names = FALSE))
    imputed <- grepl("^10 or fewer$", raw, ignore.case = TRUE)
    value   <- suppressWarnings(as.numeric(gsub(",", "", raw)))
    value[imputed] <- 5

    message("  ", length(rows), " rows x ", length(cols), " months | measure: count",
            if (has_age) " | by age band" else "")
    start_all <- rep(start, each = length(rows))
    out <- tibble::tibble(
      start    = start_all,
      end      = ceiling_date(start_all, "month") - days(1),
      dx_label = NA_character_,
      state    = rep(state, times = length(cols)),
      age      = na_if(rep(age, times = length(cols)), ""),
      meas     = "count",
      value    = value,
      imputed  = imputed
    ) %>%
      mutate(state = if_else(state == "Total", NA_character_, state))
    out
  }

  flat_long <- bind_rows(lapply(staging_files, function(f) {
    grid <- read_slicerdicer_grid(f, xlsx_password)
    require_residence(grid, basename(f))
    if (is_state_month_crosstab(grid)) extract_state_month_counts(grid, f) else extract_flat_data(grid, f)
  }))

  # ===========================================================================
  # 3. Standardize periods, diagnoses, ages and geographies
  # ===========================================================================
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    select(geography, geography_name)
  valid_states <- c(state.name, "District of Columbia")

  # Only whole calendar months are comparable; anything else is dropped
  whole_month <- flat_long$start == floor_date(flat_long$start, "month") &
    flat_long$end == ceiling_date(flat_long$start, "month") - days(1)
  if (any(!whole_month)) {
    message("Dropping ", sum(!whole_month), " cell(s) whose period is not a whole month")
  }

  flat_long <- flat_long %>%
    filter(whole_month) %>%
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
      # Which column a cell feeds. state_share_* is an intermediate for the
      # state case mix (section 5) and is not published.
      column = case_when(
        meas == "median"                       ~ paste0("epic_ed_los_median_", dx),
        meas == "q1"                           ~ paste0("epic_ed_los_q1_", dx),
        meas == "q3"                           ~ paste0("epic_ed_los_q3_", dx),
        meas == "count"                        ~ "epic_ed_n_visits",
        meas == "pct" & level == "national_dx" ~ paste0("epic_ed_pct_visits_", dx),
        meas == "pct" & level == "state"       ~ paste0("state_share_", dx),
        meas == "pct" & level == "age"         ~ paste0("epic_ed_pct_age_", dx),
        TRUE                                   ~ NA_character_
      )
    ) %>%
    filter(!is.na(column)) %>%
    left_join(
      bind_rows(
        state_fips_lookup,
        tibble::tibble(geography = "00", geography_name = "United States")
      ),
      by = "geography_name"
    ) %>%
    filter(!is.na(geography))

  # Overlapping sessions (e.g. 'psychosis all' and 'psychosis med pct') must
  # agree on every shared cell. One blank copy yields to a published copy.
  conflicts <- flat_long %>%
    group_by(geography, time, age_label, column) %>%
    summarize(n_values = n_distinct(value, na.rm = TRUE), .groups = "drop") %>%
    filter(n_values > 1)
  if (nrow(conflicts) > 0) {
    stop(
      nrow(conflicts), " cell(s) differ between overlapping exports, e.g. ",
      paste(head(paste(conflicts$geography, conflicts$time, conflicts$age_label,
                       conflicts$column), 3), collapse = "; ")
    )
  }

  # ===========================================================================
  # 4. Build the wide table
  # ===========================================================================
  # One row per geography x time x age, one column per statistic x diagnosis,
  # each value followed by its suppression flag. The flag is 1 where the
  # statistic is missing inside the months its exports cover (suppressed, or the
  # row was not returned), 0 where it was published, and NA where the exports do
  # not report that statistic for that row at all (outside their span, or not a
  # stratum the column describes). These statistics are not counts, so a
  # suppressed value cannot be imputed: the value is left missing.
  build_wide <- function(cells) {
    # The span each column's exports cover, taken before blank cells are dropped
    cover <- cells %>%
      group_by(column) %>%
      summarize(t_min = min(time), t_max = max(time), .groups = "drop")

    wide <- cells %>%
      transmute(geography, time, age = age_label, column, value) %>%
      group_by(geography, time, age, column) %>%
      summarize(
        value = if (all(is.na(value))) NA_real_ else value[!is.na(value)][1],
        .groups = "drop"
      ) %>%
      pivot_wider(id_cols = all_of(INDEX_COLS), names_from = column, values_from = value)

    cols <- setdiff(names(wide), INDEX_COLS)

    # A row exists only if the exports published something for it
    wide <- wide[rowSums(!is.na(wide[cols])) > 0, ]

    # Is the row a stratum the column describes?
    in_scope <- function(col) {
      switch(
        sub("^(epic_ed_los_median|epic_ed_los_q1|epic_ed_los_q3|epic_ed_pct_visits|epic_ed_pct_age|state_share)_.*$", "\\1", col),
        epic_ed_pct_visits = wide$geography == "00" & wide$age == "Overall",
        state_share        = wide$geography != "00" & wide$age == "Overall",
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
      cv <- cover[cover$column == col, ]
      covered <- in_scope(col) & wide$time >= cv$t_min & wide$time <= cv$t_max
      wide[[paste0(col, "_suppressed_flag")]] <- if_else(
        covered, as.integer(is.na(wide[[col]])), NA_integer_
      )
    }
    attr(wide, "cover") <- cover
    wide
  }

  data_monthly <- build_wide(filter(flat_long, column != "epic_ed_n_visits"))
  cover        <- attr(data_monthly, "cover")

  # ===========================================================================
  # 5. ED visit counts and the state case mix
  # ===========================================================================
  # Two count sessions feed epic_ed_n_visits (see note 4): the state x month
  # crosstab (all ages, nationally and by state, residence-based) and the state x
  # age band export (Country of Care; its national cells were dropped on read).
  # A count of 10 or fewer is imputed as 5 and flagged.
  counts_export <- flat_long %>%
    filter(column == "epic_ed_n_visits") %>%
    group_by(geography, time, age = age_label) %>%
    summarize(
      value = if (all(is.na(value))) NA_real_ else value[!is.na(value)][1],
      flag  = as.integer(any(imputed)),
      .groups = "drop"
    )

  # National all-ages total (residence-based) for the case-mix algebra
  nat_tbl <- counts_export %>%
    filter(geography == "00", age == "Overall") %>%
    select(time, N_nat = value)

  # National diagnosis mix (nat) from the national all-ages row
  nat_dx <- data_monthly %>%
    filter(geography == "00", age == "Overall") %>%
    select(time, all_of(paste0("epic_ed_pct_visits_", MH_DX))) %>%
    rename_with(~ sub("^epic_ed_pct_visits_", "nat_", .x), starts_with("epic_ed_pct_visits_"))

  # A state's share of the national diagnosis volume (r), from the all-ages state row
  state_r <- data_monthly %>%
    filter(geography != "00", age == "Overall") %>%
    select(geography, time, all_of(paste0("state_share_", MH_DX))) %>%
    rename_with(~ sub("^state_share_", "r_", .x), starts_with("state_share_"))

  v_state <- counts_export %>%
    filter(geography != "00", age == "Overall") %>%
    select(geography, time, V = value, V_flag = flag)

  n_state_age <- counts_export %>%
    filter(geography != "00", age != "Overall") %>%
    select(geography, time, age, Nsa = value, Nsa_flag = flag)

  # Writing N() for an encounter count, with r, nat and pa read as fractions:
  #
  #   N(state, dx)       = r(state, dx) * nat(dx) * N(nation, all)
  #   case mix, all ages = N(state, dx) / N(state, all)
  #   case mix, age band = pa(state, age, dx) * N(state, dx) / N(state, age, all)
  #
  # where N(nation, all) is the residence-based national total (nat_tbl) and
  # N(state, all) is the state's all-ages count. The dx buckets are not
  # guaranteed mutually exclusive, so the case mixes need not sum to a total.
  data_monthly <- data_monthly %>%
    left_join(nat_dx,      by = "time") %>%
    left_join(nat_tbl,     by = "time") %>%
    left_join(state_r,     by = c("geography", "time")) %>%
    left_join(v_state,     by = c("geography", "time")) %>%
    left_join(n_state_age, by = c("geography", "time", "age"))

  span_ok <- function(col, time) {
    cv <- cover[cover$column == col, ]
    if (nrow(cv) == 0) rep(FALSE, length(time)) else time >= cv$t_min & time <= cv$t_max
  }
  is_state <- data_monthly$geography != "00"
  is_all   <- data_monthly$age == "Overall"

  for (dx in MH_DX) {
    col  <- paste0("epic_ed_pct_visits_", dx)
    fcol <- paste0(col, "_suppressed_flag")
    r    <- data_monthly[[paste0("r_", dx)]]
    nat  <- data_monthly[[paste0("nat_", dx)]]
    pa   <- data_monthly[[paste0("epic_ed_pct_age_", dx)]]
    n_sdx <- (r / 100) * (nat / 100) * data_monthly$N_nat

    value <- case_when(
      is_state & is_all  ~ 100 * n_sdx / data_monthly$V,
      is_state & !is_all ~ 100 * (pa / 100) * n_sdx / data_monthly$Nsa,
      TRUE               ~ NA_real_
    )
    # The denominator must be published and not imputed
    den_ok <- case_when(
      is_state & is_all  ~ !is.na(data_monthly$V) & !is.na(data_monthly$N_nat) & data_monthly$V_flag %in% 0L,
      is_state & !is_all ~ !is.na(data_monthly$Nsa) & !is.na(data_monthly$N_nat) & data_monthly$Nsa_flag %in% 0L,
      TRUE               ~ FALSE
    )
    # ...and the numerator inputs must lie in the months their exports cover
    numer_span <- if_else(
      is_all,
      span_ok(paste0("state_share_", dx), data_monthly$time),
      span_ok(paste0("epic_ed_pct_age_", dx), data_monthly$time)
    )

    old_flag <- data_monthly[[fcol]]
    data_monthly[[col]]  <- if_else(is_state, value, data_monthly[[col]])
    data_monthly[[fcol]] <- case_when(
      !is_state          ~ old_flag,
      !is.na(value)      ~ 0L,
      den_ok & numer_span ~ 1L,
      TRUE               ~ NA_integer_
    )
  }

  # Visit counts: all ages nationally and by state, and by state x age band
  counts <- counts_export %>%
    rename(epic_ed_n_visits = value, epic_ed_n_visits_suppressed_flag = flag)

  data_monthly <- data_monthly %>%
    select(-any_of(c("N_nat", "V", "V_flag", "Nsa", "Nsa_flag")),
           -starts_with("nat_"), -starts_with("r_"), -starts_with("state_share_")) %>%
    full_join(counts, by = INDEX_COLS)

  # Order: index, each family's diagnoses in a stable order with each value
  # followed by its flag, then the visit count
  ordered <- intersect(
    as.vector(outer(names(DX_PATTERNS), FAMILIES, function(dx, fam) paste0(fam, "_", dx))),
    names(data_monthly)
  )
  data_monthly <- data_monthly %>%
    select(all_of(c(
      INDEX_COLS,
      as.vector(rbind(ordered, paste0(ordered, "_suppressed_flag"))),
      "epic_ed_n_visits", "epic_ed_n_visits_suppressed_flag"
    ))) %>%
    arrange(across(all_of(INDEX_COLS)))

  # ===========================================================================
  # 6. Validate
  # ===========================================================================
  dupes <- data_monthly %>% count(across(all_of(INDEX_COLS))) %>% filter(n > 1)
  if (nrow(dupes) > 0) {
    stop("Duplicate ", paste(INDEX_COLS, collapse = "/"), " rows: ", nrow(dupes))
  }

  # A value must carry flag 0 and a missing value must carry flag 1 or NA, never
  # 0. The visit count is imputed rather than missing when suppressed, so for it
  # the flag may be 1 on an observed value.
  measure_cols <- setdiff(
    grep("^epic_", names(data_monthly), value = TRUE),
    grep("_suppressed_flag$", names(data_monthly), value = TRUE)
  )
  for (mc in setdiff(measure_cols, "epic_ed_n_visits")) {
    v <- data_monthly[[mc]]
    f <- data_monthly[[paste0(mc, "_suppressed_flag")]]
    if (any(!is.na(v) & !(f %in% 0L)) || any(is.na(v) & f %in% 0L)) {
      stop("Value and suppression flag disagree in ", mc)
    }
  }

  # Quartile ordering over cells where all three statistics are published
  ord_total <- 0L
  ord_viol  <- 0L
  for (dxs in names(DX_PATTERNS)) {
    cols <- paste0("epic_ed_los_", c("q1", "median", "q3"), "_", dxs)
    if (!all(cols %in% names(data_monthly))) next
    tri <- data_monthly[, cols]
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
  if (any(unlist(data_monthly[, pct_cols]) < 0 | unlist(data_monthly[, pct_cols]) > 100, na.rm = TRUE)) {
    bad <- pct_cols[vapply(pct_cols, function(cn) any(data_monthly[[cn]] > 100 | data_monthly[[cn]] < 0, na.rm = TRUE), logical(1))]
    stop("Percentages outside [0, 100] in: ", paste(bad, collapse = ", "))
  }

  # The age bands of one state-month-diagnosis are shares of the same total
  # (every age), so they cannot sum past 100 beyond rounding
  pa_cols <- paste0("epic_ed_pct_age_", MH_DX)
  age_sums <- data_monthly %>%
    filter(age != "Overall", geography != "00") %>%
    group_by(geography, time) %>%
    summarize(across(all_of(pa_cols), ~ sum(.x, na.rm = TRUE)), .groups = "drop")
  max_sum <- max(unlist(age_sums[, pa_cols]))
  message("Age-share check: largest sum of age bands within a state-month-diagnosis is ",
          round(max_sum, 1), "%")
  if (max_sum > 101) stop("Age shares sum past 100 within a state-month-diagnosis.")

  # The seven mental health buckets should be a few percent of ED encounters
  mh_total <- data_monthly %>%
    filter(geography != "00", age == "Overall") %>%
    select(all_of(paste0("epic_ed_pct_visits_", MH_DX)))
  mh_total <- rowSums(mh_total)[stats::complete.cases(mh_total)]
  message("Mental health share of ED encounters, state x month: median ",
          round(median(mh_total), 2), "%, IQR ",
          paste(round(quantile(mh_total, c(0.25, 0.75)), 2), collapse = "-"), "%")
  if (median(mh_total) <= 1 || median(mh_total) >= 25) {
    stop("Derived mental health share of ED encounters is implausible.")
  }

  # The national count should be close to the sum of the states. It need not
  # match exactly: territories and unknown state of residence are in the
  # national figure only. Also, the state x age band counts (Country of Care)
  # must not exceed the state's all-ages count.
  band_sum <- counts_export %>%
    filter(age != "Overall") %>%
    group_by(geography, time) %>%
    summarize(bands = sum(value), n_bands = n(), .groups = "drop") %>%
    inner_join(filter(counts_export, age == "Overall") %>% select(geography, time, total = value),
               by = c("geography", "time"))
  message("Age-band counts / state all-ages count (all five bands present): median ",
          round(median(band_sum$bands[band_sum$n_bands == 5] / band_sum$total[band_sum$n_bands == 5]), 3))
  if (any(band_sum$bands > 1.02 * band_sum$total)) {
    stop("State age-band visit counts exceed the state's all-ages count in some month.")
  }

  count_check <- counts_export %>%
    filter(age == "Overall") %>%
    group_by(time) %>%
    summarize(ratio = value[geography == "00"] / sum(value[geography != "00"]), .groups = "drop")
  message(
    "ED visit counts, national / sum of states: median ",
    round(median(count_check$ratio), 3), ", range ",
    paste(round(range(count_check$ratio), 3), collapse = "-")
  )
  if (any(count_check$ratio < 0.9 | count_check$ratio > 1.2)) {
    stop("National ED visit count is far from the sum of the states in some month.")
  }

  message(
    "Monthly: ", nrow(data_monthly), " rows x ", ncol(data_monthly), " cols | ",
    length(unique(data_monthly$geography)), " geographies | ages: ",
    paste(sort(unique(data_monthly$age)), collapse = ", "), " | ",
    min(data_monthly$time), " to ", max(data_monthly$time)
  )

  # ===========================================================================
  # 7. Write standardized output
  # ===========================================================================
  if (!dir.exists("standard")) dir.create("standard")

  vroom::vroom_write(data_monthly, "standard/data.csv.gz", delim = ",")

  # ===========================================================================
  # 8. Record processed state
  # ===========================================================================
  process$raw_state <- current_state
  dcf::dcf_process_record(updated = process)
}
