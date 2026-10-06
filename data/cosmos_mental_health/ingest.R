# =============================================================================
# Epic Cosmos Mental Health ED Length of Stay Ingestion
# Source: Epic Cosmos SlicerDicer exports (raw/staging/*.xlsx)
#         ED length of stay statistics (average, median, standard deviation,
#         minimum, maximum) and diagnosis case mix, by mental health ED
#         diagnosis grouper, state of residence, evaluated sex, and month.
#
# Population base: all ED encounters with country of residence = United States.
#
# The export is a nested crosstab whose column hierarchy is
#   Evaluated Sex > ED Diagnoses > Measures
# so each data cell is one (sex x diagnosis x statistic) combination. The
# hierarchy is read from the export's own header rows rather than hard-coded:
# each header level is classified by matching its labels against the known
# vocabularies below, so a session with a different nesting order, or one with
# no Measures level at all (a single-measure export), parses without changes.
# Unrecognized dimensions, sexes, diagnosis buckets, or measures raise an error
# rather than being silently mapped onto the wrong column.
#
# Two things about this source need care:
#
# 1. The diagnosis grouper's "Total" bucket covers every ED encounter, including
#    the non-mental-health diagnoses not displayed as their own columns, so it is
#    emitted as an all-cause reference series (epic_ed_los_avg_all_cause) rather
#    than as a mental-health total. Its average (~300 min nationally) sits well
#    below every mental health bucket (~450-1200 min), which could not hold for
#    a total restricted to mental health encounters.
#
# 2. The source's "Percentage of Sliced Population" field has a denominator that
#    depends on the cell's position in the crosstab, so it cannot be published
#    as a single column. It is instead converted to a uniformly defined case-mix
#    measure (epic_ed_pct_visits_*); see section 5 for the algebra.
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

# Recognized stratification (row) columns, keyed by their SlicerDicer label
DIM_LABELS <- c(
  "State of Residence"        = "state_name",
  "Year"                      = "year",
  "Month"                     = "month",
  "Age at Encounter in Years"  = "age"
)

# Recognized evaluated-sex column groups, keyed by their SlicerDicer label.
# NA marks a group that is read, used for validation, and then dropped:
# "None of the above" collects encounters with no evaluated sex recorded and is
# populated in well under 1% of cells.
SEX_LABELS <- c(
  "Male"              = "Male",
  "Female"            = "Female",
  "Ambiguous"         = "Ambiguous",
  "Total"             = "Overall",
  "None of the above" = NA
)

# Recognized ED Diagnoses buckets, keyed by the output column suffix. Values are
# regexes matched against the bucket label in the export.
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

# Recognized statistics, keyed by the internal name used below. An export with
# no Measures header level is assumed to carry the average alone, which is how
# single-measure SlicerDicer sessions are laid out.
MEASURE_PATTERNS <- c(
  avg    = "^Average ED Length of Stay",
  var    = "^ED Length of Stay.*Variance",
  max    = "^Max ED Length of Stay",
  min    = "^Min ED Length of Stay",
  median = "^Median ED Length of Stay",
  pct    = "^Percentage of Sliced Population"
)
DEFAULT_MEASURE <- "avg"

# Length-of-stay statistics published as their own columns, in output order.
# "var" is not published directly: a variance in squared minutes alongside means
# in minutes invites mistakes, so it is emitted as a standard deviation.
LOS_STATS <- c(avg = "avg", median = "median", sd = "var", min = "min", max = "max")

# =============================================================================
# 1. Locate raw SlicerDicer staging files
# =============================================================================
# Place exported .xlsx files from Epic Cosmos SlicerDicer into raw/staging/.
# Expected export layout (rows):
#   1-8:    Metadata header (Session Title, ID, Population Criteria, ...)
#   9-10:   Blank
#   11-13:  One row per column-hierarchy level, outermost first; each level's
#           name sits in the last stratification column and its group labels
#           appear on the first column of each group
#   14:     Stratification column names (State of Residence, Year, Month, ...)
#   15+:    Data rows

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

  # Resolve the nested crosstab layout from the export's own header rows and
  # return the data in long form: one row per (stratification x sex x diagnosis
  # x statistic) cell.
  extract_staging_data <- function(grid, file) {
    nm    <- basename(file)
    n_col <- ncol(grid)
    cell  <- function(r) trimws(as.character(unlist(grid[r, ])))

    # -- Locate the stratification header row --------------------------------
    # It is the row whose leading cells are all recognized dimension labels.
    header_row <- NA_integer_
    for (r in seq_len(min(30, nrow(grid)))) {
      lab <- cell(r)
      n   <- sum(lab != "")
      if (n > 0 && all(lab[seq_len(n)] != "") && all(lab[seq_len(n)] %in% names(DIM_LABELS))) {
        header_row <- r
        break
      }
    }
    if (is.na(header_row) || header_row < 3) {
      stop(
        "Could not locate the stratification header row in ", nm,
        ".\nExpected a row whose leading cells are all of: ",
        paste(sQuote(names(DIM_LABELS)), collapse = ", ")
      )
    }

    dim_raw    <- cell(header_row)
    n_dims     <- sum(dim_raw != "")
    dim_raw    <- dim_raw[seq_len(n_dims)]
    value_cols <- (n_dims + 1):n_col

    # -- Read the column-hierarchy levels above it ---------------------------
    # Group labels appear only on the first column of each group, so each level
    # is carried forward across the columns it spans. Levels are walked upward
    # until a row with no labels in the value region is reached.
    carry_forward <- function(x) {
      x[seq_len(n_dims)] <- ""
      for (i in seq_along(x)) if (i > 1 && x[i] == "") x[i] <- x[i - 1]
      x
    }

    levels <- list()
    r <- header_row - 1
    while (r >= 1 && any(cell(r)[value_cols] != "")) {
      levels[[length(levels) + 1]] <- carry_forward(cell(r))
      r <- r - 1
    }
    if (length(levels) == 0) {
      stop("No column-hierarchy header rows found above the stratification row in: ", nm)
    }

    # -- Classify each level by matching its labels against the vocabularies --
    match_patterns <- function(labels, patterns) {
      vapply(labels, function(lbl) {
        hit <- names(patterns)[vapply(patterns, function(p) grepl(p, lbl), logical(1))]
        if (length(hit) != 1) NA_character_ else hit
      }, character(1), USE.NAMES = FALSE)
    }

    assigned <- list(sex = NULL, dx = NULL, meas = NULL)
    for (lv in levels) {
      labs <- lv[value_cols]
      if (any(labs == "")) {
        stop("A column-hierarchy level does not cover every value column in: ", nm)
      }

      as_meas <- match_patterns(labs, MEASURE_PATTERNS)
      as_dx   <- match_patterns(labs, DX_PATTERNS)
      as_sex  <- ifelse(labs %in% names(SEX_LABELS), labs, NA_character_)

      if (!anyNA(as_meas) && is.null(assigned$meas)) {
        assigned$meas <- as_meas
      } else if (!anyNA(as_dx) && is.null(assigned$dx)) {
        assigned$dx <- as_dx
      } else if (!anyNA(as_sex) && is.null(assigned$sex)) {
        assigned$sex <- unname(SEX_LABELS[labs])
      } else {
        unknown <- unique(labs[is.na(as_meas) & is.na(as_dx) & is.na(as_sex)])
        stop(
          "Could not classify a column-hierarchy level in ", nm, ".",
          if (length(unknown)) paste0(" Unrecognized label(s): ",
            paste(sQuote(head(unknown, 5)), collapse = ", ")) else
            " Its labels matched a level that was already assigned.",
          "\nExtend SEX_LABELS / DX_PATTERNS / MEASURE_PATTERNS as needed."
        )
      }
    }

    if (is.null(assigned$dx)) {
      stop("No ED Diagnoses level found in: ", nm)
    }
    if (is.null(assigned$sex)) {
      # A session with no sex breakout is a single all-sex slice
      assigned$sex <- rep("Overall", length(value_cols))
    }
    if (is.null(assigned$meas)) {
      # Single-measure export: the statistic is named in the metadata header only
      assigned$meas <- rep(DEFAULT_MEASURE, length(value_cols))
      message("  no Measures level; assuming a single '", DEFAULT_MEASURE, "' measure")
    }

    if (anyDuplicated(paste(assigned$sex, assigned$dx, assigned$meas))) {
      stop("Two value columns map to the same sex/diagnosis/measure triple in: ", nm)
    }

    # -- Stratification columns ----------------------------------------------
    # These are merged cells: only filled on the first row of a group
    data_rows <- (header_row + 1):nrow(grid)
    strata <- grid[data_rows, seq_len(n_dims), drop = FALSE]
    colnames(strata) <- unname(DIM_LABELS[dim_raw])
    rownames(strata) <- NULL
    strata <- strata %>%
      mutate(across(everything(), ~ na_if(trimws(iconv(.x, to = "UTF-8", sub = "")), ""))) %>%
      fill(everything(), .direction = "down")

    message(
      "  layout: ", paste(colnames(strata), collapse = " x "),
      " | sexes: ", paste(unique(assigned$sex), collapse = ", "),
      " | diagnoses: ", length(unique(assigned$dx)),
      " | measures: ", paste(unique(assigned$meas), collapse = ", ")
    )

    bind_rows(lapply(seq_along(value_cols), function(k) {
      i <- value_cols[k]
      strata %>%
        mutate(
          sex   = assigned$sex[k],
          dx    = assigned$dx[k],
          meas  = assigned$meas[k],
          value = suppressWarnings(as.numeric(
            gsub("[%,]", "", trimws(grid[[i]][data_rows]))
          ))
        )
    }))
  }

  raw_list <- lapply(staging_files, function(f) {
    extract_staging_data(read_slicerdicer_grid(f, xlsx_password), f)
  })
  data_long <- bind_rows(raw_list)

  # ===========================================================================
  # 3. Load FIPS lookup
  # ===========================================================================
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    select(geography, geography_name)

  # ===========================================================================
  # 4. Filter to whole months and standard geographies
  # ===========================================================================
  valid_months <- c(
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
  )
  valid_states <- c(state.name, "District of Columbia")

  # Partial leading/trailing periods (e.g. "Jul 1 - Jul 22") cover only part of
  # a month and are not comparable to the full-month points, so they are dropped.
  dropped_periods <- data_long %>%
    filter(!month %in% valid_months) %>%
    distinct(year, month)
  if (nrow(dropped_periods) > 0) {
    message(
      "Dropping ", nrow(dropped_periods), " incomplete period(s): ",
      paste(dropped_periods$year, dropped_periods$month, collapse = "; ")
    )
  }

  # Territories, foreign states, Armed Forces regions and unknown state of
  # residence are dropped: all_fips carries no geography_name for those codes.
  dropped_geos <- setdiff(unique(data_long$state_name), c(valid_states, "Total", NA))
  if (length(dropped_geos) > 0) {
    message(
      "Dropping ", length(dropped_geos), " non-state geograph(ies): ",
      paste(dropped_geos, collapse = ", ")
    )
  }

  data_periods <- data_long %>%
    filter(month %in% valid_months, state_name %in% c(valid_states, "Total")) %>%
    mutate(
      # Time: last day of the reporting month
      date = as.Date(paste(year, month, "01"), format = "%Y %b %d"),
      time = format(ceiling_date(date, "month") - days(1), "%Y-%m-%d"),

      # Geography: "Total" -> national ("00"), states -> 2-digit FIPS
      geography_name = if_else(state_name == "Total", "United States", state_name)
    ) %>%
    left_join(state_fips_lookup, by = "geography_name") %>%
    filter(!is.na(geography), !is.na(time))

  # Standardize age labels when the export carries an age stratification
  if ("age" %in% names(data_periods)) {
    data_periods <- data_periods %>%
      mutate(
        age = stringr::str_replace(age, "^Less than\\s+(\\d+).*$", "<\\1 Years"),
        age = stringr::str_replace(age, "^(\\d+)\\s+Years or more$", "\\1+ Years"),
        age = if_else(grepl("^Total", age, ignore.case = TRUE), "Total", age)
      )
  }

  index_cols <- intersect(c("geography", "time", "age", "sex"), names(data_periods))
  cell_cols  <- c(index_cols, "dx")

  # One row per (index x diagnosis) cell, one column per statistic. The sex
  # groups that are dropped from the output are retained for now: they are
  # needed to validate the percentage shares below.
  cells <- data_periods %>%
    select(all_of(c(cell_cols, "meas", "value"))) %>%
    pivot_wider(names_from = meas, values_from = value)

  for (st in c(unname(LOS_STATS), "pct")) {
    if (!st %in% names(cells)) cells[[st]] <- NA_real_
  }

  # ===========================================================================
  # 5. Convert the source percentage into a case-mix measure
  # ===========================================================================
  # "Percentage of Sliced Population" is a share of the nearest enclosing total
  # in the crosstab hierarchy, so its denominator depends on where the cell sits.
  # Writing N() for an encounter count, the three cases are:
  #
  #   sex != Overall :  pct = 100 * N(state, sex, dx)  / N(state, all sexes, dx)
  #   sex == Overall :  pct = 100 * N(state, all, dx)  / N(nation, all, dx)
  #   sex == Overall
  #     & state == US :  pct = 100 * N(nation, all, dx) / N(nation, all, all-cause)
  #
  # All three were confirmed against the export: sex shares sum to 100 within a
  # state-month-diagnosis, state shares sum to 100 within a month-diagnosis, and
  # the national row reproduces the diagnosis mix.
  #
  # These are three different quantities, so the raw field is not publishable as
  # one column. It is converted instead to a single well-defined measure: the
  # percentage of ED encounters in that geography, month and sex carrying the
  # diagnosis. Writing r for a state's share of the national diagnosis volume and
  # nat for the national diagnosis mix,
  #
  #   share(state, dx) = r(state, dx) / r(state, all-cause) * nat(dx)
  #   share(nation, dx) = nat(dx)                                (read directly)
  #   share(state, sex, dx) = pct(state, sex, dx) / pct(state, sex, all-cause)
  #                           * share(state, dx)
  #
  # Verified two ways: the national mix read directly off the export equals the
  # median of the independently derived state shares, and the seven mental health
  # buckets sum to ~6.5% of ED encounters, matching published estimates.

  # Validation: sex shares must sum to ~100 within a state-month-diagnosis
  sex_sums <- cells %>%
    filter(sex != "Overall") %>%
    group_by(across(all_of(setdiff(cell_cols, "sex")))) %>%
    summarize(s = sum(pct, na.rm = TRUE), n = sum(!is.na(pct)), .groups = "drop") %>%
    filter(n == length(setdiff(unname(SEX_LABELS[!is.na(SEX_LABELS)]), "Overall")))
  if (nrow(sex_sums) > 0) {
    off <- mean(abs(sex_sums$s - 100) > 0.5)
    message(
      "Sex-share validation: ", nrow(sex_sums), " fully observed cells, ",
      round(100 * (1 - off), 1), "% sum to 100 +/- 0.5"
    )
    if (off > 0.05) {
      stop(
        "Sex shares do not sum to 100 in ", round(100 * off, 1),
        "% of fully observed cells. The percentage field's meaning has changed."
      )
    }
  }

  # Join keys: (time, diagnosis) for the national mix, (geography, time) for the
  # state's all-cause share. Both already carry the dimension they key on.
  by_time_dx    <- setdiff(cell_cols, c("geography", "sex"))
  by_time_state <- setdiff(cell_cols, c("dx", "sex"))

  overall <- cells %>% filter(sex == "Overall")

  # nat(dx): national diagnosis mix, read straight off the national row
  nat <- overall %>%
    filter(geography == "00") %>%
    select(all_of(by_time_dx), nat = pct) %>%
    distinct()

  # r(state, all-cause): the state's share of national all-cause volume
  r_all_cause <- overall %>%
    filter(geography != "00", dx == "all_cause") %>%
    select(all_of(by_time_state), r_all_cause = pct) %>%
    distinct()

  dx_share <- overall %>%
    left_join(nat, by = by_time_dx) %>%
    left_join(r_all_cause, by = by_time_state) %>%
    mutate(
      dx_share = if_else(geography == "00", nat, pct / r_all_cause * nat)
    ) %>%
    select(all_of(c(cell_cols, "dx_share")))

  # pct(state, sex, all-cause): that sex's share of the geography's volume
  sex_all_cause <- cells %>%
    filter(dx == "all_cause") %>%
    select(all_of(setdiff(cell_cols, "dx")), pct_sex_all_cause = pct) %>%
    distinct()

  cells <- cells %>%
    left_join(dx_share %>% select(all_of(c(setdiff(cell_cols, "sex"), "dx_share"))),
              by = setdiff(cell_cols, "sex")) %>%
    left_join(sex_all_cause, by = setdiff(cell_cols, "dx")) %>%
    mutate(
      pct_visits = if_else(
        sex == "Overall",
        dx_share,
        pct / pct_sex_all_cause * dx_share
      ),
      # All-cause is 100% of encounters by construction, so it carries no
      # information as a case-mix column and is not published.
      pct_visits = if_else(dx == "all_cause", NA_real_, pct_visits)
    )

  # ===========================================================================
  # 6. Build the standardized wide table
  # ===========================================================================
  # A blank cell is Epic's suppression of a cell built on 10 or fewer
  # encounters. These statistics are not counts, so there is no denominator to
  # impute against: the value is left missing and only the flag is set. Every
  # value the source published is carried through as published, so a missing
  # value and a flag of 1 mean exactly the same thing.
  #
  # That includes the negative durations Epic reports for some averages and many
  # minima (encounters whose recorded departure precedes arrival). They cannot be
  # real elapsed times, but they are what the source published, so they are
  # retained rather than dropped and are reported in section 7 for the user to
  # filter on. Negative values are also why LOS columns can go below zero.
  cells <- cells %>%
    mutate(
      across(all_of(unname(LOS_STATS)), ~ as.integer(is.na(.x)), .names = "flag_{.col}"),
      flag_pct_visits = as.integer(is.na(pct_visits)),

      # Variance is published as a standard deviation, on the same scale in
      # minutes as the other statistics.
      sd = sqrt(var)
    )

  # Assemble output columns: for each statistic, one column per diagnosis, each
  # immediately followed by its own suppression flag.
  stat_specs <- c(
    setNames(paste0("epic_ed_los_", names(LOS_STATS)), names(LOS_STATS)),
    c(pct_visits = "epic_ed_pct_visits")
  )
  stat_sources <- c(LOS_STATS, c(pct_visits = "pct_visits"))
  stat_flags   <- c(paste0("flag_", unname(LOS_STATS)), "flag_pct_visits")
  names(stat_flags) <- names(stat_specs)

  wide_parts <- lapply(names(stat_specs), function(st) {
    src  <- if (st == "sd") "sd" else unname(stat_sources[st])
    keep <- if (st == "pct_visits") setdiff(names(DX_PATTERNS), "all_cause") else names(DX_PATTERNS)

    cells %>%
      filter(dx %in% keep) %>%
      select(all_of(c(index_cols, "dx")), value = all_of(src),
             flag = all_of(unname(stat_flags[st]))) %>%
      pivot_wider(
        id_cols     = all_of(index_cols),
        names_from  = dx,
        values_from = c(value, flag),
        names_glue  = paste0(stat_specs[st], "_{dx}_{.value}")
      ) %>%
      rename_with(~ sub("_value$", "", .x)) %>%
      rename_with(~ sub("_flag$", "_suppressed_flag", .x))
  })

  data_standard <- Reduce(function(a, b) full_join(a, b, by = index_cols), wide_parts)

  # Drop the sex strata that are not published, now that they have served the
  # validation and the share algebra
  data_standard <- data_standard %>% filter(sex %in% unname(SEX_LABELS[!is.na(SEX_LABELS)]))

  # Order: index columns, then each statistic's diagnoses in a stable order,
  # each value column immediately followed by its flag
  value_cols <- unlist(lapply(names(stat_specs), function(st) {
    keep <- if (st == "pct_visits") setdiff(names(DX_PATTERNS), "all_cause") else names(DX_PATTERNS)
    base <- paste0(stat_specs[st], "_", keep)
    as.vector(rbind(base, paste0(base, "_suppressed_flag")))
  }), use.names = FALSE)

  missing_cols <- setdiff(value_cols, names(data_standard))
  if (length(missing_cols) > 0) {
    stop("Expected output column(s) absent: ", paste(missing_cols, collapse = ", "))
  }

  data_standard <- data_standard %>%
    select(all_of(c(index_cols, value_cols))) %>%
    arrange(across(all_of(index_cols)))

  # ===========================================================================
  # 7. Validate
  # ===========================================================================
  measure_cols <- setdiff(value_cols, grep("_suppressed_flag$", value_cols, value = TRUE))
  los_cols     <- grep("^epic_ed_los_", measure_cols, value = TRUE)
  pct_cols     <- grep("^epic_ed_pct_visits_", measure_cols, value = TRUE)

  dupes <- data_standard %>%
    count(across(all_of(index_cols))) %>%
    filter(n > 1)
  if (nrow(dupes) > 0) {
    stop(
      "Duplicate ", paste(index_cols, collapse = "/"), " combinations found: ",
      nrow(dupes), ". Check for overlapping staging exports."
    )
  }

  # Negative durations are retained as published, so they are reported rather
  # than treated as an error. They cannot be real elapsed times: filter on them
  # downstream if a non-negative duration is required.
  negatives <- data_standard %>%
    summarize(across(all_of(los_cols), ~ sum(.x < 0, na.rm = TRUE))) %>%
    unlist()
  if (any(negatives > 0)) {
    message(
      "Retained ", sum(negatives), " negative length-of-stay value(s) as published, in ",
      sum(negatives > 0), " column(s); largest concentration: ",
      names(which.max(negatives)), " (", max(negatives), ")"
    )
  }

  # Structural check that survives keeping the raw values: min <= median <= max
  # must hold for a genuine set of order statistics over the same encounters.
  # Isolated violations would be source noise, but a systematic failure means the
  # crosstab layout was misread and statistics were mapped onto the wrong columns.
  ord_viol <- 0L
  ord_total <- 0L
  for (dxs in names(DX_PATTERNS)) {
    tri <- data_standard[, paste0("epic_ed_los_", c("min", "median", "max"), "_", dxs)]
    ok  <- stats::complete.cases(tri)
    ord_total <- ord_total + sum(ok)
    ord_viol  <- ord_viol + sum(tri[ok, 1] > tri[ok, 2] | tri[ok, 2] > tri[ok, 3])
  }
  if (ord_total > 0) {
    message(
      "Order-statistic check: ", ord_viol, " of ", ord_total,
      " min/median/max triples violate min <= median <= max (",
      sprintf("%.2f%%", 100 * ord_viol / ord_total), ")"
    )
    if (ord_viol / ord_total > 0.01) {
      stop(
        "min <= median <= max fails in ", sprintf("%.1f%%", 100 * ord_viol / ord_total),
        " of triples. The crosstab layout was probably misread - check that the ",
        "Measures header level is being matched to the right columns."
      )
    }
  }

  pct_bad <- data_standard %>%
    summarize(across(all_of(pct_cols), ~ sum(.x < 0 | .x > 100, na.rm = TRUE))) %>%
    unlist()
  if (any(pct_bad > 0)) {
    stop(
      "Case-mix percentages outside [0, 100]: ",
      paste(names(pct_bad)[pct_bad > 0], collapse = ", ")
    )
  }

  # A flag of 1 must mean a missing value; an observed value must never be flagged
  for (mc in measure_cols) {
    if (any(data_standard[[paste0(mc, "_suppressed_flag")]] == 1L & !is.na(data_standard[[mc]]))) {
      stop("Observed values carry a suppression flag in ", mc)
    }
  }

  # The mental health buckets should account for a few percent of ED encounters
  mh_total <- rowSums(data_standard[, pct_cols], na.rm = TRUE)
  mh_total <- mh_total[rowSums(!is.na(data_standard[, pct_cols])) == length(pct_cols)]
  if (length(mh_total) > 0) {
    message(
      "Mental health share of ED encounters (fully observed rows): median ",
      round(median(mh_total), 2), "%, IQR ",
      paste(round(quantile(mh_total, c(0.25, 0.75)), 2), collapse = "-"), "%"
    )
    if (median(mh_total) <= 1 || median(mh_total) >= 25) {
      stop(
        "Derived mental health share of ED encounters is implausible (median ",
        round(median(mh_total), 2), "%). Check the percentage field's semantics."
      )
    }
  }

  message(
    "Standardized ", nrow(data_standard), " rows x ", ncol(data_standard), " cols | ",
    length(unique(data_standard$geography)), " geographies | ",
    length(unique(data_standard$sex)), " sexes | ",
    min(data_standard$time), " to ", max(data_standard$time)
  )
  for (st in names(stat_specs)) {
    cols <- grep(paste0("^", stat_specs[st], "_"), measure_cols, value = TRUE)
    message(
      "  ", format(stat_specs[st], width = 22),
      " observed: ", format(sum(!is.na(data_standard[, cols])), width = 6),
      " | suppressed: ", format(sum(data_standard[, paste0(cols, "_suppressed_flag")]), width = 6),
      " | negative: ", sum(data_standard[, cols] < 0, na.rm = TRUE)
    )
  }

  # Nothing is dropped, so a missing value and a flag of 1 must coincide exactly
  for (mc in measure_cols) {
    if (!identical(
      as.integer(is.na(data_standard[[mc]])),
      as.integer(data_standard[[paste0(mc, "_suppressed_flag")]])
    )) {
      stop("Missingness and the suppression flag disagree in ", mc)
    }
  }

  # ===========================================================================
  # 8. Write standardized output
  # ===========================================================================
  if (!dir.exists("standard")) dir.create("standard")

  vroom::vroom_write(data_standard, "standard/data.csv.gz", delim = ",")

  # ===========================================================================
  # 9. Record processed state
  # ===========================================================================
  process$raw_state <- current_state
  dcf::dcf_process_record(updated = process)
}
