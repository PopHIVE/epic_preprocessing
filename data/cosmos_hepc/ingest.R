# =============================================================================
# Epic Cosmos Hepatitis C Data Ingestion
# Source: Epic Cosmos SlicerDicer exports
#         raw/staging/*.xlsx             -- hepatitis C diagnosis counts
#         raw/staging_medication/*.xlsx  -- HCV medication uptake
#
# Two crosstabs, merged into one standard/data.csv.gz keyed by
# (geography, time, age):
#
# 1. raw/staging/ (Session ID 2855468; predecessors 2850163/2850139/2847109/
#    2847096) -- annual counts of patients with a viral hepatitis C
#    diagnosis, and the all-patients denominator, by state of residence and
#    age at encounter. Population base: All Patients; Country of Care =
#    United States of America; Has Any Encounters?  (no diagnosis filter on
#    the population). Diagnosis is a ROW dimension with two buckets per
#    state/year/age:
#      "hepc"       -> patients with a viral hepatitis C diagnosis (numerator)
#      "Total: ..." -> all patients in that state/year/age cell (denominator)
#    epic_pct_hepc = numerator / denominator * 100.
#    As of the 2026-09-10 export, this session ALSO carries two extra
#    measure columns per age bucket ("HepC diagnosis (%)" and a "Percentage
#    ... within 6 months (%)" medication measure) that are NOT used here --
#    deliberately ignored, see the CAVEAT below. Only "Number of Patients"
#    is extracted, keeping this crosstab's output identical in shape to
#    before the 2026-09-10 update.
#
# 2. raw/staging_medication/ (Session ID 2855386; predecessor 2850012) --
#    among patients with a viral hepatitis C diagnosis (the same population
#    filter as crosstab 1's "hepc" bucket, applied here at the session/
#    population level), the rate of patients with 1 slice by all HCV
#    medications within 6 months of diagnosis, by state of residence and age
#    at encounter. As of the 2026-09-10 export this crosstab has THREE row
#    dimensions (Year, State of Residence, Age at Encounter in Years) and two
#    named measure columns -- "Number of Patients" and "Hep C medication
#    (%)" -- with NO "All Medications" bucket dimension (the predecessor
#    session 2850012 had a 4th row dimension, "All Medications", with a
#    "has HCV medication" vs "Total" bucket; that bucket dimension is GONE
#    in this session, replaced by applying the diagnosis filter directly to
#    the population):
#      epic_n_patients_hepc_medication_pop = "Number of Patients" (the
#        HCV-diagnosed population for that cell -- CONFIRMED to match
#        crosstab 1's epic_n_patients_hepc cell-for-cell on every
#        (geography, time, age) checked in the 2026-09-10 exports, e.g.
#        Texas / 2018 / 18-24 Years = 229 in both files)
#      epic_pct_hepc_medication = "Hep C medication (%)" (reported directly,
#        or imputed via the standard 5 / denominator rule when blank) --
#        CONFIRMED to match crosstab 1's now-ignored "Percentage ... within
#        6 months (%)" column cell-for-cell (e.g. Texas / 2018 / 18-24 Years
#        = 9.61% in both files), so this measure now genuinely represents
#        the medication-within-6-months rate AMONG ALL HCV-diagnosed
#        patients, not among an already-medicated subset (see CAVEAT below
#        -- this is a substantive, favorable change from the 2850012 session)
#      epic_n_patients_hepc_medication = round(population * rate / 100),
#        the DERIVED numerator -- not a value the source reports anywhere
#
# CAVEAT on crosstab 1's two new, unused measure columns (2026-09-10):
# "HepC diagnosis (%)" behaves inconsistently across the two Diagnosis (All)
# row buckets -- on the "Total" row it closely tracks (hepc count / total
# count) * 100, i.e. it approximates epic_pct_hepc itself, but on the
# "Hep C diagnosis" row it is a distinct ~60-93% value whose precise Epic-
# internal definition was not confirmed (plausibly the share of the loosely
# coded "hepc" bucket -- see the code-set CAVEAT below -- meeting a stricter/
# confirmed HepC diagnosis criteria, but this is not verified). The third
# column ("Percentage ... within 6 months (%)") was confirmed to duplicate
# raw/staging_medication's own epic_pct_hepc_medication exactly (see above).
# Given the ambiguity of the first and the redundancy of the second, BOTH are
# left out of the standard output; only "Number of Patients" is read from
# this crosstab, same as before this update.
#
# CAVEAT on the numerator's code set (both crosstabs): Session 2850012 (the
# predecessor of the current medication crosstab)'s own Population Criteria
# Filters read: "Chronic viral hepatitis C (ICD-10-CM: B18.2), Acute
# hepatitis E (ICD-10-CM: B17.2), Unspecified viral hepatitis C". B17.2 is
# NOT a hepatitis C code. This matches the predecessor session (2847096)
# used to define crosstab 1's "hepc" bucket (see epic_n_patients_hepc's
# long_description), so BOTH crosstabs' HCV-diagnosed populations include a
# small number of acute hepatitis E patients. This has not been re-verified
# against the current sessions (2855468 / 2855386); fix the bucket
# definition in SlicerDicer and re-export both sessions to remove it.
#
# Layout notes (resolved from each export's own header rows, not
# hard-coded):
#   Rows 1-8   session metadata (Session Title, ID, Data Model, Population
#              Base, Population Criteria Filters, Session Date Range,
#              Export User, Date of Export) -- neither crosstab currently
#              carries a separate metadata "Measure" row.
#   Rows 9-10  blank
#   Crosstab 1: Row 11 is the column axis label (Age at Encounter in Years),
#     filled only on the FIRST column of each 3-column measure group; Row 12
#     is a "Measures" header repeating ("Number of Patients", "HepC
#     diagnosis (%)", "Percentage ... within 6 months (%)") once per age
#     group; Row 13 is the row index header (Year, State of Residence,
#     Diagnosis (All)); Row 14+ is data, with the first two index columns
#     merged/filled down and the third (Diagnosis (All)) repeating every
#     row. Only the "Number of Patients" column of each age group's triplet
#     is extracted (see CAVEAT above).
#   Crosstab 2: Row 11 is a "Measures" header over two named value columns
#     (Number of Patients, Hep C medication (%)); Row 12 is the row index
#     header (Year, State of Residence, Age at Encounter in Years -- THREE
#     dimensions, no "All Medications" bucket); Row 13+ is data, with the
#     first two index columns merged/filled down and the third (Age at
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
# necessarily align -- validation therefore checks "at most one partial
# period, and it is the most recent" separately per crosstab, not on the
# merged file as a whole.
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
# summing the buckets. Both crosstabs have the age "Total" enabled.
#
# 3. raw/staging_vl_after_abnormal/ (Session ID 2857336) -- among patients
#    with the same viral hepatitis diagnosis, the rate of a subsequent NORMAL
#    HCV viral load result at >12 weeks and >20 weeks, restricted to the
#    "(All) Lab Components" row bucket = "Abonormal Hep C test" [sic --
#    source typo, kept verbatim as the match key] (patients whose qualifying
#    HCV lab result was abnormal). Four row dimensions: Year, Age at
#    Encounter in Years, (All) Lab Components, State of Residence (last,
#    repeats every row). Three named measure columns under a "Measures"
#    header: "Normal HCV VL >12 weeks (%)", "Normal HCV VL >20 weeks (%)",
#    "Number of Patients". The crosstab's OTHER Lab Components bucket
#    ("Total" -- all patients regardless of this filter) is dropped; it is
#    not the population this file is named for.
#
# 4. raw/staging_vl_after_meds/ (Session ID 2857341) -- the same two rate
#    measures, restricted to the "All Medications" row bucket = "HCV
#    medication" (patients who have received an HCV medication). Four row
#    dimensions in a DIFFERENT order: Year, All Medications, State of
#    Residence, Age at Encounter in Years (last, repeats every row). Same
#    three named measure columns as crosstab 3. Its own "Total" bucket (all
#    patients regardless of medication) is dropped for the same reason.
#
# Crosstabs 3 and 4 are written to a SEPARATE output,
# standard/data_normal_vl.csv.gz, together with a copy of the corresponding
# rows of standard/data.csv.gz -- see section 5b below. Only 4 states have
# any exported data in either session (Alabama, Illinois, Louisiana,
# Massachusetts); there is no national ("00") row in either crosstab's
# target bucket, so data_normal_vl.csv.gz is intentionally state-only with
# no national aggregate, unlike every other standard file in this repo.
#
# 5. raw/staging_dashboard/ (Session ID 2857569) -- a consolidated export
#    combining 6 measures at once (population size, HCV-medication-after-
#    diagnosis rate, abnormal-test-after-diagnosis rate, any-test-after-
#    diagnosis rate, and BOTH VL-normalization rates already covered by
#    crosstabs 3/4) into ONE crosstab, cross-tabulated by state/year AND two
#    row-bucket dimensions: (All) Lab Components in {"Hep C tests", Total}
#    and All Medications in {"Hep C meds", Total} -- 4 rows per state/year.
#    PER USER DIRECTION, only the fully-UNCONDITIONED (Total x Total) row is
#    kept. The population (epic_n_patients_hepc_new_diagnosis) is NEW/
#    INCIDENT hepatitis C diagnoses -- patients with a diagnosis in the given
#    year who did NOT have one in the prior year -- confirmed with the user;
#    this is why it is markedly smaller than crosstab 1's epic_n_patients_hepc
#    (which counts ALL patients diagnosed as of that year, i.e. the
#    cumulative/prevalent population) and why it declines toward the present
#    even as epic_n_patients_hepc grows: fewer NEW diagnoses each year, while
#    the prevalent pool keeps accumulating. This makes every one of these 6
#    measures a DIFFERENT population from the similarly-named measures
#    elsewhere in this file (crosstab 2's epic_pct_hepc_medication, crosstab
#    3/4's epic_pct_normal_vl_*_after_*, both computed among the PREVALENT
#    diagnosed population) -- see each measure's entry in measure_info.json.
#    Unlike crosstabs 3/4, this session DOES report a national "Total" row,
#    so geography here is the same 4 states (Alabama, Illinois, Louisiana,
#    Massachusetts) plus national "00" -- 5 geographies. The session has no
#    age dimension at all (grain is geography/time only). Because both the
#    grain (no age) and the national-row coverage differ from
#    data_normal_vl.csv.gz's documented invariants, this crosstab is written
#    to its own separate output, standard/data_dashboard.csv.gz, rather than
#    folded into either existing standard file.
#
#    RECONDITIONED denominators for the two "Normal VL" rates (2026-09-21
#    update, per explicit user direction): the source reports ALL 6 measures
#    against the same denominator, epic_n_patients_hepc_new_diagnosis (see
#    above) -- including "Normal hep c test after meds" and "Normal hep c
#    test after abnormal test", despite their names implying a narrower,
#    already-conditioned population. To build a true sequential care cascade
#    (diagnosed -> meds/abnormal-test/any-test -> normal VL), this ingest now
#    RE-EXPRESSES those two rates against the actual preceding-stage
#    population instead of the full new-diagnosis population:
#      epic_pct_normal_vl_20wk_after_meds_new_diagnosis is now numerator /
#        epic_n_patients_hepc_meds_after_diagnosis * 100 (was: numerator /
#        epic_n_patients_hepc_new_diagnosis * 100)
#      epic_pct_normal_vl_20wk_after_abnormal_new_diagnosis is now numerator /
#        epic_n_patients_hepc_abnormal_test_after_diagnosis * 100 (same change)
#    epic_n_patients_hepc_meds_after_diagnosis and
#    epic_n_patients_hepc_abnormal_test_after_diagnosis are new DERIVED
#    columns (the source never reports these sub-population counts directly,
#    only their rate against the new-diagnosis population -- see
#    round(population * rate / 100) below, the same derivation pattern used
#    throughout this file). The numerator (the "normal VL" patient count) is
#    still only recoverable via the source's own rate against the full
#    new-diagnosis population, so it is first backed out that way and only
#    then re-divided by the narrower denominator. Because the two stage-1
#    rates and the two VL rates are independently reported/rounded by the
#    source against the same total, the back-derived "normal VL" count can
#    rarely come out fractionally above the stage-1 count it should be a
#    subset of (observed once in the 2026-09-10 export: Illinois 2025, 194 vs
#    176) -- capped at the stage-1 count so the re-expressed rate never
#    exceeds 100%. epic_pct_hepc_meds_after_diagnosis,
#    epic_pct_hepc_abnormal_test_after_diagnosis, and
#    epic_pct_hepc_any_test_after_diagnosis are UNCHANGED -- still computed
#    against epic_n_patients_hepc_new_diagnosis, per explicit user direction.
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
  process <- list(
    raw_state = NULL, medication_raw_state = NULL,
    vl_abnormal_raw_state = NULL, vl_meds_raw_state = NULL,
    dashboard_raw_state = NULL
  )
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
DX_LABELS <- c("hepc" = "hepc", "hep c diagnosis" = "hepc")
DENOM_COL <- "epic_n_patients"

# --- Crosstab 2: HCV medication uptake (raw/staging_medication/) -----------
# Layout: THREE row dimensions (Year, State of Residence, Age at Encounter
# in Years) followed by two NAMED measure columns under a "Measures" header
# ("Number of Patients", "Hep C medication (%)") -- see
# extract_medication_data() below. MED_PCT_LABELS accepts both the current
# label and the predecessor session's (2850012), which reported the same
# rate under the name "hcv medication measure (%)"; the predecessor's extra
# "All Medications" row dimension is not otherwise supported by this parser.
DIM_LABELS_MED <- c(
  "Year"                      = "year",
  "State of Residence"        = "state_name",
  "Age at Encounter in Years" = "age_raw"
)
MED_PCT_LABELS <- c("Hep C medication (%)", "hcv medication measure (%)")
MED_NUM_COL   <- "epic_n_patients_hepc_medication"
MED_DENOM_COL <- "epic_n_patients_hepc_medication_pop"

# The measure both sessions report; asserted against each export's metadata
MEASURE_PATTERN <- "^Number of Patients"

# --- Crosstabs 3 & 4: "Normal HCV VL" outcome after two exposures ----------
# Both crosstabs report the same three named measure columns, in the same
# order, under a "Measures" header -- but with the row dimensions in
# DIFFERENT orders (see the header note above), so each gets its own
# DIM_LABELS. Only the named TARGET_BUCKET_* row of the bucket dimension is
# kept; the crosstab's own "Total" bucket is dropped (see header note).
NORMAL_VL_MEASURES <- c(
  "Normal HCV VL >12 weeks (%)", "Normal HCV VL >20 weeks (%)", "Number of Patients"
)
DIM_LABELS_VL_ABNORMAL <- c(
  "Year"                      = "year",
  "Age at Encounter in Years" = "age_raw",
  "(All) Lab Components"      = "bucket",
  "State of Residence"        = "state_name"
)
DIM_LABELS_VL_MEDS <- c(
  "Year"                      = "year",
  "All Medications"           = "bucket",
  "State of Residence"        = "state_name",
  "Age at Encounter in Years" = "age_raw"
)
TARGET_BUCKET_ABNORMAL <- "Abonormal Hep C test"  # sic -- source typo, kept verbatim
TARGET_BUCKET_MEDS     <- "HCV medication"
VL_FOUR_STATES <- c("Alabama", "Illinois", "Louisiana", "Massachusetts")

# --- Crosstab 5: consolidated dashboard export (raw/staging_dashboard/) ----
# FOUR row dimensions (Year, State of Residence, (All) Lab Components, All
# Medications) and SIX named measure columns under a "Measures" header --
# extract_named_measures_data() handles this shape unchanged, same as
# crosstabs 3/4. Only the row where BOTH bucket dimensions read their own
# "Total: ..." bucket (the unconditioned Total x Total combination) is kept;
# the other 3 combinations per state/year (restricted to the "Hep C tests"
# and/or "Hep C meds" buckets) are dropped -- the inverse of crosstabs 3/4,
# which keep a named bucket and drop "Total". The "Total" bucket text for
# (All) Lab Components embeds the state name (e.g. "Total: Total includes
# all data under the Illinois bucket..."), so matching is by prefix, not an
# exact string.
DIM_LABELS_DASHBOARD <- c(
  "Year"                  = "year",
  "State of Residence"    = "state_name",
  "(All) Lab Components"  = "lab_components",
  "All Medications"       = "all_medications"
)
DASHBOARD_MEASURES <- c(
  "Normal hep c test after meds, 20 weeks (%)",
  "Normal hep c test after abnormal test, 20 weeks (%)",
  "Number of Patients",
  "Hep C meds after diagnosis (%)",
  "Abnormal hep C tests after diagnosis (%)",
  "Any hep C test after diagnosis (%)"
)

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

vl_abnormal_staging_files <- list.files(
  "raw/staging_vl_after_abnormal",
  pattern = "\\.(csv|xlsx)$", full.names = TRUE
)
if (length(vl_abnormal_staging_files) == 0) {
  stop(
    "No staging files found in raw/staging_vl_after_abnormal/.\n",
    "Export the 'Normal HCV VL after abnormal VL' crosstab from Epic Cosmos ",
    "SlicerDicer and place the .xlsx file there."
  )
}

vl_meds_staging_files <- list.files(
  "raw/staging_vl_after_meds",
  pattern = "\\.(csv|xlsx)$", full.names = TRUE
)
if (length(vl_meds_staging_files) == 0) {
  stop(
    "No staging files found in raw/staging_vl_after_meds/.\n",
    "Export the 'Normal HCV VL after meds' crosstab from Epic Cosmos ",
    "SlicerDicer and place the .xlsx file there."
  )
}

dashboard_staging_files <- list.files(
  "raw/staging_dashboard",
  pattern = "\\.(csv|xlsx)$", full.names = TRUE
)
if (length(dashboard_staging_files) == 0) {
  stop(
    "No staging files found in raw/staging_dashboard/.\n",
    "Export the consolidated dashboard crosstab from Epic Cosmos SlicerDicer ",
    "and place the .xlsx file there."
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
current_vl_abnormal_state <- list(
  files  = vl_abnormal_staging_files,
  hashes = unname(tools::md5sum(vl_abnormal_staging_files))
)
current_vl_meds_state <- list(
  files  = vl_meds_staging_files,
  hashes = unname(tools::md5sum(vl_meds_staging_files))
)
current_dashboard_state <- list(
  files  = dashboard_staging_files,
  hashes = unname(tools::md5sum(dashboard_staging_files))
)

# A single guard covers all five crosstabs: standard/data_normal_vl.csv.gz
# (section 5b) is built from BOTH the VL crosstabs AND a slice of the main
# merged file, so any of the four inputs changing must trigger a full
# recompute of both standard outputs. Crosstab 5 writes its own separate
# output (standard/data_dashboard.csv.gz) but is tracked by the same guard
# for simplicity.
if (!identical(process$raw_state, current_state) ||
    !identical(process$medication_raw_state, current_med_state) ||
    !identical(process$vl_abnormal_raw_state, current_vl_abnormal_state) ||
    !identical(process$vl_meds_raw_state, current_vl_meds_state) ||
    !identical(process$dashboard_raw_state, current_dashboard_state)) {

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

    # Two possible layouts one row above the dimension header:
    #  - single-level: the column axis label directly, then one bucket label
    #    per value column (each bucket = one measure column)
    #  - two-level: a "Measures" header, with the column axis label (e.g. age
    #    bucket) one row further up, filled only on the FIRST column of each
    #    measure group -- see extract_staging_data()'s header comment. Only
    #    the column matching MEASURE_PATTERN is kept from each group; the
    #    others (e.g. extra reported percentages) are deliberately dropped.
    measures_row <- as.character(grid[hdr - 1L, ])
    two_level <- identical(measures_row[n_dim], "Measures")

    if (two_level) {
      axis_row_raw <- as.character(grid[hdr - 2L, ])
      if (!identical(axis_row_raw[n_dim], col_axis_label)) {
        stop(
          file_label, ": expected column axis label '", col_axis_label,
          "' in column ", n_dim, " of row ", hdr - 2L,
          " but found '", axis_row_raw[n_dim], "'.\n",
          "Update the column axis label constant in ingest.R."
        )
      }

      val_cols_all    <- (n_dim + 1L):ncol(grid)
      axis_labels_fwd  <- axis_row_raw[val_cols_all]
      axis_labels_fwd[axis_labels_fwd == ""] <- NA
      for (i in seq_along(axis_labels_fwd)) {
        if (is.na(axis_labels_fwd[i]) && i > 1L) axis_labels_fwd[i] <- axis_labels_fwd[i - 1L]
      }
      measure_labels <- measures_row[val_cols_all]

      keep <- grepl(MEASURE_PATTERN, measure_labels)
      if (!any(keep)) {
        stop(
          file_label, ": no measure column matching MEASURE_PATTERN ('",
          MEASURE_PATTERN, "') found under '", col_axis_label, "'."
        )
      }

      val_cols   <- val_cols_all[keep]
      col_labels <- axis_labels_fwd[keep]
    } else {
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
    }

    if (any(col_labels == "" | is.na(col_labels))) {
      stop(
        file_label, ": blank column label(s) at position(s) ",
        paste(val_cols[col_labels == "" | is.na(col_labels)], collapse = ", "),
        " above row ", hdr, "."
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

    body <- grid[(hdr + 1L):nrow(grid), c(seq_len(n_dim), val_cols), drop = FALSE]
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

  # Crosstab 2's layout is THREE row dimensions (Year, State of Residence,
  # Age at Encounter in Years) followed by two NAMED measure columns under a
  # "Measures" header, rather than one dimension's buckets spread across
  # columns -- extract_staging_data()'s column-axis-pivot shape does not fit,
  # so this is a dedicated parser. Both "Number of Patients" (the HCV-
  # diagnosed population for this cell) and the medication rate (the measure
  # named in MED_PCT_LABELS) are kept -- see the CAVEAT at the top of this
  # file: the source does not provide a numerator count directly, so it is
  # derived downstream as round(population * rate / 100).
  extract_medication_data <- function(grid, file_label, dim_labels, pct_label_options) {
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
    if (!measure_labels[2] %in% pct_label_options) {
      stop(
        file_label, ": unrecognized second measure column '", measure_labels[2],
        "'; expected one of: ", paste(pct_label_options, collapse = ", "), "\n",
        "Add the new label to MED_PCT_LABELS in ingest.R."
      )
    }

    body <- grid[(hdr + 1L):nrow(grid), c(seq_len(n_dim), n_dim + 1L, n_dim + 2L), drop = FALSE]
    rownames(body) <- NULL
    names(body) <- c(unname(dim_labels), "n_raw", "pct_raw")

    fill_cols <- unname(dim_labels)[1:(n_dim - 1L)] # Year, State: merged, filled down
    last_dim  <- unname(dim_labels)[n_dim]          # Age at Encounter: populated on every row

    body %>%
      mutate(across(all_of(unname(dim_labels)), ~ na_if(.x, ""))) %>%
      tidyr::fill(all_of(fill_cols), .direction = "down") %>%
      filter(!is.na(.data[[last_dim]])) %>%
      mutate(source_file = file_label)
  }

  # Generic extractor for a K-row-dimension x M-named-measure-column
  # crosstab, where the last dimension repeats every row and the earlier
  # K-1 dimensions are merged cells (filled down). Unlike
  # extract_staging_data() (one dimension spread across columns) or
  # extract_medication_data() (fixed at exactly 2 named measures), this
  # supports an arbitrary, ordered set of named measure columns under a
  # "Measures" header -- crosstabs 3 & 4's shape.
  extract_named_measures_data <- function(grid, file_label, dim_labels, expected_measures) {
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
        "Expected the leading cells of a row to be: ", paste(dim_names, collapse = " | "), "\n",
        "Update the relevant DIM_LABELS constant in ingest.R to match the export."
      )
    }

    extra_dims <- setdiff(as.character(grid[hdr, ])[-seq_len(n_dim)], "")
    if (length(extra_dims) > 0) {
      stop(
        file_label, ": unrecognized row dimension(s) in the header row: ",
        paste(extra_dims, collapse = ", "),
        "\nAdd them to the relevant DIM_LABELS constant in ingest.R."
      )
    }

    measures_row <- as.character(grid[hdr - 1L, ])
    if (!identical(measures_row[n_dim], "Measures")) {
      stop(
        file_label, ": expected 'Measures' label in column ", n_dim,
        " of row ", hdr - 1L, " but found '", measures_row[n_dim], "'.\n",
        "Update extract_named_measures_data() in ingest.R."
      )
    }

    measure_labels <- measures_row[(n_dim + 1L):ncol(grid)]
    measure_labels <- measure_labels[measure_labels != ""]
    val_cols <- (n_dim + 1L):(n_dim + length(measure_labels))

    if (!identical(measure_labels, expected_measures)) {
      stop(
        file_label, ": measure columns do not match expectations.\n",
        "Found:    ", paste(measure_labels, collapse = " | "), "\n",
        "Expected: ", paste(expected_measures, collapse = " | "), "\n",
        "Update the expected-measures constant in ingest.R, or re-export the session."
      )
    }

    body <- grid[(hdr + 1L):nrow(grid), c(seq_len(n_dim), val_cols), drop = FALSE]
    rownames(body) <- NULL
    names(body) <- c(unname(dim_labels), paste0("measure_", seq_along(measure_labels)))

    fill_cols <- unname(dim_labels)[seq_len(n_dim - 1L)]
    last_dim  <- unname(dim_labels)[n_dim]

    body %>%
      mutate(across(all_of(unname(dim_labels)), ~ na_if(.x, ""))) %>%
      tidyr::fill(all_of(fill_cols), .direction = "down") %>%
      filter(!is.na(.data[[last_dim]])) %>%
      mutate(source_file = file_label)
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
      DIM_LABELS_MED, MED_PCT_LABELS
    )
  }))

  data_raw_vl_abnormal <- bind_rows(lapply(vl_abnormal_staging_files, function(f) {
    extract_named_measures_data(
      read_slicerdicer_grid(f, xlsx_password), basename(f),
      DIM_LABELS_VL_ABNORMAL, NORMAL_VL_MEASURES
    )
  }))

  data_raw_vl_meds <- bind_rows(lapply(vl_meds_staging_files, function(f) {
    extract_named_measures_data(
      read_slicerdicer_grid(f, xlsx_password), basename(f),
      DIM_LABELS_VL_MEDS, NORMAL_VL_MEASURES
    )
  }))

  data_raw_dashboard <- bind_rows(lapply(dashboard_staging_files, function(f) {
    extract_named_measures_data(
      read_slicerdicer_grid(f, xlsx_password), basename(f),
      DIM_LABELS_DASHBOARD, DASHBOARD_MEASURES
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

  # unique(): DX_LABELS may map several source bucket spellings (e.g. "hepc"
  # and "Hep C diagnosis") to the same canonical suffix -- one output column
  # per suffix, not per spelling.
  num_cols   <- paste0("epic_n_patients_", unique(unname(DX_LABELS)))
  pct_cols   <- paste0("epic_pct_", unique(unname(DX_LABELS)))
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
  # The population filter (viral hepatitis C diagnosis) is applied at the
  # session/population level in this export, not as a row bucket -- see the
  # CAVEAT at the top of this file. The source gives this population's size
  # ("Number of Patients") and its own medication rate directly, but NOT the
  # count of patients meeting the measure -- that numerator is derived below
  # as round(population * rate / 100).
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

  data_raw_med <- data_raw_med %>%
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
      # against the predecessor session's 2026-08-18 export: every row with a
      # suppressed population also had a blank rate, and a further 60 rows
      # had a normal population but a blank rate (a numerator small enough to
      # withhold on its own) -- no row had a suppressed population with a
      # rate shown. Not independently re-verified against the current
      # (2855386) session, but the same Epic Cosmos suppression convention
      # is expected to apply.
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
  # 4c/4d. Crosstabs 3 & 4 -- "Normal HCV VL" outcome after two exposures.
  # Both report a population count for the TARGET bucket and two rates
  # directly; no numerator count is reported for either rate by the source,
  # so none is derived here -- only what the source itself provides. See the
  # header note for what "target bucket" means per crosstab and why the
  # crosstab's other bucket ("Total") is dropped.
  # ---------------------------------------------------------------------------
  build_normal_vl_standard <- function(data_raw, target_bucket, label,
                                        num_col, pct12_col, pct20_col) {
    names(data_raw)[names(data_raw) == "measure_1"] <- "pct12_raw"
    names(data_raw)[names(data_raw) == "measure_2"] <- "pct20_raw"
    names(data_raw)[names(data_raw) == "measure_3"] <- "n_raw"

    bad_age <- unique(data_raw$age_raw[!standardize_age_label(data_raw$age_raw) %in% AGE_EXPECTED])
    if (length(bad_age) > 0) {
      stop(
        label, ": unrecognized age bucket label(s): ", paste(bad_age, collapse = " | "),
        "\nExtend AGE_EXPECTED / standardize_age_label() in ingest.R."
      )
    }
    data_raw <- data_raw %>% mutate(age = standardize_age_label(age_raw))

    periods <- parse_period_end(data_raw$year)
    if (any(periods$partial)) {
      for (lbl in unique(data_raw$year[periods$partial])) {
        message(
          label, ": retaining partial period '", lbl, "' as time = ",
          format(periods$date[match(lbl, data_raw$year)], "%Y-%m-%d"),
          " - its counts cover only part of the year and are NOT comparable ",
          "to a full year."
        )
      }
    }

    n_dropped_bucket <- sum(data_raw$bucket != target_bucket)
    data_raw <- data_raw %>% filter(bucket == target_bucket)
    message(
      label, ": dropped ", n_dropped_bucket, " row(s) outside bucket = '",
      target_bucket, "' (the crosstab's own 'Total' bucket)."
    )

    dropped_geo <- setdiff(unique(data_raw$state_name), VL_FOUR_STATES)
    if (length(dropped_geo) > 0) {
      message(
        label, ": dropping geography rows outside the 4 covered states: ",
        paste(dropped_geo, collapse = " | ")
      )
      data_raw <- data_raw %>% filter(state_name %in% VL_FOUR_STATES)
    }

    out <- data_raw %>%
      left_join(state_fips_lookup, by = c("state_name" = "geography_name")) %>%
      mutate(
        time = format(parse_period_end(year)$date, "%Y-%m-%d"),
        n_suppressed_flag = if_else(
          is.na(n_raw) | n_raw == "" | n_raw == "10 or fewer", 1L, 0L, missing = 1L
        ),
        n_value = as.numeric(if_else(n_suppressed_flag == 1L, "5", gsub(",", "", n_raw))),
        pct12_parsed = as.numeric(gsub("%", "", pct12_raw)),
        pct12_suppressed_flag = as.integer(is.na(pct12_parsed)),
        pct20_parsed = as.numeric(gsub("%", "", pct20_raw)),
        pct20_suppressed_flag = as.integer(is.na(pct20_parsed))
      )

    if (any(is.na(out$geography))) {
      stop(
        label, ": unmatched state name(s): ",
        paste(unique(out$state_name[is.na(out$geography)]), collapse = " | ")
      )
    }
    if (any(is.na(out$time))) {
      stop(label, ": failed to parse year for ", sum(is.na(out$time)), " rows.")
    }
    if (any(is.na(out$n_value))) {
      stop(
        label, ": unparseable count value(s): ",
        paste(unique(out$n_raw[is.na(out$n_value)]), collapse = " | "),
        "\nAdd the new suppression marker to the flag logic in ingest.R."
      )
    }
    if (anyDuplicated(out[, index_cols]) > 0) {
      stop(label, ": duplicate cells for the same geography / time / age.")
    }

    # Rates: reported directly when present; imputed via 5 / population when
    # blank and the population itself was not suppressed; left NA when the
    # population WAS suppressed (cosmos_vaccines precedent -- 5 / 5 * 100
    # would assert a meaningless 100%).
    out[[num_col]] <- out$n_value
    out[[paste0(num_col, "_suppressed_flag")]] <- out$n_suppressed_flag
    out[[pct12_col]] <- case_when(
      !is.na(out$pct12_parsed) ~ out$pct12_parsed,
      out$n_suppressed_flag == 1L ~ NA_real_,
      TRUE ~ 5 / out$n_value * 100
    )
    out[[paste0(pct12_col, "_suppressed_flag")]] <- out$pct12_suppressed_flag
    out[[pct20_col]] <- case_when(
      !is.na(out$pct20_parsed) ~ out$pct20_parsed,
      out$n_suppressed_flag == 1L ~ NA_real_,
      TRUE ~ 5 / out$n_value * 100
    )
    out[[paste0(pct20_col, "_suppressed_flag")]] <- out$pct20_suppressed_flag

    measure_cols <- c(num_col, pct12_col, pct20_col)
    flag_cols    <- paste0(measure_cols, "_suppressed_flag")

    out <- out %>%
      select(all_of(index_cols), all_of(as.vector(rbind(measure_cols, flag_cols)))) %>%
      arrange(geography, time, age)

    # Dedicated validation, not validate_crosstab_standard(): these crosstabs
    # have no national "00" row by design -- only 4 states are covered.
    if (!all(grepl("^[0-9]{2}$", out$geography))) {
      stop(label, ": non-2-digit-FIPS geography values found.")
    }
    if (!all(grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", out$time))) {
      stop(label, ": time is not formatted as YYYY-mm-dd for all rows.")
    }
    for (mc in measure_cols) {
      fc <- paste0(mc, "_suppressed_flag")
      if (any(is.na(out[[fc]])) || !all(out[[fc]] %in% c(0L, 1L))) {
        stop(label, ": ", fc, " must be 0 or 1 with no missing values.")
      }
      if (any(out[[mc]] < 0, na.rm = TRUE)) stop(label, ": ", mc, " has negative values.")
    }
    if (any(is.na(out[[num_col]]))) {
      stop(label, ": ", num_col, " has missing values; every suppressed count should be imputed to 5.")
    }
    for (pc in c(pct12_col, pct20_col)) {
      if (any(out[[pc]] < 0 | out[[pc]] > 100, na.rm = TRUE)) {
        stop(label, ": ", pc, " has values outside [0, 100].")
      }
    }

    out
  }

  vl_abnormal_standard <- build_normal_vl_standard(
    data_raw_vl_abnormal, TARGET_BUCKET_ABNORMAL, "Crosstab 3 (VL after abnormal test)",
    num_col = "epic_n_patients_after_abnormal_vl",
    pct12_col = "epic_pct_normal_vl_12wk_after_abnormal_vl",
    pct20_col = "epic_pct_normal_vl_20wk_after_abnormal_vl"
  )

  vl_meds_standard <- build_normal_vl_standard(
    data_raw_vl_meds, TARGET_BUCKET_MEDS, "Crosstab 4 (VL after medication)",
    num_col = "epic_n_patients_after_hcv_medication",
    pct12_col = "epic_pct_normal_vl_12wk_after_hcv_medication",
    pct20_col = "epic_pct_normal_vl_20wk_after_hcv_medication"
  )

  # ---------------------------------------------------------------------------
  # 4e. Crosstab 5 -- consolidated dashboard export, Total x Total combo only.
  # Six measures at once, all computed within the SAME unconditioned baseline
  # population (see header CAVEAT) -- distinct from the similarly-named
  # measures in crosstabs 2/3/4. No age dimension; this session DOES report a
  # national "Total" row, unlike crosstabs 3/4.
  # ---------------------------------------------------------------------------
  names(data_raw_dashboard)[names(data_raw_dashboard) == "measure_1"] <- "pct_vl20wk_after_meds_raw"
  names(data_raw_dashboard)[names(data_raw_dashboard) == "measure_2"] <- "pct_vl20wk_after_abnormal_raw"
  names(data_raw_dashboard)[names(data_raw_dashboard) == "measure_3"] <- "n_raw"
  names(data_raw_dashboard)[names(data_raw_dashboard) == "measure_4"] <- "pct_meds_after_dx_raw"
  names(data_raw_dashboard)[names(data_raw_dashboard) == "measure_5"] <- "pct_abnormal_test_after_dx_raw"
  names(data_raw_dashboard)[names(data_raw_dashboard) == "measure_6"] <- "pct_any_test_after_dx_raw"

  n_dropped_combo <- sum(!(
    grepl("^Total:", data_raw_dashboard$lab_components) &
    grepl("^Total:", data_raw_dashboard$all_medications)
  ))
  data_raw_dashboard <- data_raw_dashboard %>%
    filter(grepl("^Total:", lab_components) & grepl("^Total:", all_medications))
  message(
    "Crosstab 5 (dashboard): kept only the Total x Total (unconditioned) row ",
    "per state/year, dropping ", n_dropped_combo, " row(s) restricted to the ",
    "Hep C tests / Hep C meds buckets."
  )

  periods_dash <- parse_period_end(data_raw_dashboard$year)
  if (any(periods_dash$partial)) {
    for (lbl in unique(data_raw_dashboard$year[periods_dash$partial])) {
      message(
        "Crosstab 5 (dashboard): retaining partial period '", lbl, "' as time = ",
        format(periods_dash$date[match(lbl, data_raw_dashboard$year)], "%Y-%m-%d"),
        " - its counts cover only part of the year and are NOT comparable to a ",
        "full year."
      )
    }
  }

  dropped_geo_dash <- setdiff(unique(data_raw_dashboard$state_name), valid_states)
  if (length(dropped_geo_dash) > 0) {
    message(
      "Crosstab 5 (dashboard): dropping non-state / unknown geography rows: ",
      paste(dropped_geo_dash, collapse = " | ")
    )
    data_raw_dashboard <- data_raw_dashboard %>% filter(state_name %in% valid_states)
  }

  dashboard_standard <- data_raw_dashboard %>%
    mutate(
      geography_name = if_else(state_name == "Total", "United States", state_name)
    ) %>%
    left_join(state_fips_lookup, by = "geography_name") %>%
    mutate(
      time = format(parse_period_end(year)$date, "%Y-%m-%d"),
      n_suppressed_flag = if_else(
        is.na(n_raw) | n_raw == "" | n_raw == "10 or fewer", 1L, 0L, missing = 1L
      ),
      n_value = as.numeric(if_else(n_suppressed_flag == 1L, "5", gsub(",", "", n_raw))),
      pct_meds_parsed     = as.numeric(gsub("%", "", pct_meds_after_dx_raw)),
      pct_meds_suppressed = as.integer(is.na(pct_meds_parsed)),
      pct_abn_parsed      = as.numeric(gsub("%", "", pct_abnormal_test_after_dx_raw)),
      pct_abn_suppressed  = as.integer(is.na(pct_abn_parsed)),
      pct_any_parsed      = as.numeric(gsub("%", "", pct_any_test_after_dx_raw)),
      pct_any_suppressed  = as.integer(is.na(pct_any_parsed)),
      pct_vlm_parsed      = as.numeric(gsub("%", "", pct_vl20wk_after_meds_raw)),
      pct_vlm_suppressed  = as.integer(is.na(pct_vlm_parsed)),
      pct_vla_parsed      = as.numeric(gsub("%", "", pct_vl20wk_after_abnormal_raw)),
      pct_vla_suppressed  = as.integer(is.na(pct_vla_parsed))
    )

  if (any(is.na(dashboard_standard$geography))) {
    stop(
      "Crosstab 5 (dashboard): unmatched geography name(s): ",
      paste(unique(dashboard_standard$geography_name[is.na(dashboard_standard$geography)]), collapse = " | ")
    )
  }
  if (any(is.na(dashboard_standard$time))) {
    stop("Crosstab 5 (dashboard): failed to parse year for ", sum(is.na(dashboard_standard$time)), " rows.")
  }
  if (any(is.na(dashboard_standard$n_value))) {
    stop(
      "Crosstab 5 (dashboard): unparseable count value(s): ",
      paste(unique(dashboard_standard$n_raw[is.na(dashboard_standard$n_value)]), collapse = " | "),
      "\nAdd the new suppression marker to the flag logic in ingest.R."
    )
  }
  dash_index_cols <- c("geography", "time")
  if (anyDuplicated(dashboard_standard[, dash_index_cols]) > 0) {
    stop("Crosstab 5 (dashboard): duplicate cells for the same geography / time.")
  }

  # Rule 1 (count) for the population, Rule 2 (5 / population * 100, or NA if
  # the population itself was suppressed) for every percent measure -- all
  # five rates share the same denominator, epic_n_patients_hepc_new_diagnosis.
  impute_dashboard_pct <- function(parsed, n_suppressed, n_value) {
    case_when(
      !is.na(parsed) ~ parsed,
      n_suppressed == 1L ~ NA_real_,
      TRUE ~ 5 / n_value * 100
    )
  }

  dashboard_standard$epic_n_patients_hepc_new_diagnosis <- dashboard_standard$n_value
  dashboard_standard$epic_n_patients_hepc_new_diagnosis_suppressed_flag <- dashboard_standard$n_suppressed_flag

  dashboard_standard$epic_pct_hepc_meds_after_diagnosis <- impute_dashboard_pct(
    dashboard_standard$pct_meds_parsed, dashboard_standard$n_suppressed_flag, dashboard_standard$n_value
  )
  dashboard_standard$epic_pct_hepc_meds_after_diagnosis_suppressed_flag <- dashboard_standard$pct_meds_suppressed

  dashboard_standard$epic_pct_hepc_abnormal_test_after_diagnosis <- impute_dashboard_pct(
    dashboard_standard$pct_abn_parsed, dashboard_standard$n_suppressed_flag, dashboard_standard$n_value
  )
  dashboard_standard$epic_pct_hepc_abnormal_test_after_diagnosis_suppressed_flag <- dashboard_standard$pct_abn_suppressed

  dashboard_standard$epic_pct_hepc_any_test_after_diagnosis <- impute_dashboard_pct(
    dashboard_standard$pct_any_parsed, dashboard_standard$n_suppressed_flag, dashboard_standard$n_value
  )
  dashboard_standard$epic_pct_hepc_any_test_after_diagnosis_suppressed_flag <- dashboard_standard$pct_any_suppressed

  # --- Derived stage-1 counts: patients within the new-diagnosis population
  # who received meds / had an abnormal test after diagnosis. Not reported by
  # the source directly (only the rate against epic_n_patients_hepc_new_diagnosis
  # is), so derived the same way as every other back-calculated count in this
  # file: round(population * rate / 100), with a suppressed cell imputed
  # straight to 5 rather than round-tripping through an already-imputed rate.
  # These become the denominators for the two "Normal VL" rates below.
  dashboard_standard$epic_n_patients_hepc_meds_after_diagnosis <- if_else(
    dashboard_standard$epic_pct_hepc_meds_after_diagnosis_suppressed_flag == 1L, 5,
    round(dashboard_standard$n_value * dashboard_standard$epic_pct_hepc_meds_after_diagnosis / 100)
  )
  dashboard_standard$epic_n_patients_hepc_meds_after_diagnosis_suppressed_flag <-
    dashboard_standard$epic_pct_hepc_meds_after_diagnosis_suppressed_flag

  dashboard_standard$epic_n_patients_hepc_abnormal_test_after_diagnosis <- if_else(
    dashboard_standard$epic_pct_hepc_abnormal_test_after_diagnosis_suppressed_flag == 1L, 5,
    round(dashboard_standard$n_value * dashboard_standard$epic_pct_hepc_abnormal_test_after_diagnosis / 100)
  )
  dashboard_standard$epic_n_patients_hepc_abnormal_test_after_diagnosis_suppressed_flag <-
    dashboard_standard$epic_pct_hepc_abnormal_test_after_diagnosis_suppressed_flag

  # --- Normal VL rates, RECONDITIONED onto their true care-cascade
  # denominator -- see the header note above. The source's own rate (against
  # the full new-diagnosis population) is still the only way to recover the
  # raw numerator COUNT, so it's backed out that way first and then
  # re-expressed as a percent of the narrower, preceding-stage denominator.
  raw_pct_vlm <- impute_dashboard_pct(
    dashboard_standard$pct_vlm_parsed, dashboard_standard$n_suppressed_flag, dashboard_standard$n_value
  )
  n_normal_after_meds <- if_else(
    dashboard_standard$pct_vlm_suppressed == 1L, 5,
    round(dashboard_standard$n_value * raw_pct_vlm / 100)
  )
  # Capped at the stage-1 count it should be a subset of -- see header note
  # on why the independently-rounded source rates can rarely disagree.
  n_normal_after_meds <- pmin(n_normal_after_meds, dashboard_standard$epic_n_patients_hepc_meds_after_diagnosis)
  dashboard_standard$epic_pct_normal_vl_20wk_after_meds_new_diagnosis <- case_when(
    dashboard_standard$pct_vlm_suppressed == 1L ~ NA_real_,
    dashboard_standard$epic_n_patients_hepc_meds_after_diagnosis_suppressed_flag == 1L ~ NA_real_,
    dashboard_standard$epic_n_patients_hepc_meds_after_diagnosis == 0 ~ NA_real_,
    TRUE ~ n_normal_after_meds / dashboard_standard$epic_n_patients_hepc_meds_after_diagnosis * 100
  )
  dashboard_standard$epic_pct_normal_vl_20wk_after_meds_new_diagnosis_suppressed_flag <- as.integer(
    dashboard_standard$pct_vlm_suppressed == 1L |
      dashboard_standard$epic_n_patients_hepc_meds_after_diagnosis_suppressed_flag == 1L
  )

  raw_pct_vla <- impute_dashboard_pct(
    dashboard_standard$pct_vla_parsed, dashboard_standard$n_suppressed_flag, dashboard_standard$n_value
  )
  n_normal_after_abnormal <- if_else(
    dashboard_standard$pct_vla_suppressed == 1L, 5,
    round(dashboard_standard$n_value * raw_pct_vla / 100)
  )
  n_normal_after_abnormal <- pmin(n_normal_after_abnormal, dashboard_standard$epic_n_patients_hepc_abnormal_test_after_diagnosis)
  dashboard_standard$epic_pct_normal_vl_20wk_after_abnormal_new_diagnosis <- case_when(
    dashboard_standard$pct_vla_suppressed == 1L ~ NA_real_,
    dashboard_standard$epic_n_patients_hepc_abnormal_test_after_diagnosis_suppressed_flag == 1L ~ NA_real_,
    dashboard_standard$epic_n_patients_hepc_abnormal_test_after_diagnosis == 0 ~ NA_real_,
    TRUE ~ n_normal_after_abnormal / dashboard_standard$epic_n_patients_hepc_abnormal_test_after_diagnosis * 100
  )
  dashboard_standard$epic_pct_normal_vl_20wk_after_abnormal_new_diagnosis_suppressed_flag <- as.integer(
    dashboard_standard$pct_vla_suppressed == 1L |
      dashboard_standard$epic_n_patients_hepc_abnormal_test_after_diagnosis_suppressed_flag == 1L
  )

  dash_pct_cols <- c(
    "epic_pct_hepc_meds_after_diagnosis",
    "epic_pct_hepc_abnormal_test_after_diagnosis",
    "epic_pct_hepc_any_test_after_diagnosis",
    "epic_pct_normal_vl_20wk_after_meds_new_diagnosis",
    "epic_pct_normal_vl_20wk_after_abnormal_new_diagnosis"
  )
  dash_count_cols <- c(
    "epic_n_patients_hepc_new_diagnosis",
    "epic_n_patients_hepc_meds_after_diagnosis",
    "epic_n_patients_hepc_abnormal_test_after_diagnosis"
  )
  dash_measure_cols <- c(
    "epic_n_patients_hepc_new_diagnosis",
    "epic_pct_hepc_meds_after_diagnosis",
    "epic_n_patients_hepc_meds_after_diagnosis",
    "epic_pct_hepc_abnormal_test_after_diagnosis",
    "epic_n_patients_hepc_abnormal_test_after_diagnosis",
    "epic_pct_hepc_any_test_after_diagnosis",
    "epic_pct_normal_vl_20wk_after_meds_new_diagnosis",
    "epic_pct_normal_vl_20wk_after_abnormal_new_diagnosis"
  )
  dash_flag_cols <- paste0(dash_measure_cols, "_suppressed_flag")

  dashboard_standard <- dashboard_standard %>%
    select(all_of(dash_index_cols), all_of(as.vector(rbind(dash_measure_cols, dash_flag_cols)))) %>%
    arrange(geography, time)

  # Dedicated validation: no age dimension (grain is geography/time only),
  # and a national "00" row IS expected here, unlike crosstabs 3/4.
  if (!all(grepl("^[0-9]{2}$", dashboard_standard$geography))) {
    stop("Crosstab 5 (dashboard): non-2-digit-FIPS geography values found.")
  }
  if (!"00" %in% dashboard_standard$geography) {
    stop("Crosstab 5 (dashboard): national row ('00') is missing from the output.")
  }
  if (!all(grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", dashboard_standard$time))) {
    stop("Crosstab 5 (dashboard): time is not formatted as YYYY-mm-dd for all rows.")
  }
  for (mc in dash_measure_cols) {
    fc <- paste0(mc, "_suppressed_flag")
    if (any(is.na(dashboard_standard[[fc]])) || !all(dashboard_standard[[fc]] %in% c(0L, 1L))) {
      stop("Crosstab 5 (dashboard): ", fc, " must be 0 or 1 with no missing values.")
    }
    if (any(dashboard_standard[[mc]] < 0, na.rm = TRUE)) {
      stop("Crosstab 5 (dashboard): ", mc, " has negative values.")
    }
  }
  for (cc in dash_count_cols) {
    if (any(is.na(dashboard_standard[[cc]]))) {
      stop(
        "Crosstab 5 (dashboard): ", cc, " has missing values; every ",
        "suppressed count should be imputed to 5."
      )
    }
  }
  for (pc in dash_pct_cols) {
    v <- dashboard_standard[[pc]]
    if (any(v < 0 | v > 100, na.rm = TRUE)) {
      stop("Crosstab 5 (dashboard): ", pc, " has values outside [0, 100].")
    }
  }
  # The two derived stage-1 counts are sub-populations of the new-diagnosis
  # population -- they should never exceed it (a "more meds patients than
  # diagnoses" cell would indicate a bug in the derivation above).
  for (cc in c("epic_n_patients_hepc_meds_after_diagnosis", "epic_n_patients_hepc_abnormal_test_after_diagnosis")) {
    over <- which(dashboard_standard[[cc]] > dashboard_standard$epic_n_patients_hepc_new_diagnosis)
    if (length(over) > 0) {
      stop(
        "Crosstab 5 (dashboard): ", cc, " exceeds epic_n_patients_hepc_new_diagnosis in ",
        length(over), " cell(s), e.g. ", dashboard_standard$geography[over[1]],
        " / ", dashboard_standard$time[over[1]]
      )
    }
  }

  # ---------------------------------------------------------------------------
  # 5. Merge the two crosstabs. A full join on (geography, time, age) merges
  #    them without truncating either source to the other's max date/grain --
  #    cells present in only one crosstab (e.g. each crosstab's own trailing
  #    partial period) get NA for the other crosstab's columns rather than
  #    being silently dropped.
  # ---------------------------------------------------------------------------
  data_standard <- full_join(hepc_standard, medication_standard, by = index_cols) %>%
    arrange(geography, time, age)

  # ---------------------------------------------------------------------------
  # 5b. Crosstabs 3 & 4 cover only 4 states, so they are combined with a
  #    4-state-only slice of standard/data.csv.gz into a SEPARATE output
  #    rather than folded into the (nationally-representative) main file.
  # ---------------------------------------------------------------------------
  vl_four_state_fips <- state_fips_lookup$geography[state_fips_lookup$geography_name %in% VL_FOUR_STATES]

  data_normal_vl_standard <- full_join(vl_abnormal_standard, vl_meds_standard, by = index_cols) %>%
    full_join(
      data_standard %>% filter(geography %in% vl_four_state_fips),
      by = index_cols
    ) %>%
    arrange(geography, time, age)

  # ---------------------------------------------------------------------------
  # 6. Write standardized output
  # ---------------------------------------------------------------------------
  if (!dir.exists("standard")) dir.create("standard")
  vroom::vroom_write(data_standard, "standard/data.csv.gz", delim = ",")
  vroom::vroom_write(data_normal_vl_standard, "standard/data_normal_vl.csv.gz", delim = ",")
  vroom::vroom_write(dashboard_standard, "standard/data_dashboard.csv.gz", delim = ",")

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

  message(
    "Wrote standard/data_normal_vl.csv.gz: ", nrow(data_normal_vl_standard), " rows, ",
    length(unique(data_normal_vl_standard$geography)), " geographies (",
    paste(sort(unique(data_normal_vl_standard$geography)), collapse = ", "), "), ",
    min(data_normal_vl_standard$time), " to ", max(data_normal_vl_standard$time)
  )
  for (mc in c(
    "epic_n_patients_after_abnormal_vl", "epic_pct_normal_vl_12wk_after_abnormal_vl",
    "epic_pct_normal_vl_20wk_after_abnormal_vl", "epic_n_patients_after_hcv_medication",
    "epic_pct_normal_vl_12wk_after_hcv_medication", "epic_pct_normal_vl_20wk_after_hcv_medication"
  )) {
    fc <- paste0(mc, "_suppressed_flag")
    message(
      "  ", mc, ": ", sum(data_normal_vl_standard[[fc]], na.rm = TRUE), " of ",
      sum(!is.na(data_normal_vl_standard[[fc]])), " cells suppressed (",
      round(100 * mean(data_normal_vl_standard[[fc]], na.rm = TRUE), 1), "%), ",
      sum(is.na(data_normal_vl_standard[[mc]])), " left NA"
    )
  }

  message(
    "Wrote standard/data_dashboard.csv.gz: ", nrow(dashboard_standard), " rows, ",
    length(unique(dashboard_standard$geography)), " geographies (",
    paste(sort(unique(dashboard_standard$geography)), collapse = ", "), "), ",
    min(dashboard_standard$time), " to ", max(dashboard_standard$time)
  )
  for (mc in dash_measure_cols) {
    fc <- paste0(mc, "_suppressed_flag")
    message(
      "  ", mc, ": ", sum(dashboard_standard[[fc]], na.rm = TRUE), " of ",
      sum(!is.na(dashboard_standard[[fc]])), " cells suppressed (",
      round(100 * mean(dashboard_standard[[fc]], na.rm = TRUE), 1), "%)",
      if (mc %in% dash_pct_cols) {
        paste0(", ", sum(is.na(dashboard_standard[[mc]])), " left NA")
      } else ""
    )
  }

  # ---------------------------------------------------------------------------
  # 7. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- current_state
  process$medication_raw_state <- current_med_state
  process$vl_abnormal_raw_state <- current_vl_abnormal_state
  process$vl_meds_raw_state <- current_vl_meds_state
  process$dashboard_raw_state <- current_dashboard_state
  dcf::dcf_process_record(updated = process)
}
