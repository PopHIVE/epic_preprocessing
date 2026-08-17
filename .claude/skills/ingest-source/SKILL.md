---
name: ingest-source
description: Ingest Epic Cosmos data into this repository — either onboard a NEW source (folder structure created exclusively via dcf::dcf_add_source(), then an ingest.R that standardizes SlicerDicer exports into PopHIVE wide format) or apply an UPDATE to an existing source (first confirm whether the export's structure changed; if it did, update the ingest folder; if it did not, refresh raw + standard only). Use when the user mentions "ingest", "new data source", "new SlicerDicer export", "update the data", "refresh <source>", drops a file into a raw/staging folder, or provides an Epic Cosmos session to onboard.
---

# ingest-source

Get Epic Cosmos SlicerDicer data into this repo in PopHIVE wide format — as a new
source or as an update to an existing one.

## Usage

```
/ingest-source <source_name> [description of the data, session ID, or "update"]
```

## Step 0 — Decide which path you are on (do this first, always)

Do **not** start writing code before answering this. The two paths have different
non-negotiables.

1. List the data directory: `data/`
2. Does `data/<source_name>/` already exist with a `process.json` whose `"type"` is
   `"source"`?

| Situation | Path |
|---|---|
| No such directory, or the user is onboarding a new measure/session/topic | **Path A — New source** |
| Directory exists and the user has (or is about to place) a fresher export for it | **Path B — Update** |
| Directory exists but the new export measures something genuinely different (different population base, different measure family) | **Path A**, as a new `cosmos_*` source — do not bolt it onto an existing one |

If it is ambiguous — e.g. the user says "ingest the new vaccine export" and both
`cosmos_vaccines` and `cosmos_hepb_vax` exist — ask which source it belongs to, or
whether it is a new one, before touching anything.

Every source in this repo is an Epic Cosmos SlicerDicer export, so read
**[Epic Cosmos SlicerDicer specifics](#epic-cosmos-slicerdicer-specifics)** and
**[PopHIVE wide format](#pophive-wide-format)** below on both paths.

---

# Path A — New source

## A1. Create the folder structure via dcf (non-negotiable)

The directory and **every file inside it** MUST be created by
`dcf::dcf_add_source()`. This is the only supported way to initialize a source.

**You MUST NOT:**
- Create `data/<source_name>/` or any subdirectory with `New-Item`, `mkdir`, or the Write tool
- Hand-write `process.json`, or copy one from another source or from `bundle_cosmos/`
- Scaffold empty `ingest.R` / `measure_info.json` / `README.md` before running `dcf_add_source()`
- Work around a missing or broken `dcf` install by writing the structure manually

**Why `process.json` in particular:** `dcf_add_source()` writes it with the exact
`name`, `type: "source"`, and `scripts: [{path: "ingest.R", ...}]` fields that
`dcf_process()` and `dcf_build()` depend on, and it is also where `raw_state` /
`standard_state` hashes live. A hand-written or copied one makes the source get
silently skipped or misread as a bundle (symptoms: "no standard data files found",
"processing bundle", `process file process.json does not exist`). These failures are
quiet, so there is no acceptable shortcut.

Steps:

1. **Validate the name.** Lowercase with underscores, prefixed `cosmos_` to match the
   rest of the repo (`cosmos_mental_health`, `cosmos_vaccines`, `cosmos_hepb_vax`).
   No spaces, no capitals, descriptive of the topic — not of the session.

2. **Locate Rscript.** It is on `PATH` in this environment; confirm and capture the path:
   ```powershell
   (Get-Command Rscript).Source
   ```
   If it is not on `PATH`, look under `C:\Users\<user>\AppData\Local\Programs\R\` and
   `C:\Program Files\R\`, and use the newest version found.

3. **Run dcf from the project root** (`epic_preprocessing/`, not from `data/`):
   ```powershell
   Rscript -e "dcf::dcf_add_source('<source_name>')"
   ```

4. **Verify what dcf produced:**
   ```
   data/<source_name>/
   ├── raw/                  # exports live here (create raw/staging/ for SlicerDicer files)
   ├── standard/             # standardized output
   ├── ingest.R              # you fill this in
   ├── measure_info.json     # you fill this in
   ├── README.md             # you fill this in
   ├── project.Rproj         # dcf-generated
   └── process.json          # dcf-generated — never hand-edit or hand-create
   ```

5. **Confirm `process.json`** before continuing: `"name"` matches the directory exactly,
   `"type"` is `"source"`, `"scripts"` references `"ingest.R"`. If any is wrong, re-run
   `dcf_add_source()` — do not patch the file.

If `dcf_add_source()` fails (package missing, R not found, permissions), **STOP and
report the error**. Fix the install first
(`remotes::install_github("dissc-yale/dcf")`); do not fabricate the structure.

From here on you only ever edit `ingest.R`, `measure_info.json`, and `README.md`.
`process.json` is owned by `dcf` and is updated at runtime by
`dcf::dcf_process_record()` inside `ingest.R`.

## A2. Gather information

Ask the user for anything not already supplied:

- **SlicerDicer session ID and title** — goes in the README so the export is reproducible
- **Population base** — e.g. "all ED encounters, country of residence = United States",
  or "patients with a newborn billed procedure (CPT 99460-99465, 99468, 99477)". This
  is the single most important thing to record; measures are not comparable across bases
- **Measures** — exact column labels in the export, and what each one counts
- **Stratifications (rows)** — Year, Month, State of Residence, Age at Encounter, etc.
- **Geographic level** — national/state (typical), or county/city
- **Time resolution** — monthly is typical here; weekly and annual also occur
- **Where the export is** — it should be dropped into `data/<source_name>/raw/staging/`

## A3. Examine the raw export before writing any code

Never guess the layout. Dump the top-left of the grid and read it:

```r
# from data/<source_name>/
library(openxlsx2)
f <- list.files("raw/staging", pattern = "\\.(csv|xlsx)$", full.names = TRUE)[1]

# SlicerDicer xlsx exports are password protected — decrypt first
dec <- tempfile(fileext = ".xlsx")
system(sprintf('python -m msoffcrypto -p "%s" "%s" "%s"',
               Sys.getenv("EPIC_XLSX_PASSWORD"), normalizePath(f, winslash = "/"), dec))

grid <- wb_to_df(wb_load(dec), sheet = 1, col_names = FALSE,
                 skip_empty_rows = FALSE, skip_empty_cols = FALSE)
print(grid[1:20, 1:min(12, ncol(grid))])   # header block + first data rows
```

Identify and write down:
- Which row holds the **stratification column names** (Year / Month / State of Residence …)
- Which row(s) above it hold **measure labels** or **column-group labels** — a
  two-level crosstab has an outer group one row higher (see `cosmos_mental_health`)
- Which columns are index vs value
- Suppression markers present (`"10 or fewer"`, `"-"`, blank, `"<0.01%"`)
- Rows to drop: `"None of the above"`, territories, partial periods (`"Jun 1 - Jun 22"`)
- Whether `"Total"` appears as a row (national) and/or as a column group (Overall)

## A4. Write `ingest.R`

One `ingest.R` per source — always. If a source needs several exports or several
outputs, integrate them into that one script and track each with its own
`process$<name>_state` key.

**Resolve the layout from the export's own header rows; do not hard-code column
positions.** SlicerDicer sessions drift: labels get renamed, stratifications get added.
Declare label maps at the top of the script and **error out on anything unrecognized**
rather than letting it land on the wrong column. Copy this pattern from
`data/cosmos_vaccines/ingest.R` (single-level) or
`data/cosmos_mental_health/ingest.R` (two-level crosstab).

Skeleton:

```r
# =============================================================================
# Epic Cosmos <Topic> Ingestion
# Source: Epic Cosmos SlicerDicer exports (raw/staging/*.xlsx)
#         <what it measures, stratified by what>
#
# Population base: <exact base>
# <notes on layout, suppression, censoring — future readers need these>
# =============================================================================

library(dplyr); library(tidyr); library(lubridate); library(openxlsx2)

# msoffcrypto-tool is required to decrypt the exports
msoffcrypto_check <- system(paste(Sys.which("python"), "-c \"import msoffcrypto\""),
                            ignore.stdout = TRUE, ignore.stderr = TRUE)
if (msoffcrypto_check != 0) {
  stop("Python package 'msoffcrypto-tool' is required but not installed.\n",
       "Install it with: python -m pip install msoffcrypto-tool")
}

# Initialize process record
if (!file.exists("process.json")) process <- list(raw_state = NULL) else
  process <- dcf::dcf_process_record()

xlsx_password <- Sys.getenv("EPIC_XLSX_PASSWORD")

# --- Label maps: the contract with the export -------------------------------
DIM_LABELS <- c("Year" = "year", "Month" = "month",
                "State of Residence" = "state_name")
MEASURE_PATTERNS <- c(epic_n_patients = "^Number of Patients",
                      epic_pct_<measure> = "<stable part of the label>")

# --- 1. Locate staging files & detect change --------------------------------
staging_files <- list.files("raw/staging", pattern = "\\.(csv|xlsx)$", full.names = TRUE)
if (length(staging_files) == 0) {
  stop("No staging files found in raw/staging/.\n",
       "Export data from Epic Cosmos SlicerDicer and place .xlsx files there.")
}
current_state <- list(files = staging_files,
                      hashes = unname(tools::md5sum(staging_files)))

if (!identical(process$raw_state, current_state)) {
  # 2. Read raw grid (read_slicerdicer_grid + extract_staging_data)
  # 3. Load FIPS lookup from ../../resources/all_fips.csv.gz
  # 4. Clean and standardize -> wide format
  # 5. Validate (see checklist)
  # 6. vroom::vroom_write(data_standard, "standard/data.csv.gz", delim = ",")
  # 7. Record state
  process$raw_state <- current_state
  dcf::dcf_process_record(updated = process)
}
```

Reuse `read_slicerdicer_grid()` and `extract_staging_data()` verbatim from an existing
source rather than reinventing them.

**A new source follows the same output conventions as every existing one** — these are
not optional and not negotiable per source:

- [PopHIVE wide format](#pophive-wide-format): index columns `geography`, `time`
  (+ `age` / `sex` / `race_ethnicity` when present), then one `epic_`-prefixed column
  per measure, each immediately followed by its own `_suppressed_flag`
- `time` formatted `YYYY-mm-dd` — monthly = last day of month, weekly = Saturday,
  annual = `YYYY-12-31`
- `geography` as FIPS strings resolved through `../../resources/all_fips.csv.gz`;
  national `"00"`
- [Suppression and imputation](#suppression-and-imputation-both-paths) — the blank-count
  → 5 and missing-percent → `5 / denominator` rules, with a flag per variable
- Output written as `standard/data.csv.gz` via `vroom::vroom_write(..., delim = ",")`

If a new source cannot follow one of these (say the measure is a mean, which has no
denominator to impute from), say so explicitly in the README and
`measure_info.json` — do not diverge silently.

## A5. Write `measure_info.json`

One entry for **every column** in the standardized output except the index columns —
including each `*_suppressed_flag`. Follow the schema in `CLAUDE.md` and mirror
`data/cosmos_vaccines/measure_info.json`.

```json
{
  "epic_<measure>": {
    "id": "epic_<measure>",
    "short_name": "Human-readable short name",
    "long_name": "Full descriptive name",
    "category": "respiratory|immunization|chronic|injury|mental_health",
    "short_description": "One sentence.",
    "long_description": "Population base, exact numerator/denominator, how suppression was handled and imputed, and any right-censoring caveat.",
    "statement": "In {location}, {value}...",
    "measure_type": "Percent|Count|Rate|Prevalence|Incidence",
    "unit": "Percent|Count|Minutes",
    "time_resolution": "Month",
    "sources": [{ "id": "epic_cosmos" }],
    "citations": []
  },
  "epic_<measure>_suppressed_flag": {
    "id": "epic_<measure>_suppressed_flag",
    "short_name": "Suppressed flag: <measure>",
    "long_name": "Suppression flag for <measure>",
    "category": "<same as measure>",
    "short_description": "Indicates whether the value was suppressed and imputed.",
    "long_description": "1 when Epic Cosmos suppressed the cell (count of 10 or fewer) and the value was imputed; 0 otherwise. State what the imputation was, or that the value was left missing.",
    "measure_type": "Binary",
    "unit": "0/1",
    "time_resolution": "Month",
    "sources": [{ "id": "epic_cosmos" }],
    "citations": []
  }
}
```

Rules:
- Reference sources by ID only — `"sources": [{ "id": "epic_cosmos" }]`. Never inline
  `name` / `url` / `restrictions` per measure.
- End the file with the `_sources` block containing `epic_cosmos`, copied from
  `CLAUDE.md` (or from `cosmos_vaccines/measure_info.json`), with `date_accessed` set
  to the export year.
- `long_description` is where the analytical caveats live: right-censoring, population
  base differences from sibling sources, coverage artifacts. Write them; they do not
  survive anywhere else.
- Use the `variants` mechanism when many columns differ only by a variant (virus,
  diagnosis bucket).

## A6. Write `README.md`

Model it on `data/cosmos_vaccines/README.md`. It must include:

1. One-paragraph description of what the source measures and its population base
2. An **Updating** section: the SlicerDicer session ID, the exact rows/measures to
   select, "drop the export into `raw/staging/`, replacing the previous file", and that
   the ingest reprocesses only when a staging hash changes
3. Environment prerequisites: `EPIC_XLSX_PASSWORD` in `.Renviron`, `msoffcrypto-tool`
   on the Python R uses
4. That the layout is resolved from the export's header rows, and which constants
   (`DIM_LABELS`, `MEASURE_PATTERNS`, …) to extend when the session changes
5. Notes: dropped rows, suppression handling, censoring, coverage caveats

## A7. Run and validate

```powershell
# from the project root
Rscript -e "dcf::dcf_process('<source_name>')"
```

Or, when iterating on the script itself:

```r
setwd("data/<source_name>"); source("ingest.R"); setwd("../..")
```

Then work the [validation checklist](#validation-checklist) and report: source path,
output columns, geographies, time range, row count, suppression counts, and what to do
next (add to a bundle, commit).

## A8. Render the validation report

Run [`scripts/validate_dataset.Rmd`](#validation-report-scriptsvalidate_datasetrmd)
against the new standard file before reporting the source done. Render to a **temp
file, never into the repo** — this report is a review aid, not a deliverable — and open
it straight in the browser:

```powershell
Rscript -e "out <- tempfile(fileext = '.html'); rmarkdown::render('scripts/validate_dataset.Rmd', output_file = out, params = list(data_file = 'data/<source_name>/standard/data.csv.gz'), envir = new.env(), quiet = TRUE); browseURL(out)"
```

This is a first-time source, so there is no previous committed version to diff against —
`git_compare` (on by default) will find none and the comparison sections will say so;
that is expected, not a failure. Read the rendered report for anything the
[validation checklist](#validation-checklist) only catches visually — unexpected
demographic categories, suppression-imputation FAILs, implausible trends, geographic
coverage gaps in the maps — and report anything odd to the user before moving on.

---

# Path B — Update of an existing source

The rule: **confirm whether the export's structure changed before touching anything.**
Structure unchanged → refresh `raw/` and `standard/` only. Structure changed → update
the ingest folder (`ingest.R`, `measure_info.json`, `README.md`) first, then refresh.

## B1. Snapshot what exists now

Before the new export overwrites anything, capture the current contract so you can diff
against it:

```r
# from data/<source_name>/
old <- vroom::vroom("standard/data.csv.gz", show_col_types = FALSE)
names(old); range(old$time); nrow(old)
```

Save the old standard file to the scratchpad directory (not into the repo) so you can
compare values on overlapping periods in B4.

Also read the current `ingest.R` label maps (`DIM_LABELS`, `MEASURE_PATTERNS`,
`SEX_LABELS`, `DX_PATTERNS`, …) — those are the declared structure.

## B2. Place the new export

Put the new file in `data/<source_name>/raw/staging/`. Then decide, explicitly:

- **Replacing** the prior export (the usual case — the new session covers the full
  history): delete the superseded file, so the same period is not read twice.
- **Adding** to it (the new export covers later periods only): keep both. The ingest
  binds all staging files, so overlapping periods would duplicate — the duplicate-index
  check in the script must pass.

Changing which files are in `raw/staging/` changes `current_state$files`/`hashes`, which
is what triggers reprocessing.

## B3. Confirm whether the structure changed

Check all four of these — a clean run alone is not sufficient evidence.

1. **Header rows vs label maps.** Dump the header block of the new export (the snippet
   in A3) and compare each stratification label and measure label against the maps in
   `ingest.R`.
2. **Run the ingest.** These scripts are written to `stop()` on an unrecognized
   dimension, an unrecognized/ambiguous measure label, a blank label, or a duplicate
   mapping. An error here *is* the structure-change signal — read the message; it names
   the constant to extend.
3. **Diff the output column set.** This catches the silent direction: a measure
   *removed* from the session produces fewer columns without any error.
   ```r
   setdiff(names(old), names(new))   # measures that disappeared
   setdiff(names(new), names(old))   # measures that appeared
   ```
4. **Diff the index columns.** A stratification added or dropped (e.g. an age split)
   changes the grain of the file, which is a structure change even if every measure
   column is identical.
5. **Check every measure still has its own flag.** A measure column without a matching
   `<measure>_suppressed_flag` means the
   [suppression rules](#suppression-and-imputation-both-paths) were not applied to it —
   treat that as a structure change to fix, on either path:
   ```r
   m <- setdiff(names(new), c("geography","time","age","sex","race_ethnicity"))
   m <- m[!grepl("_suppressed_flag$", m)]
   setdiff(m, sub("_suppressed_flag$", "", grep("_suppressed_flag$", names(new), value = TRUE)))
   ```

Report the verdict to the user in one line before proceeding — "structure unchanged,
refreshing data only" or "structure changed: <what>, updating the ingest folder".

## B4. If the structure is UNCHANGED — refresh raw + standard only

Do **not** rewrite `ingest.R`, `measure_info.json`, or `README.md`. Do not reformat,
"tidy", or re-order the script. The only changes should be:

- `raw/staging/<new export>` (and the deleted superseded export)
- `standard/data.csv.gz`
- `process.json` (`raw_state` / `standard_state` hashes, written by dcf)

Then run the regression checks:

```r
new <- vroom::vroom("standard/data.csv.gz", show_col_types = FALSE)

identical(names(old), names(new))                 # column set and order preserved
range(new$time)                                   # end date extended, start unchanged
nrow(new) >= nrow(old)                            # rows added, not lost
length(unique(new$geography))                     # still 52 (50 states + DC + "00")

# Values on overlapping periods should be stable — Epic backfills, so small drift is
# expected; large shifts mean the session criteria changed, not just the date range.
inner_join(old, new, by = intersect(names(old), c("geography","time","age","sex")),
           suffix = c("_old", "_new")) %>%
  summarize(across(ends_with("_new"), ~ NA)) # -> compare each measure old vs new
```

Suppression must behave the same way it did before — the new export brings new small
cells, so check that they were imputed rather than left blank:

```r
# Every measure keeps its flag, and no imputed cell was left missing
for (mc in setdiff(names(new), grep("_suppressed_flag$", names(new), value = TRUE))) {
  fl <- paste0(mc, "_suppressed_flag")
  if (!fl %in% names(new)) next
  cat(mc, "flagged:", sum(new[[fl]]), " still NA:", sum(is.na(new[[mc]])), "\n")
}
```

A count column should have **no** remaining `NA` (blanks become 5) and a percent column
should have `NA` only where the rules deliberately leave it — see the two documented
cases in [Suppression and imputation](#suppression-and-imputation-both-paths). Anything
else means a new suppression marker slipped through the parser.

Flag to the user: any dropped geography, any period that lost data, any measure whose
overlapping values moved materially, and the suppression counts. If a regression check
fails, stop and investigate — do not commit a quietly degraded file.

## B5. If the structure CHANGED — update the ingest folder, then refresh

Make the smallest correct change, in this order:

1. **`ingest.R`** — extend the label map that the change touches:
   - Renamed measure → widen the regex in `MEASURE_PATTERNS` to the stable part of the
     label (keep the old wording matching too, so historical exports still parse)
   - New measure → new `MEASURE_PATTERNS` entry, added to the measure/pct column vectors
     and to the validation blocks. It must get the same treatment as its siblings: an
     `epic_`-prefixed name, its own `_suppressed_flag`, and the
     [suppression rules](#suppression-and-imputation-both-paths) — a missing count filled
     with 5, a missing percent filled with `5 / denominator * 100`. A new percent measure
     needs its denominator identified; if the export carries none, say so in
     `measure_info.json` rather than leaving the cells silently `NA`
   - New stratification → new `DIM_LABELS` entry, plus handling in the transform and in
     `index_cols` (the existing scripts already build `index_cols` with `intersect()`,
     so a new dimension needs its own normalization, e.g. the age-label cleanup)
   - New column group / bucket → new `SEX_LABELS` / `DX_PATTERNS` entry
   - Keep the "unrecognized → `stop()`" behavior intact. Never relax an error into a
     silent default.
2. **`measure_info.json`** — add entries for every new column *and its
   `_suppressed_flag`*; update `long_description` where the measure's definition,
   labeling, or censoring changed; bump `date_accessed`. Remove entries only for
   columns genuinely gone from the output.
3. **`README.md`** — update the session ID, the rows/measures to select when
   re-exporting, and the notes for anything new (new caveat, new dropped rows).
4. **Re-run** the ingest, then run the full B4 regression check set plus the
   [validation checklist](#validation-checklist).
5. **Report** the column diff explicitly: columns added, removed, renamed, and any
   change in the file's grain.

Never hand-edit `process.json` on this path either — and if the source directory turns
out to be missing `process.json` altogether, it was not created by
`dcf::dcf_add_source()`: go to A1 and initialize it properly instead of writing the file.

## B6. Render the validation report

Once the B4 regression checks (or the B5 re-run) pass, render the same validation
report so the refreshed data gets an actual visual check, not just column-diff
assertions. Render to a **temp file, never into the repo**, and open it directly:

```powershell
Rscript -e "out <- tempfile(fileext = '.html'); rmarkdown::render('scripts/validate_dataset.Rmd', output_file = out, params = list(data_file = 'data/<source_name>/standard/data.csv.gz'), envir = new.env(), quiet = TRUE); browseURL(out)"
```

Leave `old_data_file` blank — `git_compare` (default `TRUE`) automatically diffs
against the previously committed version of this same file, which is exactly the
comparison an update needs (category diff in section 2, trend overlay and change map in
section 6). Review section 3 (suppression) and section 6 (old vs previous) especially
closely, and report anything unexpected before committing.

---

# PopHIVE wide format

The standardized output is **wide**: one row per unique index combination, one column
per measure.

**Index columns**, in this order, only those the export actually carries:
`geography`, `time`, then optionally `age`, `sex`, `race_ethnicity`.

**Value columns**, each immediately followed by its own suppression flag:

```
epic_<descriptive_name>
epic_<descriptive_name>_suppressed_flag
```

- Prefix `epic_` (or a more specific stable prefix such as `epic_ed_los_`) on **all**
  value columns from the source
- Lowercase with underscores, short but unambiguous
- No redundancy with `measure_type` (don't append `_rate` when `measure_type` is `Rate`)
- Suppression is **per measure** — Epic suppresses each cell independently, so a single
  shared `suppressed_flag` column is wrong whenever there is more than one measure

Geography:
- National `"00"`, state 2-digit FIPS string, county 5-digit FIPS string — always strings
- Resolve via `../../resources/all_fips.csv.gz`, never `cdlTools::fips()` (slow)
- SlicerDicer row `"Total"` → `geography_name = "United States"` → `"00"`

```r
all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)
state_fips_lookup <- all_fips %>%
  filter(nchar(geography) == 2) %>%
  select(geography, geography_name)
```

Time — `YYYY-mm-dd`, as used by every `cosmos_*` source in this repo:
- Monthly → last day of the month
- Weekly → Saturday at end of epiweek
- Annual → `YYYY-12-31`
- Always `format()` the Date explicitly before writing; never rely on implicit
  serialization

```r
date = as.Date(paste(year, month, "01"), format = "%Y %b %d"),
time = format(ceiling_date(date, "month") - days(1), "%Y-%m-%d")
```

Output:
```r
if (!dir.exists("standard")) dir.create("standard")
vroom::vroom_write(data_standard, "standard/data.csv.gz", delim = ",")
```

`standard/datapackage.json` is generated by `dcf` — do not write it by hand.

---

# Suppression and imputation (both paths)

Epic Cosmos suppresses small cells. Every source in this repo handles that the same way,
and **suppression is per variable** — each measure gets its own flag, never one shared
flag for the row.

**Rule 1 — a missing count is filled with 5.** A count cell that is blank, or reported
as `"10 or fewer"`, means 10 or fewer patients. Set the value to **5** and set that
variable's flag to **1**. Observed counts keep their value and get flag **0**.

**Rule 2 — a missing percent is filled with `5 / denominator`.** A percent cell that is
blank or `"-"` means its numerator was suppressed (an explicit `0%` is never emitted, so
a blank covers 0-10). Impute the numerator as 5 and re-express it on the percentage
scale: **`5 / <population denominator> * 100`**. Flag it **1**, exactly as for a count.
The denominator is the source's own patient-count column for that cell
(e.g. `epic_n_patients`).

**Flags are computed before imputation**, so they record what the source actually
withheld rather than what the script wrote. One flag column per measure, named
`<measure>_suppressed_flag`, emitted immediately after its measure, `0`/`1` integer.

```r
# --- Rule 1: counts ---------------------------------------------------------
epic_n_patients_suppressed_flag = if_else(
  is.na(epic_n_patients) | epic_n_patients == "10 or fewer", 1L, 0L, missing = 1L
),
epic_n_patients = if_else(
  epic_n_patients_suppressed_flag == 1L, 5, as.numeric(gsub(",", "", epic_n_patients))
),

# --- Rule 2: percents -------------------------------------------------------
# Flag first (pct_cols already parsed to numeric, with "-" and blanks -> NA)
across(all_of(pct_cols), ~ as.integer(is.na(.x)), .names = "{.col}_suppressed_flag"),

# Then impute the suppressed numerator as 5, on the percentage scale
across(all_of(pct_cols), ~ if_else(is.na(.x), 5 / epic_n_patients * 100, .x)),
```

Two cases where the rules need a stated decision rather than blind application — handle
them explicitly and document the choice in `measure_info.json` and the README:

- **The denominator was itself suppressed.** It has already been imputed to 5, so
  `5 / 5 * 100` asserts a meaningless 100%. `cosmos_vaccines` keeps the flag at 1 but
  leaves the percent `NA` there. Either follow that precedent or, if the user wants the
  literal `5 / denominator` everywhere, apply it — but say in the `long_description`
  that those cells are 100% by construction, not by observation.
- **The measure is a mean, not a count or percent** (e.g. average ED length of stay in
  `cosmos_mental_health`). There is no denominator to impute from, so leave the value
  `NA` with the flag set to 1.

Bounded values such as `"<0.01%"` are imputed at half the bound (`0.005`), the same
half-the-bound logic as `"10 or fewer"` → 5.

Every flag must satisfy: a flag of `1` never sits on a value the source actually
reported, and an imputed value always carries a flag of `1`. Assert this in the
validation block of `ingest.R` so a future export cannot break it quietly, and
`message()` the imputed-cell count per measure at the end of the run.

---

# Epic Cosmos SlicerDicer specifics

**Encrypted exports.** SlicerDicer `.xlsx` files are password protected. Decrypt with
`python -m msoffcrypto` using the password from `Sys.getenv("EPIC_XLSX_PASSWORD")`
(set in `.Renviron` via `usethis::edit_r_environ()`). Check for the Python package at
the top of the script and `stop()` with the install command if it is absent. Never
hard-code the password, and never commit it.

**Header block.** Rows 1-8/9 are session metadata (Session Title, ID, Data Model,
Population Base, Population Criteria Filters, Session Date Range, Export User, Date of
Export), then blank rows, then one or two rows of column-group/measure labels, then the
stratification header row, then data. Locate the stratification row by content, not by
a fixed `skip =`.

**Merged cells.** Stratification columns are only filled on the first row of each group.
Convert blanks to `NA`, then `tidyr::fill(..., .direction = "down")`.

**Two-level crosstabs.** The outer group label appears only on the first column of each
group; carry it forward across the columns it spans (see `cosmos_mental_health`).

**Suppression markers in the raw grid.** Counts of 10 or fewer arrive as `"10 or fewer"`
or as a blank cell; percentages for suppressed cells arrive as `"-"` or blank; bounded
values arrive as `"<0.01%"`. Parse `"-"` and `""` to `NA` before anything else, then
apply the rules in [Suppression and imputation](#suppression-and-imputation-both-paths):
missing count → 5, missing percent → `5 / denominator * 100`, one flag per variable,
flags computed before imputation.

**Rows to drop, with a message saying what was dropped:**
- Partial leading/trailing periods (`"Jun 1 - Jun 22"`, `"Jul 1 - Jul 15"`) — not
  comparable to whole months
- `"None of the above"` — unknown state of residence / no evaluated sex
- Non-US geographies: Canadian provinces, Mexican states, territories
  (filter to `c(state.name, "District of Columbia", "Total")`)
- Impossible values (e.g. negative durations) — drop, and say how many

**Non-UTF-8 bytes.** Apply `iconv(x, to = "UTF-8", sub = "")` *before*
`trimws()` / `na_if()`, especially on `month` and `state_name`.

**Thousands separators.** Strip `","` from counts before `as.numeric()`.

**Right censoring.** Any measure with a follow-up window (PCV within 182 days, RSV
immunization in a seasonal window) is understated over the most recent months. Document
it in `measure_info.json` and the README — it is not visible in the data.

---

# dcf quick reference

```powershell
Rscript -e "dcf::dcf_add_source('cosmos_<topic>')"   # NEW source only — from project root
Rscript -e "dcf::dcf_process('cosmos_<topic>')"      # run one source
Rscript -e "dcf::dcf_status()"                       # what is stale
Rscript -e "dcf::dcf_build()"                        # full rebuild
```

Within a source directory: `dcf::dcf_check()` validates the project;
`dcf::dcf_process_record()` reads/writes `process.json`.

`data/bundle_cosmos/ingest.R` is a wrapper that runs selected sub-projects and copies
their `standard/` outputs for backward compatibility with `PopHIVE/Ingest`. If a new or
updated source needs to flow there, add the `copy_if_exists()` line — but only when the
user asks for it.

---

# Validation report: `scripts/validate_dataset.Rmd`

An R Markdown report that gives the [validation checklist](#validation-checklist) a
visual pass: overview and format checks, demographic categories (diffed against a prior
version), suppression-imputation checks, national time series, choropleth maps, and an
old-vs-new comparison. It takes one required param, `data_file` (the standard file's
path relative to `project_root`, default `".."`), and otherwise defaults to comparing
against the file's previous committed git version (`git_compare: true`) — pass
`old_data_file` explicitly only when the comparison target isn't the git history (e.g.
comparing two non-committed files).

**Always render it to a temp file and open it in the browser — never render it into the
repo.** It is a review aid consumed once during ingestion, not a build artifact:

```powershell
Rscript -e "out <- tempfile(fileext = '.html'); rmarkdown::render('scripts/validate_dataset.Rmd', output_file = out, params = list(data_file = 'data/<source_name>/standard/data.csv.gz'), envir = new.env(), quiet = TRUE); browseURL(out)"
```

Used in [A8](#a8-render-the-validation-report) (new source) and
[B6](#b6-render-the-validation-report) (update).

---

# Validation checklist

- [ ] `process.json` exists, is dcf-generated, `type: "source"`, `scripts: ["ingest.R"]`
- [ ] Exactly one `ingest.R` in the source directory
- [ ] `geography` values are valid FIPS strings; national is `"00"`; leading zeros intact
- [ ] `time` is `YYYY-mm-dd`; monthly = last day of month, weekly = Saturday
- [ ] No duplicate rows per index combination (the script `stop()`s on this)
- [ ] Every value column is `epic_`-prefixed and has its own `_suppressed_flag`
- [ ] Missing/`"10 or fewer"` counts filled with **5**, flag set to 1
- [ ] Missing percents filled with **`5 / denominator * 100`**, flag set to 1; any cell
      deliberately left `NA` (suppressed denominator, mean-valued measure) documented
- [ ] Flags computed before imputation; a flag of 1 never sits on a source-reported value,
      and every imputed value carries a flag of 1
- [ ] Imputed-cell counts per measure reported via `message()` at the end of the run
- [ ] Values within plausible range (percentages in [0, 100]; durations positive)
- [ ] Dropped rows reported via `message()`, not silently discarded
- [ ] `measure_info.json` has an entry for every non-index column, `sources` by ID only,
      `_sources` block present
- [ ] Output is gzipped CSV at `standard/data.csv.gz`
- [ ] README documents the session ID, the update procedure, and the caveats
- [ ] On an update: column set, index grain, geography count, and time range all checked
      against the previous standard file
- [ ] [`scripts/validate_dataset.Rmd`](#validation-report-scriptsvalidate_datasetrmd)
      rendered to a temp HTML file and reviewed (never rendered into the repo)

---

# Example — new source

User: `/ingest-source cosmos_asthma Epic Cosmos ED visits for asthma by state and month, session 2901447`

1. `Rscript -e "dcf::dcf_add_source('cosmos_asthma')"` from the project root, then verify
   `process.json`
2. Confirm the population base and measures with the user; the export goes into
   `raw/staging/`
3. Dump the header block, note the stratification row and measure labels
4. Write `ingest.R` with `DIM_LABELS` / `MEASURE_PATTERNS`, producing
   `epic_pct_ed_visits_asthma` (+ flag) indexed by `geography`, `time`
5. Write `measure_info.json` (measures + flags + `_sources`) and `README.md`
6. Run `dcf::dcf_process('cosmos_asthma')`, work the checklist
7. Render `validate_dataset.Rmd` to a temp HTML file, open it, review, report

# Example — update

User: "here's the new mental health export"

1. Snapshot `cosmos_mental_health/standard/data.csv.gz` columns, grain, and time range
2. Drop the export into `raw/staging/`, remove the superseded file
3. Dump its header rows; compare against `DIM_LABELS` / `SEX_LABELS` / `DX_PATTERNS`;
   run the ingest
4. Structure unchanged → only `raw/staging/*`, `standard/data.csv.gz`, and `process.json`
   change; run the regression checks and report the extended time range and suppression
   counts.
   Structure changed (say a new `Anxiety` diagnosis bucket) → add it to `DX_PATTERNS`
   and the expected measure columns, add its two `measure_info.json` entries, note it in
   the README, re-run, and report the column diff.
5. Render `validate_dataset.Rmd` to a temp HTML file (git_compare picks up the
   previously committed version automatically), open it, review, report
