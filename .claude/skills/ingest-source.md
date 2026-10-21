---
name: ingest-source
description: Ingest a new Epic Cosmos data source into this repo (epic_preprocessing) — creates the folder structure via the dcf R package (required), writes an ingest.R script that parses a SlicerDicer export and standardizes it into wide format, and creates a measure_info.json documenting every output column. Use when the user wants to add a new cosmos_* data source, mentions "ingest", "new data source", or provides a SlicerDicer export to onboard.
---

# ingest-source

Ingest a new Epic Cosmos data source: create the folder structure, write the `ingest.R` script to
standardize a SlicerDicer export, and create `measure_info.json`.

Adapted from the `ingest-source` skill in the `PopHIVE/Ingest` repository. This repo only ever
ingests Epic Cosmos SlicerDicer exports (every source directory is `cosmos_*`, feeding
`data/bundle_cosmos`) — it has no website data-sources catalog build step (no
`scripts/build_docs.R`, no `docs/data_sources_index.json`), so this version drops that build
phase. It **keeps** the `_catalog` block itself, since its `files` field is a good place to
document the population/stratification each SlicerDicer pull actually represents (e.g. "ZIP-level,
general population" vs. "county-level, base-population-eligible patients only") even though
nothing currently builds a website index from it.

## Usage

```
/ingest-source <source_name> [description of data source and where to get it]
```

## Description

End-to-end skill for adding and ingesting a new Epic Cosmos data source into this repository:

1. Creates the folder structure **exclusively via `dcf::dcf_add_source()`** — never by hand
2. Examines the raw SlicerDicer export to understand its structure
3. Writes an `ingest.R` script that transforms the raw export into the standard wide format
4. Creates a `measure_info.json` documenting every output column, including the `_sources` block

## Instructions

When the user invokes this skill:

### Phase 1: Create Folder Structure

**CRITICAL — this phase is non-negotiable.** The directory and **every file inside it** MUST be
created by the `dcf` R package via `dcf::dcf_add_source()`. This is the only supported way to
initialize a source.

**You MUST NOT:**
- Create `data/<source_name>/` (or any subdirectory) with `mkdir`, `New-Item`, or the Write tool
- Hand-write `process.json`, or copy one from another source or from `bundle_cosmos`
- Scaffold empty `ingest.R` / `measure_info.json` files before running `dcf_add_source()`
- Work around a missing/broken `dcf` installation by writing the structure manually

**If `dcf_add_source()` fails** (package not installed, R not found, permissions), STOP and report
the error to the user. Do not proceed to Phase 4/5 and do not fabricate the structure — fix the
`dcf` installation first (`install.packages("dcf")` or `remotes::install_github("dissc-yale/dcf")`).

Steps:

1. **Validate the source name**: lowercase with underscores, prefixed `cosmos_` to match the
   convention of every existing source in this repo (`cosmos_immunization`, `cosmos_chronic`,
   `cosmos_birth_vaccines`, …). No spaces or special characters.

2. **Detect the R installation** (Windows only):
   ```powershell
   Get-ChildItem 'C:\Program Files\R' | Select-Object Name
   ```
   Use the most recent version found.

3. **Run the dcf command** from the project root:
   ```powershell
   & 'C:\Program Files\R\<R_VERSION>\bin\Rscript.exe' -e "dcf::dcf_add_source('<source_name>')"
   ```

4. **Verify the created structure**:
   ```
   data/<source_name>/
   ├── raw/                  # For downloaded/staged source files
   ├── standard/             # For standardized output files
   ├── ingest.R              # Transformation script (filled in below)
   ├── measure_info.json     # Variable metadata (filled in below)
   └── process.json          # Processing state (dcf-generated — DO NOT hand-edit or create)
   ```

5. **Confirm `process.json` is correct** before continuing: `"name"` matches the directory
   (dcf sometimes writes `"."` — that's expected) and `"type"` is `"source"`. If it looks wrong,
   re-run `dcf::dcf_add_source()` rather than patching the file by hand.

From this point on, only `ingest.R` and `measure_info.json` are edited by you. `process.json` is
owned by `dcf` and is updated at runtime through `dcf::dcf_process_record()` inside `ingest.R`.

### Phase 2: Gather Information

Ask the user (if not already provided):
- **What does the SlicerDicer session measure?**: the outcome(s)/variable(s) — e.g. "Percentage
  with Any Encounter preceded by Immunizations", filtered to specific vaccines
- **Geographic level(s)**: state, county, ZIP, or multiple — each level in this repo typically
  gets its own staging folder and its own `standard/data_*.csv.gz` file (see
  `cosmos_immunization` for an example with `staging/` → ZIP, `staging_county/` → county,
  `staging_county_base_pt/` → county with a base-population toggle)
- **Time resolution**: SlicerDicer exports here are typically annual or monthly
- **Demographic breakdowns**: age at encounter, sex, race/ethnicity, other filters used in the
  session
- **Whether the export is password-protected**: most raw `.xlsx` exports from SlicerDicer are
  encrypted and require the `EPIC_XLSX_PASSWORD` environment variable plus the
  `msoffcrypto-tool` Python package to decrypt (see `cosmos_immunization/ingest.R` for the
  pattern) — confirm this dependency is available before writing decrypt logic

If the user has already placed raw files in `raw/` (or a `raw/staging*` subfolder matching the
existing convention), examine them directly.

### Phase 3: Examine Raw Data

Before writing any code, understand the raw SlicerDicer export structure. See the
**"Epic Cosmos SlicerDicer Exports"** section of `CLAUDE.md` for the full parsing reference; the
key points:

- Multi-row header block before the actual data — skip 12 rows and assign column names manually
  (the true column names split across rows 11–12, so `header = TRUE` is unreliable)
- **Merged cells**: grouping columns (year, month/age group, state/county/ZIP, etc.) are only
  filled in on the first row of each group — convert blanks to `NA` then
  `tidyr::fill(..., .direction = "down")`
- **Suppression**: counts ≤ 10 appear as `"10 or fewer"`; percentages for suppressed rows appear
  as `"-"` — treat `"-"` as `NA`
- **Non-US / catch-all rows**: filter out foreign states/provinces, `"None of the above"`, and
  decide whether `"Total"` should map to geography `"00"` or be dropped, depending on what the
  session's population base represents
- **Non-UTF-8 characters**: apply `iconv(column, to = "UTF-8", sub = "")` **before**
  `trimws()`/`na_if()` on `month`/`state_name`/`county_name` columns
- **County name mismatches**: Epic Cosmos county names need normalization before joining to
  `resources/all_fips.csv.gz` (SAINT → ST., missing apostrophes, LA SALLE → LASALLE, Alaska
  boroughs/census areas, Virginia independent cities, Connecticut planning regions) — see the
  "County names not matching FIPS lookup" and "Connecticut county FIPS codes" sections of
  `CLAUDE.md` for the full normalization table and fallback `stringdist` fuzzy-match pattern
- **ZIP-level exports**: no FIPS join needed — ZIP is used directly as the geography-like
  identifier (see `cosmos_immunization`'s `data_zip.csv.gz`, which keeps ZIP rather than
  converting to FIPS)

Identify:
- Which columns map to `geography`, `time`, and demographic dimensions (`age`, `sex`,
  `race_ethnicity`)
- Which columns are outcome values vs. the patient-count denominator
- Any filtering needed (record types, aggregate rows to drop)
- Geographic format (state name, county name, ZIP, FIPS)
- Date format in the raw data (year/month columns, or year only)

### Phase 4: Write ingest.R

Write the `ingest.R` script at `data/<source_name>/ingest.R`.

#### Script Structure

```r
# =============================================================================
# {SOURCE_NAME} Data Ingestion
# Source: Epic SlicerDicer export ("{session title}")
# Geography:
#   raw/staging/        -> standard/data.csv.gz         ({geography level})
# Dimensions: {age/sex/etc.} x geography
# =============================================================================

library(dplyr)
library(tidyr)

# Initialize process record (creates process.json if it doesn't exist)
if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

# --- 1. Decrypt/read raw SlicerDicer export(s) ---
# --- 2. Check for changes (hash of raw file(s)) ---
if (!identical(process$raw_state, raw_state)) {

  # --- 3. Parse (skip 12 rows, assign names, tidyr::fill merged cells) ---
  # --- 4. Clean (iconv, suppression handling, geography normalization + FIPS join) ---
  # --- 5. Transform to standard wide format ---
  # --- 6. Write standardized output ---

  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
```

#### Output Format: Standard Wide Format

The standardized output MUST be in **wide format** with:

- **Index columns**: `geography`, `time`, and whichever of `age`, `sex`, `race_ethnicity` the
  session was stratified by (one row per unique combination)
- **Value columns**: each output measure gets its own column, e.g. `pct_rsv`, `pct_pcv`,
  `pct_zoster`, `pct_flu`, `n_patients` (see `cosmos_immunization/measure_info.json` for a live
  example)
- **Per-column suppression flags**: because a wide row can mix suppressed and non-suppressed
  measures, flag suppression **per value column** as `suppressed_flag_{column_name}`, not with a
  single shared `suppressed_flag` column — e.g. `suppressed_flag_pct_rsv`,
  `suppressed_flag_n_patients`

#### Column Naming Convention

Existing sources in this repo use short, descriptive, lowercase-with-underscores names for value
columns **without a source prefix** (`pct_rsv`, `pct_pcv`, `n_patients` — not `epic_pct_rsv`),
since every source here is already Epic Cosmos. Keep new columns consistent with that:

- All lowercase with underscores
- Short but unambiguous (`pct_{vaccine}`, `n_patients`, `rate_{condition}`)
- Avoid redundancy (don't repeat "percentage" if `pct_` prefix already implies it)

#### Geography Handling

- County/state exports: convert names to FIPS codes using `resources/all_fips.csv.gz` (see
  Phase 3 for the county-name-normalization caveats specific to Epic Cosmos exports)
- ZIP-level exports: keep ZIP as-is, do not force a FIPS conversion
- National level = `"00"`; state = 2-digit FIPS string; county = 5-digit FIPS string

#### Time Handling

- **Format**: `MM-DD-YYYY` (this repo's standard — see `CLAUDE.md`; note the Ingest repo has
  since moved to `YYYY-mm-dd`, but files in this repo currently write `MM-DD-YYYY`, so stay
  consistent with existing sources unless the user says otherwise)
- Always call `format()` explicitly on `Date` objects before writing — do not rely on vroom's
  implicit `Date` serialization — and verify the written output matches the expected format
- Weekly data: Saturday at end of epiweek. Monthly data: last day of month. Annual data:
  `12-31-YYYY`

#### Data Quality

- Handle suppressed values: `"10 or fewer"` → impute as `5`, flag with the matching
  `suppressed_flag_{column}` column; suppressed percentages (`"-"`) → `NA`
- Remove aggregate/catch-all rows that would double-count (`"None of the above"`, and decide
  case-by-case whether `"Total"` becomes geography `"00"` or is dropped)
- Ensure no duplicate rows per (geography, time, demographic) combination

#### Output Writing

```r
vroom::vroom_write(data_standard, "standard/data.csv.gz", ",")
```

If multiple geography levels are exported (state/county/ZIP, or a county export with a different
population base), write each to its own file — `standard/data_county.csv.gz`,
`standard/data_zip.csv.gz`, `standard/data_county_base_pt.csv.gz`, etc. — following the
`cosmos_immunization` pattern.

### Phase 5: Write measure_info.json

Create `data/<source_name>/measure_info.json` with an entry for every value column, following the
schema in `CLAUDE.md`.

#### For Each Value Column

```json
{
  "column_name": {
    "id": "column_name",
    "short_name": "Human-readable short name (< 100 chars)",
    "long_name": "Full descriptive name",
    "category": "respiratory|immunization|chronic|injury",
    "short_description": "One sentence description.",
    "long_description": "Detailed description with methodology notes — mention the SlicerDicer stratification, suppression of counts ≤ 10 imputed as 5, and suppression of percentages when the denominator is too small.",
    "statement": "Template: 'In {location}, {value}% of patients ...'",
    "measure_type": "Incidence|Prevalence|Rate|Percent|Count",
    "unit": "Cases per 100,000|Percent|Count",
    "time_resolution": "Week|Month|Year",
    "sources": [{ "id": "epic_cosmos" }],
    "citations": []
  }
}
```

Do **not** put inline `restrictions`, `name`, or `url` in each measure's `sources` array — only
`{ "id": "epic_cosmos" }`.

#### Always Include `_sources`

Use the standard `epic_cosmos` `_sources` block from `CLAUDE.md`'s "Epic Cosmos Source Metadata"
section verbatim (name, url, organization, description, restrictions) — copy it from an existing
`measure_info.json` in this repo (e.g. `data/cosmos_immunization/measure_info.json`) rather than
retyping it, so the wording stays identical across sources.

#### Include `_catalog` — Document the Population of Each Pull

Write `_catalog` as the last top-level key, after `_sources`. No script in this repo reads it
yet, but it's the natural place to record, in one spot, what population each SlicerDicer pull in
this source actually covers — useful the next time someone (human or Claude) needs to remember
why `data_county.csv.gz` and `data_county_base_pt.csv.gz` give different denominators.

```json
{
  "_catalog": {
    "summary": "One to two plain-English sentences describing the dataset and, if it matters, which patient population it was pulled from.",
    "search_terms": ["Immunizations", "rsv", "flu"],
    "bucket": [],
    "files": {
      "data_zip.csv.gz":            "ZIP-level, all patients with any encounter, by age",
      "data_county.csv.gz":         "County-level, all patients with any encounter, by age",
      "data_county_base_pt.csv.gz": "County-level, restricted to base-population-eligible patients only, by age"
    }
  }
}
```

- **`summary`** — ~1-2 sentences, derived from the `_sources` description; call out the
  population base only when it isn't the default "any patient with an encounter" (e.g. a pull
  restricted to a specific age range, insurance type, or the "base population" cohort).
- **`search_terms`** — 3-4 terms: topic label(s) first (`Immunizations`, `Respiratory`,
  `Chronic diseases`, …), then lowercase keywords a person would actually type (`rsv`, `flu`,
  `pcv`, `zoster`).
- **`bucket`** — leave `[]` unless the user names a specific grouping.
- **`files`** — one entry per `standard/data*.csv.gz`, a short noun phrase (~5-10 words) naming
  the geography level, **which population the pull was drawn from**, and any stratification
  beyond time/geography. This is the field that matters most here: when a source has multiple
  staging pulls with different population bases (like `cosmos_immunization`'s `staging` vs.
  `staging_county_base_pt`), spell out the difference explicitly rather than writing the same
  generic blurb for both files.

### Phase 6: Validate and Report

After writing all files:

1. **Check file structure**: verify `ingest.R`, `measure_info.json`, and the dcf-generated
   `process.json` all exist.
2. **If raw data is available**: offer to run the ingest.R script to test it (from the source
   directory, or via `dcf::dcf_process("<source_name>")` from the project root).
3. **Manually inspect the output**: read the first rows of each `standard/data*.csv.gz` produced
   (e.g. `vroom::vroom("standard/data.csv.gz", n_max = 10)`) and check:
   - `time` values match `MM-DD-YYYY`
   - `geography` values are valid FIPS codes (or ZIP codes, for ZIP-level files) with no
     unexpected `NA`s from a failed county-name join
   - Suppressed rows show `suppressed_flag_{column} == 1` and an imputed value of `5` (counts) or
     `NA` (percentages)
   - No duplicate rows per (geography, time, demographic) combination

   This repo does not currently have an automated `validate_standard.R` / `validate_dataset.Rmd`
   report — do this inspection manually, or propose adding one if the user wants a repeatable
   check.
4. **Report what was created**:
   - Source directory path
   - List of standardized output columns (name + what it measures)
   - Geographic level(s) covered
   - Time resolution
   - Next steps (run ingest, add to `bundle_cosmos`, etc.)

### Phase 7: Add to bundle_cosmos

Once the source's `standard/data*.csv.gz` files exist, wire it into
`data/bundle_cosmos/build.R` (or the relevant bundle) and re-run
`dcf::dcf_process("bundle_cosmos")` (or `dcf::dcf_process('./data/bundle_cosmos')` per this
repo's `README.md`) to regenerate the combined parquet output.

## Example

User: `/ingest-source cosmos_asthma Epic SlicerDicer export of "Percentage with Asthma Diagnosis", state and county level, annual, staged in raw/staging_county`

The skill would:
1. Run `dcf::dcf_add_source("cosmos_asthma")` to create the directory and all of its contents,
   then verify the generated `process.json`
2. Examine the staged SlicerDicer export (skip 12 header rows, check for merged cells and
   suppression markers)
3. Write `ingest.R` that decrypts/reads the export, normalizes county names, joins to
   `resources/all_fips.csv.gz`, and writes wide-format columns like `pct_asthma` and `n_patients`
   with matching `suppressed_flag_pct_asthma` / `suppressed_flag_n_patients` columns
4. Write `measure_info.json` with an entry per output column plus the standard `epic_cosmos`
   `_sources` block
5. Manually inspect the standardized output, then report the created structure and suggest
   wiring it into `bundle_cosmos`
