# cosmos_mental_health

Emergency department length-of-stay (median, Q1, Q3) and each diagnosis's own
population-share measure from Epic Cosmos, for two mental-health ED diagnosis
buckets - **Suicidal behavior** and **Mood** - by state, month, and age at time
of visit.

**Population base**: data model `ED Encounters`, population base
`All ED Encounters`, criteria `Country of Residence = United States of America`.

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

## Standard output

`standard/data.csv.gz`, in PopHIVE wide format:

| Column | Notes |
|---|---|
| `geography` | FIPS string; `"00"` is national (the SlicerDicer `Total` state row) |
| `time` | `YYYY-mm-dd`, the **last day of the month** |
| `age` | Epic's own "Age at Time of Visit" buckets, standardized to inclusive ranges, plus `Overall` from the age dimension's own Total bucket |
| `epic_median_ed_los_suicidal_behavior` / `_mood` | Median ED length of stay (minutes) |
| `epic_pct_sliced_population_suicidal_behavior` / `_mood` | See **"Percentage of Sliced Population" is not a rate** below |
| `epic_q1_ed_los_suicidal_behavior` / `_mood` | 25th-percentile ED length of stay (minutes) |
| `epic_q3_ed_los_suicidal_behavior` / `_mood` | 75th-percentile ED length of stay (minutes) |
| `<measure>_suppressed_flag` | One per measure, immediately following it |

The **Behavioral** ED-diagnosis bucket (session `2852635`) was intentionally
excluded from this ingest - it was exported with no age breakdown and without
the median-LOS/percentage measures, so it doesn't fit this file's grain. If it
is added later, re-export it the same way as the other two buckets first (see
below), then extend `ingest.R`'s allowed-diagnosis list.

The all-diagnoses **Total** ED-diagnoses bucket is dropped entirely: the two
diagnosis-pair exports below cover different date ranges and (for Mood)
slightly different age-bucket boundaries, so keeping "Total" would produce two
inconsistent baselines rather than one canonical series.

## Raw exports

Each diagnosis bucket requires **two** SlicerDicer sessions (Epic doesn't
export both measure pairs in one session), dropped into two staging folders
that `ingest.R` combines:

| Folder | Session | Diagnosis | Measures |
|---|---|---|---|
| `raw/staging_median_pct/` | `2852625` - "Median ED Length of Stay (mins) and Percentage of Sliced Population by ED Diagnoses and State of Residence and Age at Time of Visit Range" | Suicidal behavior | Median ED LOS, Percentage of Sliced Population |
| `raw/staging_median_pct/` | `2852768` - "Percentage of Sliced Population and Median Length of Stay by ED Diagnoses and State of Residence and Age at Time of Visit Range" | Mood | Percentage of Sliced Population, Median LOS |
| `raw/staging_iqr/` | `2852630` - "Q3 ED length of stay and Q1 ED length of stay by ED Diagnoses and State of Residence and Age at Time of Visit Range" | Suicidal behavior | Q3 ED LOS, Q1 ED LOS |
| `raw/staging_iqr/` | `2852738` - "Q1 Length of Stay and Q3 Length of Stay by ED Diagnoses and State of Residence and Age at Time of Visit Range" | Mood | Q1 LOS, Q3 LOS |

The Mood sessions above are re-exports of the original `2852663`/`2852656`
sessions, which had a corrupted "Age at Time of Visit" label (see below); the
replacements fixed it and are what `raw/staging_*` currently holds.

Rows: Year, Month, State of Residence, Age at Time of Visit Range (column
order varies between exports - `ingest.R` locates each by name, not position).
Columns: ED Diagnoses (the bucket above, plus Total) x the two measures.

## Updating

1. Re-run each SlicerDicer session above (or a replacement covering the same
   diagnosis + measure pair) and export to `.xlsx`.
2. Drop each export into its matching `raw/staging_median_pct/` or
   `raw/staging_iqr/` folder, **replacing** the file for that diagnosis so the
   same months aren't read twice. Each folder currently expects one file per
   diagnosis bucket (two files total).
3. From the project root: `Rscript -e "dcf::dcf_process('cosmos_mental_health')"`,
   or while iterating:
   `setwd("data/cosmos_mental_health"); source("ingest.R"); setwd("../..")`.

### Prerequisites

- `EPIC_XLSX_PASSWORD` set in `.Renviron` (`usethis::edit_r_environ()`) -
  SlicerDicer xlsx exports are password protected.
- `msoffcrypto-tool` installed for the Python that R resolves via
  `Sys.which("python")` (`python -m pip install msoffcrypto-tool`).

### When the session structure changes

`ingest.R` locates the Year/Month/State of Residence/Age at Time of Visit
header row by content (not a fixed row/column position), and locates each
measure by a case-insensitive regex against the measure label one row above
it (`^Median.*Length of Stay`, `^Percentage of Sliced Population`,
`^Q3.*Length of Stay`, `^Q1.*Length of Stay`). An unrecognized ED Diagnoses
label or measure label **stops the run** rather than silently landing on the
wrong column - extend the allowed-diagnosis vector or the measure regex map
in `ingest.R` when a session changes wording or adds a bucket.

## Notes and caveats

- **"Percentage of Sliced Population" is not a rate.** This was verified
  empirically against the raw export: summed across every age bucket at the
  national ("Total" state) row for a fixed month, it totals ~100%; summed
  across every state at age "Overall" for a fixed month, it also totals
  ~100%. That's only consistent with (this cell's diagnosis-specific encounter
  count) / (that diagnosis's total US encounter count that month, across all
  states and ages) x 100 - i.e. a compositional share describing *which* ages
  and states a diagnosis's encounters fall into, not the share of *all* ED
  encounters that carry the diagnosis (unlike `epic_pct_concussion` in
  `cosmos_concussions`, which is a true prevalence rate, because that export
  also carries a total-ED-encounter denominator). A populous state will show
  a large share here purely from volume. Do not use this measure to compare
  diagnosis rates across geographies.
- **No denominator count is exported.** Unlike `cosmos_concussions` or
  `cosmos_gas`, none of these four exports include a raw patient/encounter
  count column - only length-of-stay statistics and the sliced-population
  percentage. So a suppressed cell can't be imputed as `5 / denominator`;
  every suppressed cell (median/Q1/Q3 LOS, and a blank percentage) is left
  `NA` and flagged, the same treatment used elsewhere in this repo for a
  mean-valued measure. The one exception is a bounded percentage like
  `"<0.01%"`, which is a reported (if imprecise) value: it's imputed at half
  the bound (`0.005`) and still flagged suppressed.
- **Mismatched session coverage.** The Suicidal-behavior sessions cover
  7/1/2022-6/30/2026; the Mood sessions cover 7/1/2023-6/30/2026 (one year
  shorter). The Mood columns are `NA` for 2022-07 through 2023-06.
- **Corrupted Mood age label (fixed).** The original Mood exports (sessions
  `2852663`/`2852656`) contained one malformed "Age at Time of Visit" label -
  `"Years or more and less than 30 Years"` - missing its leading lower-bound
  number, which meant the `10-14 Years` bucket was entirely missing for Mood.
  The replacement exports (`2852768`/`2852738`) fixed this; Mood's age buckets
  now match Suicidal-behavior's (`<5`, `5-9`, `10-14`, `15-19`, ...).
  `ingest.R` keeps a defensive guard that drops any row matching that
  malformed pattern (with a `message()` reporting the count) rather than
  guessing at the intended bound, in case a future re-export regresses.
- **Age buckets differ slightly by diagnosis.** Because they come from
  separate SlicerDicer sessions, the age dimension isn't guaranteed to be
  configured identically between the Suicidal-behavior and Mood exports.
  Compare the two diagnoses only at ages both report.
- **Dropped rows**, each reported via `message()`: the malformed Mood age
  label; the `"No value"` (unclassified) age bucket; non-US/catch-all state
  rows (territories, armed forces designations, `"None of the above"`).
- The script validates geography, time, duplicate index rows, percentage
  range, and the flag invariants before writing, and reports per-measure
  suppression counts.
