# cosmos_diabetes_county

Diabetes indicators from Epic Cosmos by county of residence for January 2024 (Illinois,
Florida, Ohio, Wyoming): percent of patients with A1c 6.5+, percent meeting the CMS CCW
diabetes definition, and the patient count (the denominator). The same data are exported
twice with different stratifications, which makes the source suitable for studying how
suppression depends on how finely the data are sliced.

**Population base** (SlicerDicer sessions titled "DM in ICD10 by State Age 2023"): data
model `Patients`, population base `All Patients`, criteria `State of Residence = Illinois,
Florida, Ohio, Wyoming`, `Country of Care = United States of America`, `Has Any
Encounters`, `Base Patient`. Not restricted to emergency department visits.

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

## Standard output

| File | Session | Index columns |
|---|---|---|
| `standard/data_ruca_age.csv.gz` | 2860367 | `geography` (county FIPS), `time`, `ruca`, `age` |
| `standard/data_age_race.csv.gz` | 2860371 | `geography` (county FIPS), `time`, `age`, `race_ethnicity` |

Value columns, each followed by its own `_suppressed_flag`: `epic_pct_a1c_6_5_plus`,
`epic_pct_diabetes_ccw`, `epic_n_patients`.

- `time` is `2024-01-31` (end of the session date range).
- `ruca` / `age` / `race_ethnicity` include a `Total` level (all marginals are kept). RUCA
  levels: `Metropolitan`, `Micropolitan`, `Rural`, `Unknown`. Race and RUCA
  `None of the above` are relabelled `Unknown`.
- Race categories overlap (a patient can have several), so race counts do not sum to
  `Total`; RUCA levels do partition it.

## Updating

1. Re-run sessions 2860367 and 2860371 and export the crosstabs (rows: County of
   Residence, then the two stratifiers above; measures: A1c 6.5+ (%), Diabetes CCW
   definition (%), Number of Patients).
2. Drop both exports into `raw/staging/`, **replacing** the previous files.
3. From the project root: `Rscript -e "dcf::dcf_process('cosmos_diabetes_county')"`.

### Prerequisites

- `EPIC_XLSX_PASSWORD` set in `.Renviron` (`usethis::edit_r_environ()`).
- `msoffcrypto-tool` installed for the Python that R resolves (`python -m pip install msoffcrypto-tool`).

## Notes

- **Layout** is resolved from the export's header rows (the row holding `Measures` and the
  row below it). Extend `DIM_LABELS`, `MEASURE_PATTERNS` and `LAYOUTS` in `ingest.R` if
  the session changes; unrecognized labels stop the run.
- **Dropped rows**: the county `Total` row (a four-state total, not national, so it is
  not labelled `"00"`) and the county `None of the above` row.
- **Suppression** (flags computed before imputation): counts of 10 or fewer -> 5; a
  blank percent -> `5 / epic_n_patients * 100`; where the denominator was itself
  suppressed the percent is left `NA` (flag 1), since `5/5` would assert 100%.
- **County matching** normalizes names (case, punctuation, `Saint` -> `St`, county
  suffixes) and matches on state; unmatched counties stop the run.
- `missingness_report.Rmd` analyzes suppression across both exports; render it with
  `rmarkdown::render("missingness_report.Rmd")` from this directory.
