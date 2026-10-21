# cosmos_vector_borne

Monthly Epic Cosmos vector-borne disease data, by state, for six diseases: Lyme disease,
Babesiosis, Malaria, RMSF (Rocky Mountain Spotted Fever), West Nile virus, and Dengue.
Two exports, each in its own `raw/staging/` subfolder (one file per folder, replacing the
previous file when updating), tracked by its own key in `process.json`, and written to its
own standard file. This is a dcf data source project, initialized with `dcf::dcf_add_source`.

| Export | Staging folder | Output | Columns |
|---|---|---|---|
| New cases, session `2863672` | `raw/staging/new_cases/` | `standard/data_new_cases.csv.gz` | `epic_n_new_<dz>`, `epic_pct_new_<dz>`, `epic_new_<dz>_suppressed_flag`, `epic_n_new_patients`, `epic_n_new_patients_suppressed_flag` |
| Lab component results, session `2863670` | `raw/staging/lab_tests/` | `standard/data_lab_tests.csv.gz` | `epic_n_tests_<dz>`, `epic_pct_tests_<dz>`, `epic_tests_<dz>_suppressed_flag`, `epic_n_lab_results`, `epic_n_lab_results_suppressed_flag` |

Both are PopHIVE wide format: `geography` (FIPS string; `"00"` is national, the SlicerDicer
`Total` row) and `time` (`YYYY-mm-dd`, the **last day of the month**).

## New cases

Monthly (Year > Month > State of Residence). The population itself is filtered to patients
with a first diagnosis of one of the six diseases (ICD-10 based, not preceded by the same
diagnosis within 6 months), Country of Care = United States, Has Any Encounters. Measures:
`n lyme`, `n babesiosis`, `n malaria`, `n RMSF`, `n west nile`, `n dengue`, `Number of
Patients`. `Number of Patients` is the size of that filtered population (not all patients),
so `epic_pct_new_<dz>` is a share among patients with a new vector-borne diagnosis. Most
state-month cells for the rarer diseases are suppressed (imputed as 5); prefer national or
annual aggregates for those. The trailing partial month is dropped.

## Lab tests

SlicerDicer session `2863670`, "Number of Lab Component Results and n west nile tests and
n babesia and n RMSF tests and n malaria tests and n dengue tests by State of Residence":
data model `Lab Component Results`, population base `All Lab Component Results`, criteria
`Country of Care = United States of America`. States are rows, and each measure
(`Number of Lab Component Results` as the denominator, `n west nile tests`, `n babesia`,
`n RMSF tests`, `n malaria tests`, `n dengue tests`) spans one column per month, grouped by
year. The `n babesia` label is assumed to be babesia tests.

- Row 11 holds the measure label (first column of each group only, so it is carried
  forward), row 12 the year, row 13 the month; row 14 is `State of Residence`. The ingest
  `stop()`s on any unrecognized measure label.
- The trailing partial month is dropped with a `message()`.
- Values count **lab component results**, not patients, and `epic_pct_tests_*` is the
  percent of **all** lab component results (any test), not a positivity rate.

## Common notes

- **Suppression**: a count of 10 or fewer arrives as a blank cell or `"10 or fewer"`; it is
  imputed as 5 and the corresponding flag set to 1 (flags are computed before imputation).
  One flag per disease (covering its `n` and `pct` columns) plus one for the denominator.
  Where the denominator was suppressed, `epic_pct_*` is `NA`.
- **Dropped rows**: non-US geographies (Canadian provinces, Mexican states, territories)
  and `None of the above` are dropped, with a `message()` reporting the labels.
- Malaria and Dengue counts in the US are mostly travel-associated rather than locally
  acquired; these sessions do not distinguish the two.
- Each export expects a **single** staging file and `stop()`s if more than one is present.

## Updating

1. Re-run the SlicerDicer session and export the crosstab.
2. Drop it into the matching `raw/staging/` subfolder, **replacing** the previous file.
3. From the project root: `Rscript -e "dcf::dcf_process('cosmos_vector_borne')"`, or
   `setwd("data/cosmos_vector_borne"); source("ingest.R"); setwd("../..")`.

### Prerequisites

- `EPIC_XLSX_PASSWORD` set in `.Renviron` (`usethis::edit_r_environ()`); SlicerDicer xlsx
  exports are password protected.
- `msoffcrypto-tool` installed for the Python that R resolves via `Sys.which("python")`
  (`python -m pip install msoffcrypto-tool`).