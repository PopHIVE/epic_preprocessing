# cosmos_vector_borne_new_cases

Monthly count of patients with a **new** (first-time) vector-borne disease diagnosis from
Epic Cosmos, by state, for six diseases: Lyme disease, Babesiosis, Malaria, RMSF (Rocky
Mountain Spotted Fever), West Nile virus, and Dengue. The denominator (`epic_n_patients`)
is the total patient count for the same state/month cell.

This is the new-case sibling of `cosmos_vector_borne`, whose measures count patients with
*any* diagnosis in the month (new or ongoing). Both share the same population base and
denominator definition, but the numerators are **not interchangeable**: new cases are a
subset of the all-diagnosis counts. Output columns carry a `new_` infix so the two
sources can be combined in a bundle without name collisions.

**Population base** (SlicerDicer session `2861722`, "n new west nile and n new
babesiosis and n new malaria and n new RMSF and n new dengue and Number of Patients and
n new lyme by State of Residence"): data model `Patients`, population base `All
Patients`, criteria `Country of Care = United States of America` and `Has Any
Encounters`. Session date range 1/1/2022 - 7/28/2026 (export of 2026-09-30).

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

## Standard output

`standard/data.csv.gz`, in PopHIVE wide format:

| Column | Notes |
|---|---|
| `geography` | FIPS string; `"00"` is national (the SlicerDicer `Total` row) |
| `time` | `YYYY-mm-dd`, the **last day of the month** |
| `epic_n_new_<disease>` / `epic_pct_new_<disease>` / `epic_new_<disease>_suppressed_flag` | `<disease>` = `lyme`, `babesiosis`, `malaria`, `rmsf`, `west_nile`, `dengue` |
| `epic_n_patients` | Total patients (denominator for every `epic_pct_new_*` column) |
| `epic_n_patients_suppressed_flag` | Suppression flag for the denominator |

Each `epic_pct_new_*` measure is a **percent of all patients**, not a rate per 100,000.
There is no age or sex stratification in this session.

## Updating

1. Re-run SlicerDicer session `2861722` and export the crosstab (rows: Year, Month,
   State of Residence; measures: `n new lyme`, `n new babesiosis`, `n new malaria`,
   `n new RMSF`, `n new west nile`, `n new dengue`, `Number of Patients`).
2. Drop the export into `raw/staging/`, **replacing** the previous file. This ingest
   expects exactly one staging file.
3. From the project root: `Rscript -e "dcf::dcf_process('cosmos_vector_borne_new_cases')"`.

### Prerequisites

- `EPIC_XLSX_PASSWORD` set in `.Renviron` (`usethis::edit_r_environ()`).
- `msoffcrypto-tool` installed for the Python that R resolves via `Sys.which("python")`.

## Notes

- **Layout**: identical to `cosmos_vector_borne` (row 11 measure labels, row 12
  `Year`/`Month`/`State of Residence`, data from row 13; Year and Month are merged cells
  that are filled down). The header is located dynamically and `MEASURE_PATTERNS` in
  `ingest.R` stops the run on any unrecognized measure label. Columns may appear in any
  order (the 2026-09-30 export has Lyme last, after `Number of Patients`).
- **Dropped rows**: non-US geographies and `None of the above`, plus the trailing partial
  period (`Jul 1 - Jul 28`), with a `message()` reporting each. The last full month is
  June 2026.
- **Suppression**: a count of 10 or fewer arrives blank or as `"10 or fewer"`; it is
  imputed as 5 and the disease's flag set to 1 (flags computed before imputation). The
  flag covers both `epic_n_new_*` and `epic_pct_new_*`. New-case counts are small, so most
  state-month cells for the rarer diseases are suppressed (see the flag sums in the run
  log); prefer national or annual aggregates for those diseases.
- Where the *denominator* was suppressed, `epic_pct_new_*` is left `NA` (none in the
  current export).
- Malaria and Dengue cases in the US are mostly travel-associated; this session does not
  distinguish travel-associated from locally acquired cases.
