# cosmos_vector_borne

Monthly patient counts with a vector-borne disease diagnosis from Epic Cosmos, by state,
for six diseases: Lyme disease, Babesiosis, Malaria, RMSF (Rocky Mountain Spotted
Fever), West Nile virus, and Dengue. Each disease's numerator is the number of patients
with that diagnosis; the denominator (`epic_n_patients`) is the total patient count for
the same state/month cell.

**Population base** (SlicerDicer session `2852629`, "Lyme N and Babesiosis N and
Malaria N and RMSF N and West Nile N and Dengue N and Number of Patients by State of
Residence"): data model `Patients`, population base `All Patients`, criteria
`Country of Care = United States of America`. This is **not** restricted to any
diagnosis, encounter type, or emergency department visits - the denominator is all
patients.

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

## Standard output

`standard/data.csv.gz`, in PopHIVE wide format:

| Column | Notes |
|---|---|
| `geography` | FIPS string; `"00"` is national (the SlicerDicer `Total` row) |
| `time` | `YYYY-mm-dd`, the **last day of the month** |
| `epic_n_lyme` / `epic_pct_lyme` / `epic_lyme_suppressed_flag` | Lyme disease |
| `epic_n_babesiosis` / `epic_pct_babesiosis` / `epic_babesiosis_suppressed_flag` | Babesiosis |
| `epic_n_malaria` / `epic_pct_malaria` / `epic_malaria_suppressed_flag` | Malaria |
| `epic_n_rmsf` / `epic_pct_rmsf` / `epic_rmsf_suppressed_flag` | Rocky Mountain Spotted Fever |
| `epic_n_west_nile` / `epic_pct_west_nile` / `epic_west_nile_suppressed_flag` | West Nile virus |
| `epic_n_dengue` / `epic_pct_dengue` / `epic_dengue_suppressed_flag` | Dengue |
| `epic_n_patients` | Total patients (denominator for every `epic_pct_*` column) |
| `epic_n_patients_suppressed_flag` | Suppression flag for the denominator |

Each `epic_pct_*` measure is a **percent of all patients**, not a rate per 100,000. The
raw `State of Residence` label is used only to resolve `geography` and is dropped from
the output. There is no age or sex stratification in this session.

## Updating

1. Re-run SlicerDicer session `2852629` and export the crosstab (rows: Year, Month,
   State of Residence; measures: Lyme N, Babesiosis N, Malaria N, RMSF N, West Nile N,
   Dengue N, Number of Patients).
2. Drop the export into `raw/staging/`, **replacing** the previous file, so the same
   months are not read twice. This ingest expects exactly one staging file.
3. From the project root: `Rscript -e "dcf::dcf_process('cosmos_vector_borne')"`, or
   while iterating: `setwd("data/cosmos_vector_borne"); source("ingest.R"); setwd("../..")`.

### Prerequisites

- `EPIC_XLSX_PASSWORD` set in `.Renviron` (`usethis::edit_r_environ()`) — SlicerDicer
  xlsx exports are password protected.
- `msoffcrypto-tool` installed for the Python that R resolves via `Sys.which("python")`
  (`python -m pip install msoffcrypto-tool`).

## Notes

- **Layout**: row 11 holds the measure labels for the value columns (`Lyme N`,
  `Babesiosis N`, `Malaria N`, `RMSF N`, `West Nile N`, `Dengue N`,
  `Number of Patients`) and row 12 the row-dimension labels (`Year`, `Month`,
  `State of Residence`). Data starts at row 13, ordered Year > Month > State (State
  changes fastest). Year and Month are merged cells and must be filled down; State of
  Residence is present on every row. The ingest locates the header dynamically (the row
  where column C equals `"State of Residence"`) rather than hardcoding a row number, and
  `stop()`s if columns A/B aren't `Year`/`Month` on that row, or if any measure column
  label doesn't match `MEASURE_PATTERNS` in `ingest.R` - extend that map if the session
  renames or adds a disease.
- **Dropped rows**: non-US geographies (Canadian provinces, Mexican states, territories,
  e.g. Aguascalientes, Alberta, Manitoba, Nova Scotia, Ontario, Puerto Rico, Quebec,
  Virgin Islands, Armed Forces Africa) and `None of the above` are dropped, with a
  `message()` reporting the count and labels. The trailing partial period at the end of
  the exported range (e.g. `Jul 1 – Jul 28`) is dropped as well, since it isn't a full
  month.
- **Suppression**: a count of 10 or fewer arrives as a blank cell (most common in this
  export) or as the literal string `"10 or fewer"`; it is imputed as 5 and the
  corresponding flag set to 1. Flags are computed before imputation, so they record what
  Epic withheld rather than what the ingest wrote. There is one flag per disease
  (covering both its `epic_n_*` and `epic_pct_*` columns, since the percent is derived
  from that same numerator cell) plus `epic_n_patients_suppressed_flag` for the
  denominator.
- Where the *denominator* was suppressed, every `epic_pct_*` column is left `NA` rather
  than the meaningless `5 / 5 * 100`, following the `cosmos_gas`/`cosmos_concussions`
  precedent. The script asserts that each percent column is missing exactly where the
  denominator flag is 1. In the 2026-08-28 export the denominator was never suppressed
  nationally or by state (it is a large aggregate count), so no percent cells are
  currently missing for that reason.
- Malaria and Dengue counts in the US are mostly travel-associated rather than locally
  acquired; this session does not distinguish travel-associated from locally acquired
  cases.
- The script validates geography, time, duplicate index rows, percentage range, and the
  flag invariants before writing, and reports per-measure imputation counts.
- This ingest expects a **single** staging file and `stop()`s if more than one is
  present, since the export already covers the full history in one file. Extend
  `ingest.R` if a future update needs to combine multiple exports.
