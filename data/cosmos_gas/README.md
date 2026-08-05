# cosmos_gas

Group A Streptococcus (GAS) patient counts from Epic Cosmos, by state, age group,
and calendar quarter. The numerator counts patients with a strep throat diagnosis
(ICD-10 `J02.0` streptococcal pharyngitis, `J03.00` acute streptococcal tonsillitis
unspecified, `J03.01` acute recurrent streptococcal tonsillitis); the denominator is
the total patient count for the same state/quarter/age cell.

**Population base** (SlicerDicer session `2809857`, "Number of Patients by State of
Residence and Diagnosis (All) and Age at Encounter in Years Range"): data model
`Patients`, population base `All Patients`, criteria `Country of Care = United States
of America` and `Has Any Encounters`. This is **not** restricted to emergency
department visits - the denominator is all patients with any encounter.

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

## Standard output

`standard/data.csv.gz`, in PopHIVE wide format:

| Column | Notes |
|---|---|
| `geography` | FIPS string; `"00"` is national (the SlicerDicer `Total` row) |
| `time` | `YYYY-mm-dd`, the **last day of the quarter** (e.g. `2025-03-31`) |
| `age` | `<1 Years`, `1-4 Years`, `5-17 Years`, `18-49 Years`, `50-64 Years`, `65+ Years`, `Total` |
| `epic_n_strep_throat` | Count of patients with a strep throat diagnosis |
| `epic_pct_strep_throat` | **Percent** of patients, `n / denominator * 100` |
| `epic_strep_throat_suppressed_flag` | Suppression flag for the numerator — covers **both** measures above |
| `epic_n_patients` | Total patients (denominator) |
| `epic_n_patients_suppressed_flag` | Suppression flag for the denominator |

The measure is a percentage, not a rate per 100,000. The raw `State of Residence`
label is used only to resolve `geography` and is dropped from the output.

## Updating

1. Re-run SlicerDicer session `2809857` and export the crosstab (rows: Year, Quarter,
   State of Residence, Age at Encounter in Years Range; columns: Diagnosis (All) ->
   `strep throat` and `Total`; measure: Number of Patients).
2. Drop the export into `raw/staging/`, **replacing** the previous file, so the same
   quarters are not read twice.
3. From the project root: `Rscript -e "dcf::dcf_process('cosmos_gas')"`, or while
   iterating: `setwd("data/cosmos_gas"); source("ingest.R"); setwd("../..")`.

### Prerequisites

- `EPIC_XLSX_PASSWORD` set in `.Renviron` (`usethis::edit_r_environ()`) — SlicerDicer
  xlsx exports are password protected.
- `msoffcrypto-tool` installed for the Python that R resolves via `Sys.which("python")`
  (`python -m pip install msoffcrypto-tool`).

## Notes

- **Layout**: row 13 holds the stratification header (Year, Quarter, State of
  Residence, Age at Encounter in Years) and row 12 the column-group labels
  (`strep throat`, `Total`); data rows start at row 14. Grouping columns are merged
  cells and are filled downward. If the session adds
  or reorders stratifications, update `base_names` in `read_gas_staging()`.
- **Time**: quarter labels (`Jan 1 - Mar 31`, with an en dash U+2013 as the
  separator) are parsed by `parse_quarter_range()`. Range detection counts month
  tokens rather than matching the dash, because a non-ASCII regex pattern fails
  outright under a C locale. Quarters only partially covered by the exported date
  range are dropped, with a `message()` naming them - the 2026-04-01 export ends
  `3/3/2026`, so its trailing `Jan 1 - Mar 3` bucket is dropped rather than passed
  off as a full Q1. The script `stop()`s if every quarter looks partial, which means
  the label format changed.
- **Dropped rows**: non-US geographies (Canadian provinces, Mexican states,
  territories) and `None of the above` are dropped, with a `message()` reporting the
  count and labels.
- **Suppression**: a count of 10 or fewer arrives as `"10 or fewer"` or as a blank
  cell; it is imputed as 5 and the corresponding flag set to 1. Flags are computed
  before imputation, so they record what Epic withheld rather than what the ingest
  wrote. There are two independently suppressible cells per row, so two flags:
  `epic_strep_throat_suppressed_flag` for the numerator (covering
  `epic_n_strep_throat` **and** `epic_pct_strep_throat`, since the percent is derived
  from that same cell) and `epic_n_patients_suppressed_flag` for the denominator. No
  separate percent flag is emitted — it would be exactly the OR of those two, so take
  the OR if you need "was the percentage affected at all?".
- Where the *denominator* was suppressed, `epic_pct_strep_throat` is left `NA` rather
  than the meaningless `5 / 5 * 100`, following the `cosmos_vaccines` precedent. The
  script asserts that the percent is missing exactly where the denominator flag is 1.
- The script validates geography, time, duplicate index rows, percentage range, and
  the flag invariants before writing, and reports per-measure imputation counts.
