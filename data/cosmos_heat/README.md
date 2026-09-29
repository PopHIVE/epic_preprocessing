# cosmos_heat

Monthly heat-related illness (HRI) data from Epic Cosmos, by state of
residence: total patient count plus the percent of all patients diagnosed with a
heat-related illness of any severity (individual severity levels are not included).

**Population base** (SlicerDicer session `2860390`, "Number of Patients and All HRI
by State of Residence"): data
model `Patients`, population base `All Patients`, criteria `Country of Residence =
United States of America` and `Has Any Encounters?`. The denominator for every
percentage is the **entire** Epic patient population for that state/month, **not** a
cohort restricted to heat illness, injury, or emergency department visits.

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

## Standard output

`standard/data.csv.gz`, in PopHIVE wide format:

| Column | Notes |
|---|---|
| `geography` | FIPS string; `"00"` is national (the SlicerDicer `Total` row) |
| `time` | `YYYY-mm-dd`, the **last day of the month** |
| `epic_n_patients` | Total patients (denominator) |
| `epic_n_patients_suppressed_flag` | Suppression flag for the denominator |
| `epic_pct_hri` | Percent of patients with any heat-related illness |
| `epic_pct_hri_suppressed_flag` | Suppression/imputation flag (see Suppression below) |

No age/sex/race stratification is present in this export - the grain is
geography x month.

## Updating

1. Re-run SlicerDicer session `2860390` and export the crosstab (rows: Measures
   [`Number of Patients`, `All HRI (%)`], State of Residence; columns: Year, Month).
2. Drop the export into `raw/staging/`, **replacing** the previous file, so the same
   months are not read twice.
3. From the project root: `Rscript -e "dcf::dcf_process('cosmos_heat')"`, or while
   iterating: `setwd("data/cosmos_heat"); source("ingest.R"); setwd("../..")`.

### Prerequisites

- `EPIC_XLSX_PASSWORD` set in `.Renviron` (`usethis::edit_r_environ()`) — SlicerDicer
  xlsx exports are password protected.
- `msoffcrypto-tool` installed for the Python that R resolves via `Sys.which("python")`
  (`python -m pip install msoffcrypto-tool`).

## Notes

- **Layout is transposed** relative to most exports in this repo: rows are
  (measure block) x (state), columns are (year) x (month). Each measure occupies a
  62-row block (50 states + DC + territories + "None of the above" + "Total"), and
  the measure label only appears on the first row of its block (merged cell, filled
  down). The header row (`"Measures"` / `"State of Residence"` in columns A/B) is
  located by content, not a fixed row number - if the session changes, update
  `MEASURE_LABELS` in `ingest.R`, which `stop()`s on any unrecognized measure label.
- **Time**: the `Year` label is only populated on the first column of each year block
  (filled right/down via `zoo::na.locf`); `Month` is a plain 3-letter abbreviation per
  column. Columns whose month label isn't a full `month.abb` value (e.g. the export's
  trailing `"Sep 1"` column, a partial month) are dropped, with a `message()` naming
  them. The script `stop()`s if every column looks partial.
- **Dropped rows**: non-US geographies (territories, armed forces, "None of the
  above") are dropped, with a `message()` reporting the labels.
- **Suppression and imputation** - three distinct cases, all folded into one flag per
  percent measure:
  1. **Suppressed numerator**: Epic never emits an explicit `0%`; a blank cell means
     10 or fewer patients had an HRI diagnosis. Imputed as
     `5 / epic_n_patients * 100`, flag `1`.
  2. **Bounded numerator**: Epic reports the literal string `"<0.01%"` when the count
     is above 10 but the percentage still rounds under 0.01% (common here, because the
     denominator is the entire patient population, not a heat-illness cohort).
     Imputed at half the bound, `0.005`, flag `1`.
  3. **Suppressed denominator**: if `epic_n_patients` itself were suppressed (imputed
     to 5), the percent is left `NA` rather than asserting a meaningless `5/5*100`,
     following the `cosmos_gas`/`cosmos_vaccines` precedent. In practice this never
     happens for the 50 states/DC/national total in this export - only the
     already-dropped territory rows ever hit the count-suppression threshold.
  Because the denominator is the full "All Patients" cohort and heat illness is rare,
  **the large majority of percent cells are suppressed or bounded** (case 1 or 2) -
  this is expected, not a data quality problem. `epic_n_patients` itself is almost
  never suppressed for any retained geography.
  Flags are computed before imputation, so they record what Epic withheld/bounded
  rather than what the ingest wrote.
- The script validates geography (52 = 50 states + DC + national), time format,
  duplicate index rows, percentage range, and the flag/NA invariants before writing,
  and reports per-measure imputation counts via `message()`.
