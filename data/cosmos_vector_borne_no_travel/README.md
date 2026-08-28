# cosmos_vector_borne_no_travel

Monthly, **national-only** patient counts with a vector-borne disease diagnosis from Epic
Cosmos, for the same six diseases as [`cosmos_vector_borne`](../cosmos_vector_borne):
Lyme disease, Babesiosis, Malaria, RMSF (Rocky Mountain Spotted Fever), West Nile virus,
and Dengue - but restricted to patients **without a travel history**. This source exists
to be compared against `cosmos_vector_borne` to see how much of the signal for each
disease (especially Malaria and Dengue, which are mostly travel-associated in the US) is
travel-related versus locally acquired.

**Population base** (SlicerDicer session `2852825`, "n babesiosis and n malaria and n
RMSF and n west nile and n dengue and Number of Patients and n lyme"): data model
`Patients`, population base `All Patients`, criteria `Country of Care = United States of
America`, `Not Travel History` (New User Grouper 1), and `Has Any Encounters`.

**This is a different population base from `cosmos_vector_borne`**, not just a travel
exclusion: it also restricts to patients with any encounter, and it has **no state
stratification** (national only, vs. state + national for `cosmos_vector_borne`). That is
why this is a separate source rather than a refreshed export of `cosmos_vector_borne` -
see the ingest-source skill's Path A criteria ("different population base" -> new
source).

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

## Standard output

`standard/data.csv.gz`, in PopHIVE wide format:

| Column | Notes |
|---|---|
| `geography` | Always `"00"` (national) - this export has no state breakdown |
| `time` | `YYYY-mm-dd`, the **last day of the month** |
| `epic_n_lyme` / `epic_pct_lyme` / `epic_lyme_suppressed_flag` | Lyme disease |
| `epic_n_babesiosis` / `epic_pct_babesiosis` / `epic_babesiosis_suppressed_flag` | Babesiosis |
| `epic_n_malaria` / `epic_pct_malaria` / `epic_malaria_suppressed_flag` | Malaria |
| `epic_n_rmsf` / `epic_pct_rmsf` / `epic_rmsf_suppressed_flag` | Rocky Mountain Spotted Fever |
| `epic_n_west_nile` / `epic_pct_west_nile` / `epic_west_nile_suppressed_flag` | West Nile virus |
| `epic_n_dengue` / `epic_pct_dengue` / `epic_dengue_suppressed_flag` | Dengue |
| `epic_n_patients` | Total patients meeting this session's population base (denominator for every `epic_pct_*` column) |
| `epic_n_patients_suppressed_flag` | Suppression flag for the denominator |

Each `epic_pct_*` measure is a **percent of patients meeting this session's population
base** (not travel history, has any encounters), not a rate per 100,000, and **not
directly comparable to `cosmos_vector_borne`'s percentages** because the denominator
criteria differ. The `epic_n_*` counts are the more meaningful comparison across the two
sources.

## Updating

1. Re-run SlicerDicer session `2852825` and export the crosstab (rows: Year, Month;
   measures: n lyme, n babesiosis, n malaria, n RMSF, n west nile, n dengue, Number of
   Patients).
2. Drop the export into `raw/staging/`, **replacing** the previous file, so the same
   months are not read twice. This ingest expects exactly one staging file.
3. From the project root: `Rscript -e "dcf::dcf_process('cosmos_vector_borne_no_travel')"`,
   or while iterating: `setwd("data/cosmos_vector_borne_no_travel"); source("ingest.R"); setwd("../..")`.

### Prerequisites

- `EPIC_XLSX_PASSWORD` set in `.Renviron` (`usethis::edit_r_environ()`) - SlicerDicer
  xlsx exports are password protected.
- `msoffcrypto-tool` installed for the Python that R resolves via `Sys.which("python")`
  (`python -m pip install msoffcrypto-tool`).

## Notes

- **Layout**: row 11 holds the measure labels for the value columns (`n babesiosis`,
  `n malaria`, `n RMSF`, `n west nile`, `n dengue`, `Number of Patients`, `n lyme` - note
  the different order and lowercase labels compared to `cosmos_vector_borne`'s session)
  and row 12 the row-dimension labels (`Year`, `Month` only - **no** `State of
  Residence` column). Data starts at row 13, ordered Year > Month (Year changes
  slowest). Year is a merged cell and must be filled down; Month is present on every
  row. The ingest locates the header dynamically (the row where columns A/B equal
  `"Year"`/`"Month"`) and `stop()`s if a third stratification column is present
  (that would mean the session gained a geography breakdown, which is a structure
  change - see `cosmos_vector_borne` for that pattern instead) or if any measure column
  label doesn't match `MEASURE_PATTERNS` in `ingest.R` - extend that map if the session
  renames or adds a disease.
- **Dropped rows**: the trailing partial period at the end of the exported range (e.g.
  `Jul 1 – Jul 28`) is dropped, since it isn't a full month.
- **Suppression**: a count of 10 or fewer would arrive as a blank cell or as the literal
  string `"10 or fewer"`; it is imputed as 5 and the corresponding flag set to 1. In the
  2026-08-28 export, no cell was suppressed at the national level (all counts are large
  monthly aggregates), so all suppression flags are currently 0.
- Where the *denominator* was suppressed, every `epic_pct_*` column is left `NA` rather
  than the meaningless `5 / 5 * 100`, following the `cosmos_vector_borne` precedent.
- Malaria and Dengue counts in the US are mostly travel-associated rather than locally
  acquired; this source's exclusion of travel history is intended to let you see the
  locally-acquired share by comparing `epic_n_malaria` / `epic_n_dengue` here against the
  same columns in `cosmos_vector_borne`.
- The script validates geography (always `"00"`), time, duplicate index rows, percentage
  range, and the flag invariants before writing, and reports per-measure imputation
  counts.
- This ingest expects a **single** staging file and `stop()`s if more than one is
  present, since the export already covers the full history in one file.
