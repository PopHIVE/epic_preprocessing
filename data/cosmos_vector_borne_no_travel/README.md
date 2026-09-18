# cosmos_vector_borne_no_travel

Monthly patient counts with a vector-borne disease diagnosis from Epic Cosmos, by state,
for the same six diseases as [`cosmos_vector_borne`](../cosmos_vector_borne): Lyme
disease, Babesiosis, Malaria, RMSF (Rocky Mountain Spotted Fever), West Nile virus, and
Dengue - but restricted to patients **without a travel history**. This source exists to
be compared against `cosmos_vector_borne` to see how much of the signal for each disease
(especially Malaria and Dengue, which are mostly travel-associated in the US) is
travel-related versus locally acquired.

**Population base** (SlicerDicer session `2853292`, "n babesiosis and n malaria and n
RMSF and n west nile and n dengue and Number of Patients and n lyme by State of
Residence"): data model `Patients`, population base `All Patients`, criteria
`Country of Care = United States of America`, `Not Travel History` (New User Grouper 1),
`Not Diagnosis (All): Travel abroad`, and `Has Any Encounters`.

**This is a different population base from `cosmos_vector_borne`**, not just a travel
exclusion: it also restricts to patients with any encounter. That is why this is a
separate source rather than a refreshed export of `cosmos_vector_borne` - see the
ingest-source skill's Path A criteria ("different population base" -> new source).

This source previously existed (through the 2026-08-28 export) as a **national-only**
export with no state stratification, and was removed from the repo on 2026-08-31. The
2026-09-11 export reintroduces it with a full state breakdown, matching
`cosmos_vector_borne`'s layout, so this ingest is modeled on that source.

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
| `epic_n_patients` | Total patients meeting this session's population base (denominator for every `epic_pct_*` column) |
| `epic_n_patients_suppressed_flag` | Suppression flag for the denominator |

Each `epic_pct_*` measure is a **percent of patients meeting this session's population
base** (not travel history, has any encounters), not a rate per 100,000, and **not
directly comparable to `cosmos_vector_borne`'s percentages** because the denominator
criteria differ. The `epic_n_*` counts are the more meaningful comparison across the two
sources.

## Updating

1. Re-run SlicerDicer session `2853292` and export the crosstab (rows: Year, Month,
   State of Residence; measures: n lyme, n babesiosis, n malaria, n RMSF, n west nile,
   n dengue, Number of Patients).
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

- **Layout**: row 11 holds the measure labels for the value columns and row 12 the
  row-dimension labels (`Year`, `Month`, `State of Residence`). Data starts at row 13,
  ordered Year > Month > State (State changes fastest). Year and Month are merged cells
  and must be filled down; State of Residence is present on every row. The ingest
  locates the header dynamically (the row where column C equals `"State of Residence"`)
  rather than hardcoding a row number, and `stop()`s if columns A/B aren't `Year`/`Month`
  on that row, or if any measure column label doesn't match `MEASURE_PATTERNS` in
  `ingest.R` - extend that map if the session renames or adds a disease.
  `MEASURE_PATTERNS` matches both the lowercase `n <disease>` form (used by the current
  export) and the older `<Disease> N` form, for consistency with `cosmos_vector_borne`.
- **Dropped rows**: non-US geographies (Canadian provinces, Mexican states, territories)
  and `None of the above` are dropped, with a `message()` reporting the count and
  labels. The trailing partial period at the end of the exported range (e.g.
  `Jul 1 – Jul 28`) is dropped as well, since it isn't a full month.
- **Suppression**: a count of 10 or fewer arrives as a blank cell or as the literal
  string `"10 or fewer"`; it is imputed as 5 and the corresponding flag set to 1. Flags
  are computed before imputation, so they record what Epic withheld rather than what the
  ingest wrote. There is one flag per disease (covering both its `epic_n_*` and
  `epic_pct_*` columns, since the percent is derived from that same numerator cell) plus
  `epic_n_patients_suppressed_flag` for the denominator.
- Where the *denominator* was suppressed, every `epic_pct_*` column is left `NA` rather
  than the meaningless `5 / 5 * 100`, following the `cosmos_vector_borne` precedent. The
  script asserts that each percent column is missing exactly where the denominator flag
  is 1.
- Malaria and Dengue counts in the US are mostly travel-associated rather than locally
  acquired; comparing `epic_n_malaria` / `epic_n_dengue` here against the same columns in
  `cosmos_vector_borne` gives a sense of the locally-acquired share.
- The script validates geography, time, duplicate index rows, percentage range, and the
  flag invariants before writing, and reports per-measure imputation counts.
- This ingest expects a **single** staging file and `stop()`s if more than one is
  present, since the export already covers the full history in one file.
- **Counterintuitive comparison note**: in the 2026-09-11 export, every disease count
  and `epic_n_patients` here are slightly *higher* than the corresponding values in
  `cosmos_vector_borne` for the same months (e.g. national dengue: 1,227 here vs. 1,205
  there; `epic_n_patients`: ~74.56M here vs. ~73.80M there, for the most recent month in
  both). This is NOT because the no-travel-history population is somehow larger than
  "all patients" - it is expected for two reasons: (1) `Not Travel History` most likely
  means "no travel history documented," not "confirmed did not travel" - most patients
  are never asked about travel, so nearly everyone falls into this bucket by default,
  making it close to (not meaningfully smaller than) the full population; and (2) the two
  sources' exports were pulled ~2 weeks apart (`cosmos_vector_borne` 2026-08-31 vs. this
  source 2026-09-11), and Epic Cosmos aggregates can shift slightly between pulls as the
  underlying EHR data continues to backfill. Do not treat the count difference as a
  measure of travel-associated cases; treat within-source trends (this source's own
  time series) as the reliable signal, and compare shapes/timing across sources rather
  than absolute levels.
