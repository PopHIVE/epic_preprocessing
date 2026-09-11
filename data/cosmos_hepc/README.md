# cosmos_hepc

Epic Cosmos annual counts related to **hepatitis C**, from two SlicerDicer sessions merged
into one standard file, keyed by `(geography, time, age)`:

1. **Hepatitis C diagnosis counts** (`raw/staging/`) -- patients with a viral hepatitis C
   diagnosis, alongside the all-patients denominator, stratified by state of residence and
   age at encounter. The population base is *All Patients* with `Has Any Encounters?` and
   `Country of Care = United States of America`. No diagnosis filter is applied to the
   population itself -- the diagnosis restriction is a **row dimension**, so each
   state/year/age cell reports both the hepatitis C bucket and a `Total` row covering all
   patients. As of the 2026-09-10 update this export also carries two extra per-age-group
   measure columns that are deliberately NOT used (see caveats).
2. **HCV medication measure** (`raw/staging_medication/`) -- among ALL patients with a viral
   hepatitis C diagnosis (the same diagnosis criteria as crosstab 1, applied here as a
   **session-level population filter**), the percent who received an HCV medication within
   6 months of diagnosis, stratified by state of residence and age at encounter. As of the
   2026-09-10 update there is no separate `All Medications` row dimension -- the population
   itself IS the HCV-diagnosed population, confirmed to match the diagnosis crosstab's
   `epic_n_patients_hepc` cell-for-cell (see caveats). This is a substantive change from the
   predecessor session, whose population was instead a smaller, already-medicated subgroup.

Output: `standard/data.csv.gz`

| Column | Description |
|---|---|
| `geography` | FIPS string -- `"00"` national, 2-digit state |
| `time` | Period end date. `YYYY-12-31` for a whole year; each crosstab's trailing **partial** year keeps its own real end date -- currently `2026-08-18` for the diagnosis crosstab, `2026-07-28` for the medication crosstab (they do NOT align as of this update) |
| `age` | `<5 Years`, `5-17 Years`, `18-24 Years`, `25-44 Years`, `45-64 Years`, `65+ Years`, `Total` |
| `epic_n_patients_hepc` | Patients with a viral hepatitis C diagnosis |
| `epic_n_patients_hepc_suppressed_flag` | 1 if suppressed and imputed as 5 |
| `epic_n_patients` | All patients with any encounter (denominator for `epic_pct_hepc`) |
| `epic_n_patients_suppressed_flag` | 1 if suppressed and imputed as 5 |
| `epic_pct_hepc` | `epic_n_patients_hepc / epic_n_patients * 100` |
| `epic_pct_hepc_suppressed_flag` | 1 if either numerator or denominator was suppressed |
| `epic_n_patients_hepc_medication_pop` | ALL patients with a viral hepatitis C diagnosis (denominator for `epic_pct_hepc_medication`) -- confirmed to match `epic_n_patients_hepc` cell-for-cell; see caveats |
| `epic_n_patients_hepc_medication_pop_suppressed_flag` | 1 if suppressed and imputed as 5 |
| `epic_pct_hepc_medication` | The source's own rate: percent who received an HCV medication within 6 months of diagnosis, taken directly (imputed via `5 / denominator * 100` when blank) |
| `epic_pct_hepc_medication_suppressed_flag` | 1 if the underlying rate was suppressed |
| `epic_n_patients_hepc_medication` | **DERIVED**, not source-reported: `round(epic_n_patients_hepc_medication_pop * epic_pct_hepc_medication / 100)` -- patients medicated within 6 months of diagnosis |
| `epic_n_patients_hepc_medication_suppressed_flag` | 1 if the underlying rate was suppressed (identical to `epic_pct_hepc_medication_suppressed_flag`) |

Because the two crosstabs cover different time ranges and are separately exported sessions,
rows are merged with a **full join** on `(geography, time, age)`: a cell that exists in only
one crosstab gets `NA` for the other crosstab's columns rather than being dropped or
truncated to a shared date range.

## Updating

**SlicerDicer session IDs:**
- Diagnosis crosstab: `2855468` (predecessors: `2850163`, `2850139`, `2847109`, `2847096`)
- Medication crosstab: `2855386` (predecessor: `2850012`)

### Diagnosis crosstab (`raw/staging/`)

Rows, in this order:

1. `Year`
2. `State of Residence`
3. `Diagnosis (All)` -- the `hepc` bucket (bucket text as of this session: `Hep C
   diagnosis`; the predecessor session used the literal text `hepc` -- both are recognized
   by `DX_LABELS`), **with the Total row enabled** (this is the denominator; the ingest
   stops with an error if it is missing)

Columns: `Age at Encounter in Years`, bucketed as Less than 5 / 5-18 / 18-25 / 25-45 /
45-65 / 65 or more, **with the Total column enabled** (this produces `age = "Total"` rows).
As of this update, each age group carries **three** measure columns under a "Measures"
header (`Number of Patients`, `HepC diagnosis (%)`, and a medication-within-6-months
percentage) rather than one -- only `Number of Patients` is extracted; see the caveat below
and the CAVEAT comment in `ingest.R`.

**Note:** session `2855468`'s date range is `1/1/2018 - 8/18/2026`, restoring the 2018-2019
history that the intermediate `2850163` session lacked while keeping the age Total column --
the earlier "2018/2019 NA" gap documented in prior updates is now resolved.

### Medication crosstab (`raw/staging_medication/`)

As of this update, this crosstab's layout is **three** row dimensions (matching the
diagnosis crosstab's shape more closely than the predecessor session did) with two *named*
measure columns rather than a bucket dimension:

Rows, in this order:

1. `Year`
2. `State of Residence`
3. `Age at Encounter in Years`, bucketed the same way as the diagnosis crosstab, **with the
   Total row enabled**

Measures (columns): `Number of Patients` and `Hep C medication (%)` (the predecessor
session used the label `hcv medication measure (%)` -- both are recognized by
`MED_PCT_LABELS`). The population filter -- viral hepatitis C diagnosis, same definition as
the diagnosis crosstab's `hepc` bucket -- is now applied at the **session/population
level**, so `Number of Patients` IS the HCV-diagnosed population directly (no separate
`All Medications` bucket dimension exists in this session). The source does NOT report a
count of patients meeting the measure directly, so the ingest derives it as
`round(population * rate / 100)`.

**Note -- population definition changed, values are NOT comparable to the predecessor
session:** as of this update, `Number of Patients` was CONFIRMED (cell-for-cell, e.g. Texas
/ 2018 / 18-24 Years = 229 in both files) to equal the diagnosis crosstab's
`epic_n_patients_hepc`. The predecessor session (`2850012`) instead scoped this column to a
smaller, already-medicated subgroup via an `All Medications` bucket dimension, producing a
much higher rate (77-84 percent) than this session's rate among all diagnosed patients
(14.5-17.5 percent nationally). Do not chart the two as a continuous series. When
re-exporting this session in the future, re-verify with a cell-for-cell spot check against
the diagnosis crosstab that the population is still "all HCV-diagnosed patients" and not a
narrower bucket, since that distinction is not obvious from the column labels alone.

### Then

1. Export both sessions from SlicerDicer and drop the `.xlsx` files into `raw/staging/` and
   `raw/staging_medication/` respectively, **replacing** the previous export in each folder
   (the ingest binds every file in each folder, so leaving an old one in place would
   double-count overlapping years).
2. Run the ingest:
   ```r
   setwd("data/cosmos_hepc"); source("ingest.R"); setwd("../..")
   ```
   or, from the project root, `dcf::dcf_process("cosmos_hepc")`.

The ingest reprocesses only when the set of staging files or their md5 hashes changes in
*either* folder; an unchanged pair of folders is a no-op. If only the ingest LOGIC changed
(not the staging files), clear `raw_state` / `medication_raw_state` to `null` in
`process.json` first to force a reprocess with the same files -- `dcf` will regenerate both
fields with correct hashes on a successful run.

### Prerequisites

- `EPIC_XLSX_PASSWORD` in `.Renviron` (`usethis::edit_r_environ()`) -- SlicerDicer `.xlsx`
  exports are password protected.
- `msoffcrypto-tool` installed for the Python that R invokes:
  `python -m pip install msoffcrypto-tool`. The script checks for it and stops with the
  install command if absent.

### When a session changes

The layout is resolved from each export's own header rows, not from hard-coded column
positions, and anything unrecognized **stops the run** rather than landing on the wrong
column. Extend the constants at the top of `ingest.R`:

| Constant | Covers |
|---|---|
| `DIM_LABELS` / `DIM_LABELS_MED` | Row index dimensions and their order, per crosstab |
| `COL_AXIS_LABEL` | The column axis label (diagnosis crosstab only) |
| `AGE_EXPECTED` / `standardize_age_label()` | Age bucket labels and their standard form (shared by both crosstabs) |
| `DX_LABELS` | Diagnosis bucket text -> measure column suffix (diagnosis crosstab; matched case-insensitively; currently recognizes both `hepc` and `Hep C diagnosis`) |
| `MED_PCT_LABELS` | Recognized labels for the medication crosstab's rate column (currently `Hep C medication (%)` and the predecessor's `hcv medication measure (%)`) |
| `DENOM_COL` / `MED_NUM_COL` / `MED_DENOM_COL` | Denominator/numerator column names per crosstab |
| `MEASURE_PATTERN` | The measure name used both to pick "Number of Patients" out of the diagnosis crosstab's per-age-group measure triplet, and (where present) to assert the metadata block |

The two crosstabs use **different parsing functions** because their layouts differ in kind,
not just in labels: `extract_staging_data()` handles the diagnosis crosstab's shape (3 row
dimensions + a column axis, one or more measures per axis bucket -- only the one matching
`MEASURE_PATTERN` is kept); `extract_medication_data()` handles the medication crosstab's
shape (3 row dimensions + 2 named measure columns under a "Measures" header). Adding a
second diagnosis bucket to the diagnosis crosstab needs only a `DX_LABELS` entry -- the
script generates its count column, its percentage, and both flags automatically. See
sections "4a"/"4b" in `ingest.R` for the downstream numerator/denominator/percent pattern to
follow for a new measure. Add the corresponding `measure_info.json` entries by hand in all
cases.

## Notes and caveats

- **The numerator's code set contains an error, in BOTH crosstabs (as of the predecessor
  sessions; not re-verified against the current ones).** Session `2850012`'s own Population
  Criteria Filters read: `Chronic viral hepatitis C (ICD-10-CM: B18.2)`,
  `Acute hepatitis E (ICD-10-CM: B17.2)`, `Unspecified viral hepatitis C`. `B17.2` is **not**
  a hepatitis C code. This matched the predecessor session (`2847096`) used to define the
  diagnosis crosstab's `hepc` bucket, so both `epic_n_patients_hepc` and
  `epic_n_patients_hepc_medication_pop` likely still include a small number of acute
  hepatitis E patients. Correct the bucket/population-filter definition in SlicerDicer and
  re-export both sessions to remove it.
- **`epic_n_patients_hepc_medication_pop` now means "all HCV-diagnosed patients"** -- this
  changed as of the 2026-09-10 update. Previously (session `2850012`) it was a smaller
  `has HCV medication` bucket's population; the current session (`2855386`) applies the
  diagnosis filter to the whole population instead, and this column was CONFIRMED to match
  `epic_n_patients_hepc` cell-for-cell on every `(geography, time, age)` checked. Re-verify
  this cell-for-cell agreement after each future update rather than assuming it holds, since
  the two sessions are exported independently and nothing in the column labels themselves
  guarantees it.
- **`epic_pct_hepc_medication`'s definition changed as of the 2026-09-10 update, and is NOT
  comparable to the predecessor session's values.** It is now an explicit, source-stated
  measure: percent of ALL HCV-diagnosed patients with 1 slice by a named set of HCV
  antiviral medications (ledipasvir/sofosbuvir, sofosbuvir, elbasvir/grazoprevir,
  sofosbuvir/velpatasvir/voxilaprevir, glecaprevir/pibrentasvir, or sofosbuvir/velpatasvir)
  within 6 months of diagnosis -- i.e. a treatment-initiation rate. The predecessor session's
  rate (nationally 77-84 percent, of unconfirmed clinical meaning) was computed among an
  already-medicated subgroup; the current rate (nationally 14.5-17.5 percent, 2018-2025,
  trending upward) is computed among all diagnosed patients. Do not chart the two as a
  continuous series across this update.
- **`epic_n_patients_hepc_medication` is derived, not source-reported.** The source gives the
  population and its own rate, but never a numerator count, so the ingest computes
  `round(population * rate / 100)`. Treat this column with the same caution as any other
  back-calculated count: rounding means it will not always reconcile exactly with the rate
  to the last decimal.
- **Use the `Total` age row, don't sum the buckets.** `age = "Total"` is the source's own
  overall-age total and *includes* patients whose age at encounter is unknown, so it runs
  slightly above the sum of the six buckets. Both crosstabs have the Total age row enabled.
- **The diagnosis crosstab's history now starts at 2018.** As of the 2026-09-10 update
  (session `2855468`), the series covers 2018 onward with the age Total column throughout --
  the earlier "2018/2019 NA" gap from the intermediate `2850163` session is resolved.
- **Two extra measure columns in the diagnosis crosstab export are deliberately unused.** As
  of the 2026-09-10 export, each age group carries `HepC diagnosis (%)` and a
  medication-within-6-months percentage in addition to `Number of Patients`. The former
  behaves inconsistently across the two `Diagnosis (All)` row buckets and its precise
  definition was not confirmed; the latter was confirmed to duplicate
  `epic_pct_hepc_medication` (from the medication crosstab) exactly. Both are left out of the
  standard output -- see the CAVEAT comment at the top of `ingest.R` for the full analysis.
- **The last period may be partial, and is kept -- independently per crosstab.** `time` is
  the period *end* date, so a whole year is `YYYY-12-31` while a trailing partial period
  keeps its true end. As of this update: `2026-08-18` for the diagnosis crosstab (Jan 1 -
  Aug 18), `2026-07-28` for the medication crosstab (Jan 1 - Jul 28) -- they do NOT align.
  That off-Dec-31 date is the signal; there is no separate indicator column. The ingest
  validates, separately per crosstab, that at most one partial period exists and that it is
  the most recent, and reports it via `message()`.

  **Counts for a partial period are not comparable to a full year** -- use the percentage
  measure for any trend that spans a partial period, since the derived count is itself
  downstream of the (window-limited) rate.
- **Dropped rows**, reported via `message()` at run time, in both crosstabs:
  - `None of the above` (unknown state of residence) and non-state geographies
    (`Puerto Rico`, other territories).
- **Suppression.** For the medication measure, a blank rate can mean either that the
  population itself was 10 or fewer (denominator suppressed, Rule 1), or that the population
  was shown but the count meeting the measure was small enough to withhold on its own (Rule
  2) -- confirmed against the predecessor session's 2026-08-18 export: every
  suppressed-population row had a blank rate, and a further 60 rows had a normal population
  but a blank rate; no row had a suppressed population with a rate shown. Not independently
  re-verified against the current session, but the same Epic Cosmos suppression convention
  is expected to apply. Where the denominator *is* suppressed, the rate is left `NA` rather
  than asserting the meaningless `5 / 5 = 100%`, while its flag stays 1.
- **Interpretation.** `epic_pct_hepc`/`epic_n_patients_hepc` describe diagnosed hepatitis C
  among people who sought care, not population prevalence, sensitive to testing patterns and
  Epic's coverage in each state. `epic_pct_hepc_medication` describes the percent of all
  HCV-diagnosed patients who initiated a named HCV medication within 6 months of diagnosis
  (see above) -- a genuine among-all-diagnosed treatment-initiation rate as of this update.
- **Merge grain.** The two crosstabs are combined with a `full_join` on
  `(geography, time, age)`, not truncated to a shared date range or age grain -- see the
  table above. A row present in only one crosstab has `NA` in the other crosstab's columns.

## Current output

52 geographies (50 states + DC + national) x 7 age groups (6 buckets + `Total`) x periods
that differ by crosstab -- diagnosis crosstab: 2018-2025 full years plus the 2026-08-18
partial (9 periods); medication crosstab: 2018-2025 full years plus the 2026-07-28 partial
(9 periods) -- = 3,640 rows after the full join.
