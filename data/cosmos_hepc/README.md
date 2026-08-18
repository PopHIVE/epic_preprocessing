# cosmos_hepc

Epic Cosmos annual counts related to **hepatitis C**, from two SlicerDicer sessions merged
into one standard file, keyed by `(geography, time, age)`:

1. **Hepatitis C diagnosis counts** (`raw/staging/`) -- patients with a viral hepatitis C
   diagnosis, alongside the all-patients denominator, stratified by state of residence and
   age at encounter. The population base is *All Patients* with `Has Any Encounters?` and
   `Country of Care = United States of America`. No diagnosis filter is applied to the
   population itself -- the diagnosis restriction is a **row dimension**, so each
   state/year/age cell reports both the hepatitis C bucket and a `Total` row covering all
   patients.
2. **HCV medication measure** (`raw/staging_medication/`) -- among patients with an HCV
   medication on record (itself among patients meeting the same viral hepatitis C diagnosis
   criteria, applied here as a **population filter**), the rate of a named Epic Cosmos
   quality measure ("hcv medication measure"), stratified by state of residence and age at
   encounter. `All Medications` is a **row dimension** with two buckets, `has HCV medication`
   (used) and `Total` (unused; confirmed to match the diagnosis crosstab's
   `epic_n_patients_hepc` exactly as of the 2026-08-18 update -- see caveats).

Output: `standard/data.csv.gz`

| Column | Description |
|---|---|
| `geography` | FIPS string -- `"00"` national, 2-digit state |
| `time` | Period end date. `YYYY-12-31` for a whole year; each crosstab's trailing **partial** year keeps its own real end date -- currently `2026-07-28` for the diagnosis crosstab, `2026-06-30` for the medication crosstab (they do NOT align as of this update) |
| `age` | `<5 Years`, `5-17 Years`, `18-24 Years`, `25-44 Years`, `45-64 Years`, `65+ Years`, `Total` |
| `epic_n_patients_hepc` | Patients with a viral hepatitis C diagnosis |
| `epic_n_patients_hepc_suppressed_flag` | 1 if suppressed and imputed as 5 |
| `epic_n_patients` | All patients with any encounter (denominator for `epic_pct_hepc`) |
| `epic_n_patients_suppressed_flag` | 1 if suppressed and imputed as 5 |
| `epic_pct_hepc` | `epic_n_patients_hepc / epic_n_patients * 100` |
| `epic_pct_hepc_suppressed_flag` | 1 if either numerator or denominator was suppressed |
| `epic_n_patients_hepc_medication_pop` | Patients with an HCV medication on record (denominator for `epic_pct_hepc_medication`) -- a smaller, more specific population than "all HCV-diagnosed patients"; see caveats |
| `epic_n_patients_hepc_medication_pop_suppressed_flag` | 1 if suppressed and imputed as 5 |
| `epic_pct_hepc_medication` | The source's own "hcv medication measure (%)" rate for that population, taken directly (imputed via `5 / denominator * 100` when blank) |
| `epic_pct_hepc_medication_suppressed_flag` | 1 if the underlying rate was suppressed |
| `epic_n_patients_hepc_medication` | **DERIVED**, not source-reported: `round(epic_n_patients_hepc_medication_pop * epic_pct_hepc_medication / 100)` -- patients meeting the hcv medication measure |
| `epic_n_patients_hepc_medication_suppressed_flag` | 1 if the underlying rate was suppressed (identical to `epic_pct_hepc_medication_suppressed_flag`) |

Because the two crosstabs cover different time ranges and are separately exported sessions,
rows are merged with a **full join** on `(geography, time, age)`: a cell that exists in only
one crosstab gets `NA` for the other crosstab's columns rather than being dropped or
truncated to a shared date range.

## Updating

**SlicerDicer session IDs:**
- Diagnosis crosstab: `2850163` (predecessors: `2850139`, `2847109`, `2847096`)
- Medication crosstab: `2850012`

### Diagnosis crosstab (`raw/staging/`)

Rows, in this order:

1. `Year`
2. `State of Residence`
3. `Diagnosis (All)` -- the `hepc` bucket, **with the Total row enabled** (this is the
   denominator; the ingest stops with an error if it is missing)

Columns: `Age at Encounter in Years`, bucketed as Less than 5 / 5-18 / 18-25 / 25-45 /
45-65 / 65 or more, **with the Total column enabled** (this produces `age = "Total"` rows;
the ingest runs fine without it, just producing no overall-age row for these measures --
that was the case for the intermediate `2850139` export, now corrected by `2850163`).

**Note:** session `2850163`'s date range is `1/1/2020 - 7/28/2026`, narrower than its
predecessor `2850139` (`1/1/2018 - 7/22/2026`). Trading the earlier history for the Total
column was a deliberate choice at the time of this update; if a future re-export restores
2018-2019 coverage while keeping the Total column, no code changes should be needed.

### Medication crosstab (`raw/staging_medication/`)

This crosstab's layout is structurally different from the diagnosis crosstab -- **four** row
dimensions rather than three, and two *named* measure columns rather than one dimension's
buckets spread across columns:

Rows, in this order:

1. `Year`
2. `State of Residence`
3. `All Medications` -- `has HCV medication` and `Total`, **both rows enabled** (the ingest
   stops with an error if either is missing, though only `has HCV medication` is used --
   `Total` is validated for structural sanity only)
4. `Age at Encounter in Years`, bucketed the same way as the diagnosis crosstab, **with the
   Total row enabled**

Measures (columns): `Number of Patients` and `hcv medication measure (%)`, both used --
**neither column alone is what you might expect**. For the `has HCV medication` bucket,
`Number of Patients` is that bucket's own population (patients with an HCV medication on
record) and `hcv medication measure (%)` is that population's own rate for the named
measure; the source does NOT report a count of patients meeting the measure directly, so
the ingest derives it as `round(population * rate / 100)`. Population filter:
`Diagnosis (All)` set to the same hepatitis C definition used for the diagnosis crosstab's
`hepc` bucket.

**Note:** an earlier export of this same session (`Patients_Crosstab_2026-08-18T10_23_34`)
appeared to be missing the diagnosis population filter and was discarded without being
ingested -- its `All Medications: Total` bucket's counts happened to coincidentally
resemble `epic_n_patients_hepc`'s numbers, which is what first surfaced the need to verify
this crosstab's population filter carefully rather than assume it. Always re-verify that
the diagnosis filter is present in the Population Criteria Filters metadata before ingesting
a new export of this session. Separately, an earlier version of this ingest treated the
`Total` bucket's population and a directly-computed ratio as the measure -- that produced
implausibly low rates (~5-35%) because it ignored the `has HCV medication` bucket's own
rate column entirely; the current design (using that bucket's own population and rate, and
deriving the count from them) was confirmed against expected real-world HCV treatment rates
(roughly 70%+) before being adopted.

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
| `DX_LABELS` | Diagnosis buckets -> measure column suffix (diagnosis crosstab; matched case-insensitively) |
| `MED_EXPECTED` / `standardize_med_label()` | Medication bucket labels and their standard form |
| `DENOM_COL` / `MED_NUM_COL` / `MED_DENOM_COL` | Denominator/numerator column names per crosstab |
| `MEASURE_PATTERN` | The measure named in the metadata block (diagnosis crosstab only -- the medication crosstab has no metadata "Measure" row) |

The two crosstabs use **different parsing functions** because their layouts differ in kind,
not just in labels: `extract_staging_data()` handles the diagnosis crosstab's shape (3 row
dimensions + one dimension's buckets spread across N columns);
`extract_medication_data()` handles the medication crosstab's shape (4 row dimensions +
2 named measure columns under a "Measures" header). Adding a second diagnosis bucket to the
diagnosis crosstab needs only a `DX_LABELS` entry -- the script generates its count column,
its percentage, and both flags automatically. See sections "4a"/"4b" in `ingest.R` for the
downstream numerator/denominator/percent pattern to follow for a new measure. Add the
corresponding `measure_info.json` entries by hand in all cases.

## Notes and caveats

- **The numerator's code set contains an error, in BOTH crosstabs.** Session `2850012`'s own
  Population Criteria Filters read: `Chronic viral hepatitis C (ICD-10-CM: B18.2)`,
  `Acute hepatitis E (ICD-10-CM: B17.2)`, `Unspecified viral hepatitis C`. `B17.2` is **not**
  a hepatitis C code. This matches the predecessor session (`2847096`) used to define the
  diagnosis crosstab's `hepc` bucket, so both `epic_n_patients_hepc` and
  `epic_n_patients_hepc_medication_pop` include a small number of acute hepatitis E patients.
  Correct the bucket/population-filter definition in SlicerDicer and re-export both sessions
  to remove it.
- **`epic_n_patients_hepc_medication_pop` is NOT "all HCV-diagnosed patients."** It is the
  smaller `has HCV medication` bucket's population (patients with an HCV medication already
  on record). The crosstab's separate `Total` bucket -- unused here -- is the one confirmed
  to match `epic_n_patients_hepc` exactly cell-for-cell on every overlapping
  `(geography, time, age)` checked as of the 2026-08-18 update. Do not confuse the two, and
  do not assume the `Total`-vs-`epic_n_patients_hepc` agreement is structurally guaranteed on
  future updates (the two sessions have independently set date ranges) -- re-verify after
  each update rather than assuming it holds.
- **The exact clinical definition of "hcv medication measure" is not confirmed.** The
  SlicerDicer export does not state it, and it has not been checked against Epic
  Cosmos/SlicerDicer documentation. Plausible candidates include treatment adherence,
  treatment completion, or sustained virologic response, given the rate's magnitude
  (nationally 77-84 percent, 2018 through mid-2026). Do not assert a specific clinical
  meaning in downstream use without confirming it first.
- **`epic_n_patients_hepc_medication` is derived, not source-reported.** The source gives the
  `has HCV medication` bucket's population and its own rate, but never a numerator count, so
  the ingest computes `round(population * rate / 100)`. Treat this column with the same
  caution as any other back-calculated count: rounding means it will not always reconcile
  exactly with the rate to the last decimal, and a future export that changes what
  "hcv medication measure" itself measures would silently change what this count represents.
- **Use the `Total` age row, don't sum the buckets.** `age = "Total"` is the source's own
  overall-age total and *includes* patients whose age at encounter is unknown, so it runs
  slightly above the sum of the six buckets. Both crosstabs have the Total age row enabled
  as of this update.
- **The diagnosis crosstab's history currently starts at 2020, not 2018.** The intermediate
  export used earlier in this update cycle (`2850139`) covered 2018-2025 but lacked the age
  `Total` column; the corrected re-export (`2850163`) restored the Total column but only
  covers 2020-2025 plus the 2026 partial. Rows for 2018 and 2019 have `NA` for
  `epic_n_patients_hepc` / `epic_n_patients` / `epic_pct_hepc` but are still populated for
  the medication measures, which have covered 2018 onward throughout. If a future re-export
  restores 2018-2019 while keeping the Total column, no code changes should be needed.
- **The last period may be partial, and is kept -- independently per crosstab.** `time` is
  the period *end* date, so a whole year is `YYYY-12-31` while a trailing partial period
  keeps its true end. As of this update: `2026-07-28` for the diagnosis crosstab (Jan 1 -
  Jul 28), `2026-06-30` for the medication crosstab (Jan 1 - Jun 30) -- they do NOT align.
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
  2) -- confirmed against the 2026-08-18 export: every suppressed-population row had a blank
  rate, and a further 60 rows had a normal population but a blank rate; no row had a
  suppressed population with a rate shown. Where the denominator *is* suppressed, the rate is
  left `NA` rather than asserting the meaningless `5 / 5 = 100%`, while its flag stays 1.
- **Interpretation.** `epic_pct_hepc`/`epic_n_patients_hepc` describe diagnosed hepatitis C
  among people who sought care, not population prevalence, sensitive to testing patterns and
  Epic's coverage in each state. `epic_pct_hepc_medication` describes a named quality
  measure's rate among patients who already have an HCV medication on record (see the
  caveats above on what population and measure this actually covers) -- it is NOT a
  medication-uptake-among-all-HCV-patients rate.
- **Merge grain.** The two crosstabs are combined with a `full_join` on
  `(geography, time, age)`, not truncated to a shared date range or age grain -- see the
  table above. A row present in only one crosstab has `NA` in the other crosstab's columns.

## Current output

52 geographies (50 states + DC + national) x 7 age groups (6 buckets + `Total`) x periods
that differ by crosstab -- diagnosis crosstab: 2020-2025 full years plus the 2026-07-28
partial (7 periods); medication crosstab: 2018-2025 full years plus the 2026-06-30 partial
(9 periods) -- = 3,640 rows after the full join.
