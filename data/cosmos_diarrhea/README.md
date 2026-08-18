# cosmos_diarrhea

Epic Cosmos SlicerDicer measures for acute gastrointestinal illness: all-cause
diarrhea (ICD-10-CM A00-A09, R19.7), nausea and vomiting (ICD-10-CM R11.\*), and
cyclospora lab testing, weekly by state and (where available) age.

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

```R
dcf_check()    # validate the project
dcf_process()  # run ingest.R
```

## Outputs

| File | Grain | Contents |
|---|---|---|
| `standard/data_weekly.csv.gz` | `geography` x `age` x `time` | ED encounters (diarrhea, nausea/vomiting, all-cause denominator) full-joined with all-encounter-type diarrhea and its denominator |
| `standard/weekly_tests.csv.gz` | `geography` x `time` | Cyclospora tests performed and positives, plus the all-ages diarrhea and total-encounter counts |

`time` is the Saturday ending each Sunday-Saturday Epic week. Partial weeks at
the start and end of an export's date range are dropped.

## Raw exports

Each subfolder of `raw/` holds exactly one password-protected SlicerDicer
crosstab; a sibling `.json` records the session metadata (session ID, date
range, export date) that produced the current `standard/` files.

| Folder | Session | Rows | Columns |
|---|---|---|---|
| `raw/staging_diarrhea_wide/` | 2849923 — "Number of ED Encounters by ED Diagnoses and Age at Time of Visit Range and State of Residence" | State of Residence, Year, Week | ED Diagnoses buckets (all-cause diarrhea A00-09 R19.7; Nausea and vomiting R11.\*; Total) x Age at Time of Visit |
| `raw/staging_diarrhea_all_encounters_weekly_wide/` | All encounter types, diarrhea | Year, Week, State of Residence | diarrhea; None of the above (dropped); Total x age |
| `raw/staging_cyclospora_weekly_wide/` | Cyclospora lab components | Year, Week, Abnormal Lab Components, State of Residence | cyclospora tests; Total |

Population base for the ED crosstab: **All ED Encounters**, Country of
Residence = United States of America.

## Updating

1. Re-run the SlicerDicer session, keeping the same rows and measures listed
   above, and export to `.xlsx`.
2. Drop the file into the matching `raw/staging_*` folder, **deleting the
   superseded export** — each `process_*()` function reads a single file per
   folder, and leaving two would double-count overlapping weeks.
3. Run `dcf::dcf_process('cosmos_diarrhea')` from the project root, or
   `setwd("data/cosmos_diarrhea"); source("ingest.R")`.

Prerequisites:

- `EPIC_XLSX_PASSWORD` set in `.Renviron` (`usethis::edit_r_environ()`) — the
  exports are password protected.
- `msoffcrypto-tool` installed for the Python on `PATH`
  (`python -m pip install msoffcrypto-tool`); `ingest.R` stops with the install
  command if it is missing.

### When the session structure changes

Column positions are resolved from the export's own header rows (row 12 =
outcome/bucket labels, row 13 = age labels, row 15+ = data), not hard-coded.
The ED crosstab's buckets are matched by regex against `ED_OUTCOME_PATTERNS` in
`ingest.R`; an unrecognized or ambiguous bucket label **stops the run** rather
than silently landing on the wrong measure. Extend that map when a bucket is
added or re-worded. The all-encounters and cyclospora parsers have their own
label handling in `process_diarrhea_all_encounters_weekly_wide()` and
`process_cyclospora_weekly_wide()`.

## Notes and caveats

- **Suppression.** Epic reports counts of 10 or fewer as `"10 or fewer"`. These
  are imputed as 5 and flagged in the matching `epic_suppressed_flag_*` column,
  which is computed before imputation. Percentages are computed from the imputed
  counts; where the *denominator* was suppressed (and so imputed as 5) the
  percentage is 100% by construction rather than by observation. This affects a
  small number of low-volume state/age/week cells in `epic_pct_ed_diarrhea` and
  `epic_pct_ed_vomiting` alike.
- **Overlapping diagnosis buckets.** The diarrhea (A00-A09, R19.7) and nausea/
  vomiting (R11.\*) buckets are not mutually exclusive — an encounter with both
  diagnoses is counted in both — so `epic_n_ed_diarrhea` and
  `epic_n_ed_vomiting` (and their percentages) must not be summed.
- **ED vs all encounter types.** `epic_*_ed_*` columns are restricted to
  emergency department encounters; `epic_n_all_diarrhea` and
  `epic_n_encounters_total_weekly` cover encounters of any type. The `ed_`
  prefix exists to keep the two denominators apart.
- **Separate export vintages.** The ED and all-encounters crosstabs are exported
  independently and can end on different weeks. They are combined with a full
  join on `(geography, age, time)`, so a week present in only one source keeps
  its values and gets `NA` for the other source's columns rather than being
  dropped. Export dates per output file are recorded in `process.json` under
  `vintages`.
- **Reporting lag.** The most recent week or two are understated as encounters
  continue to be recorded after export.
- **Dropped rows.** Non-US geographies (territories, other countries) and
  catch-all rows are dropped in `map_state_to_geography()`; `"Total"` becomes
  geography `"00"`. The all-encounters crosstab's `"None of the above"` outcome
  block (redundant with Total minus diarrhea) and its `"No value"` age bucket
  (undocumented age, with no equivalent in the ED crosstab) are also dropped.
