# cosmos_resp_infections

Epic Cosmos SlicerDicer measures for respiratory infections: weekly emergency
department encounters with an RSV, COVID-19 (U07.\*), or influenza (J09-J11) ED
diagnosis, plus the all-cause ED encounter denominator, by state and age. A
separate monthly RSV lab-testing export (`raw/rsv_tests.csv.xz`) feeds
`standard/monthly_tests.csv.gz` and `standard/no_geo.csv.gz`.

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

```R
dcf_check()    # validate the project
dcf_process()  # run ingest.R
```

## Outputs

| File | Grain | Contents |
|---|---|---|
| `standard/weekly.csv.gz` | `geography` x `age` x `time` | ED encounters for RSV, COVID, flu and all causes, with percentages and per-measure suppression flags |
| `standard/monthly_tests.csv.gz` | `geography` x `age` x `time` | Monthly RSV test positivity among J12-J18 ED visits |
| `standard/no_geo.csv.gz` | `age` x `time` | The raw RSV test export, standardized but without geography |

`time` is the Saturday ending each Sunday-Saturday Epic week. Partial weeks at
the start and end of an export's date range are dropped, and the most recent
weeks whose all-cause volume fell more than 50% below the prior four-week
median are dropped as still-arriving data.

## Raw exports

`raw/staging_resp_infections_wide/` holds one password-protected SlicerDicer
crosstab **per date range**; `ingest.R` binds all files in the folder, so each
period must appear exactly once. `raw/staging_resp_infections_wide.json` records
the session metadata (session ID, date range, export date) behind the current
`standard/weekly.csv.gz`.

Session title: "Number of ED Encounters by ED Diagnoses and State of Residence
and Age at Time of Visit Range". Population base: **All ED Encounters**, Country
of Residence = United States of America. Measure: Number of ED Encounters.
Columns: ED Diagnoses buckets (RSV icd10; COVID-19 U07.\*; Influenza J09-J11;
Total) x Year x Week. Rows: Age at Time of Visit x State of Residence (either
order; see below).

| Date range | Session (last export) |
|---|---|
| 1/7/2018 - 12/25/2021 | 2869726 (2026-10-23) |
| 12/26/2021 - 12/28/2024 | 2870096 (2026-10-23) |
| 12/29/2024 - most recent complete week | 2869974 (2026-10-23) |

## Updating

1. Re-run each SlicerDicer session above with the same rows and measures, set
   the last session's end date to the most recent Saturday that Cosmos has
   data for, and wait for the queued background session to finish before
   exporting to `.xlsx` -- an export taken early is a "Sneak Peek" sample
   (row 3 of the file says so) with counts roughly 1/80 of the full data.
2. Drop each file into `raw/staging_resp_infections_wide/`, **deleting the
   superseded export for that date range** so no week is read twice. Epic
   backfills older data, so the earlier ranges should be refreshed too.
3. Run `dcf::dcf_process('cosmos_resp_infections')` from the project root, or
   `setwd("data/cosmos_resp_infections"); source("ingest.R")`.

Prerequisites:

- `EPIC_XLSX_PASSWORD` set in `.Renviron` (`usethis::edit_r_environ()`) -- the
  exports are password protected.
- `msoffcrypto-tool` installed for the Python on `PATH`
  (`python -m pip install msoffcrypto-tool`); `ingest.R` stops with the install
  command if it is missing.

### When the session structure changes

Column positions are resolved from the export's own header rows (row 12 =
ED Diagnoses labels, row 13 = year/season, row 14 = week ranges, row 15 = id
column headers, row 16+ = data), not hard-coded. The two id columns are named
by matching row 15 against `ID_LABELS` in `ingest.R`, so State x Age and
Age x State exports both parse; an unrecognized header **stops the run**.
Outcome buckets are matched by keyword (RSV / COVID / flu / Total) in
`process_wide_epic_file()`.

## Notes and caveats

- **Suppression.** Epic reports counts of 10 or fewer as `"10 or fewer"`. These
  are imputed as 5 and flagged in the matching `epic_suppressed_flag_*` column,
  computed before imputation. Percentages are computed from the imputed counts.
- **Reporting lag.** The most recent week or two are understated as encounters
  continue to be recorded after export; see the volume-drop filter above.
- **Dropped rows.** Non-US geographies (Puerto Rico, Virgin Islands, other
  countries) and `"None of the above"` are dropped; `"Total"` becomes
  geography `"00"`.
