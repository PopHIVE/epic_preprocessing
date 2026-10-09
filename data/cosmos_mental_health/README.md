# cosmos_mental_health

Epic Cosmos mental health emergency department burden: quarterly ED length-of-stay statistics
(median and quartiles) and ED visit counts for suicidal behavior, other ED diagnoses, and all
ED encounters, by state of residence and age band.

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

## Updating

The only raw input is one password-protected SlicerDicer crosstab in `raw/staging/`
(`ED Encounters_Crosstab_2026-10-09T14_24_58.xlsx`, session 2865212, exported 2026-10-09,
7/1/2022 - 6/30/2026 by arrival date). Re-export the session over the full date range, put the
file in `raw/staging/` and move the superseded one to `raw/archive/`; `ingest.R` reprocesses
only when a staging file hash changes.

Session setup: Data model = ED Encounters, population base = All ED Encounters, filtered to
**Country of Residence** = United States of America. Rows = Year > Quarter > Age at Time of
Visit > State of Residence; columns = ED Diagnoses (Suicidal behavior, Other, and the Total
column) over the measures Q3 ED length of stay, Q1 ED length of stay, Median ED Length of Stay
and Number of ED Encounters. The ingest rejects a Country of Care export and a Sneak Peek
sample export.

Requires:

- `EPIC_XLSX_PASSWORD` in `.Renviron` (see `usethis::edit_r_environ()`) - exports are
  password protected.
- `msoffcrypto-tool` on the Python used by R (`python -m pip install msoffcrypto-tool`).

The layout is read from the export's own header rows. If the session changes, extend
`DIM_LABELS`, `DX_PATTERNS`, `MEASURE_PATTERNS` or `AGE_PATTERNS` at the top of `ingest.R`; the
script stops on any label it does not recognize.

## Output

`standard/data.csv.gz`, indexed by `geography` x `time` (quarter end, `YYYY-MM-DD`) x `age`:
7,200 rows, 27 columns, 2022-09-30 through 2026-06-30 (16 whole quarters). `age` takes `0-9`,
`10-14`, `15-19`, `20-24`, `0-24`, `25-44`, `45-64`, `65+` and `Overall`. Geographies are 48
states, DC, and national (`"00"`); South Dakota and Wisconsin are not in the export.

| Column family | Statistic |
|---------------|-----------|
| `epic_ed_los_median_*`, `epic_ed_los_q1_*`, `epic_ed_los_q3_*` | ED length of stay in minutes, for `all` (every ED encounter), `suicidal_behavior` and `other` (other mental health reasons) |
| `epic_ed_n_visits` | Number of ED encounters, all diagnoses |
| `epic_ed_n_visits_suicidal_behavior`, `epic_ed_n_visits_other` | Number of ED encounters in the diagnosis group |

Every column has a `_suppressed_flag`.

## Notes

- **Suppression.** Counts of "10 or fewer" are imputed as 5 and flagged 1. Length-of-stay
  statistics are not counts, so a suppressed value is left `NA` and flagged 1. Flags are
  computed before imputation.
- **Dropped rows.** `Armed Forces Africa`, territories, other non-state residences and
  `None of the above` have no name in `resources/all_fips.csv.gz` and are dropped. The
  `Total` state row is the nation (`"00"`); the `Total` age row is `Overall`.
- **Age bands** do not sum to `Overall` (some encounters have no recorded age), and `0-24`
  overlaps the narrower bands.
- **Length of stay** runs to ED departure, so it includes boarding time for patients awaiting an
  inpatient or psychiatric bed.
- **No percent measures.** This export carries no percentage columns; diagnosis share of ED
  visits can be computed from the visit counts. The earlier flat-export measures
  (`epic_ed_pct_visits_*`, `epic_ed_pct_age_*`, and the behavioral, mood, substance use,
  psychosis and eating disorder groups) are not in this file; the previous exports are in
  `raw/archive/`.
- **`ingest.R` validates rather than cleans.** It stops on duplicate index rows, a value and its
  flag disagreeing, unrecognized labels or cell values, a period that is not a whole quarter,
  `Q1 <= median <= Q3` failing in more than 1% of cells, a diagnosis group exceeding all ED
  encounters, or the national count straying from the sum of the states.

You can use the `dcf` package to check the project:

```R
dcf_check()
```

And process it:

```R
dcf_process()
```
