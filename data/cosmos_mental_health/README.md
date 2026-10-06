# cosmos_mental_health

Epic Cosmos mental health emergency department burden: monthly ED length-of-stay statistics
(average, median, standard deviation, minimum, maximum) and diagnosis case mix, by mental
health diagnosis group, state of residence, and evaluated sex, with an all-cause ED baseline
for comparison.

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

## Updating

1. Re-run the SlicerDicer session (Session ID 2846547) in Epic Cosmos and export the
   crosstab as xlsx. Data model = ED Encounters; population base = All ED Encounters,
   filtered to Country of Residence: United States of America. Rows = State of Residence /
   Year / Month; columns = Evaluated Sex (outer) x ED Diagnoses (middle) x Measures (inner),
   with measures = Average, Variance, Max, Min, Median ED Length of Stay (mins) and
   Percentage of Sliced Population.
2. Drop the export into `raw/staging/`, replacing the previous file.
3. Run the ingest — `ingest.R` reprocesses only when a staging file hash changes.

Requires two things in the environment:

- `EPIC_XLSX_PASSWORD` in `.Renviron` (see `usethis::edit_r_environ()`) — SlicerDicer
  exports are password protected.
- `msoffcrypto-tool` on the Python used by R (`python -m pip install msoffcrypto-tool`),
  used to decrypt the xlsx.

`ingest.R` resolves the crosstab layout from the export's own header rows rather than
hard-coding it. Each level of the column hierarchy is classified by matching its labels
against the vocabularies at the top of the script, so a session with a different nesting
order — or one with no Measures level at all, like the earlier single-measure export — parses
without changes. Unrecognized dimensions, sexes, diagnosis buckets, or measures raise an
error instead of being silently mapped onto the wrong column; extend `DIM_LABELS` /
`SEX_LABELS` / `DX_PATTERNS` / `MEASURE_PATTERNS` when the session changes.

## Output

`standard/data.csv.gz`, indexed by `geography` x `time` x `sex` — 9,600 rows, 97 columns.
`sex` takes `Male`, `Female`, `Ambiguous`, and `Overall`.

Six statistics, each with one column per diagnosis bucket and a per-column suppression flag:

| Column family | Statistic | Notes |
|---------------|-----------|-------|
| `epic_ed_los_avg_*` | Average minutes | Right-skewed by boarders |
| `epic_ed_los_median_*` | Median minutes | **Most robust — recommended default** |
| `epic_ed_los_sd_*` | SD in minutes | Square root of the source's variance field |
| `epic_ed_los_min_*` | Shortest stay | Unreliable, see below |
| `epic_ed_los_max_*` | Longest stay | Unreliable, see below |
| `epic_ed_pct_visits_*` | Percent of ED encounters | Derived, see below |

Diagnosis suffixes: `suicidal_behavior`, `mood`, `behavioral`, `substance_use`, `psychosis`,
`eating_disorders`, `other`, `all_cause`. `epic_ed_pct_visits_*` omits `all_cause` (100% by
construction), giving 5x8 + 7 = 47 measure columns and 47 flags.

## Notes

- **`*_all_cause` is all-cause, not a mental health total.** It is the total row of the ED
  Diagnoses grouper, which includes the non-mental-health diagnoses that are not displayed as
  their own columns. Nationally the average runs ~300 minutes against ~450-1000 for the mental
  health buckets, which could not hold for a mental-health-only total. Use it as the baseline
  to normalize against when comparing geographies or months with differing ED throughput.

- **`epic_ed_pct_visits_*` is derived, not a source column.** The source's "Percentage of
  Sliced Population" field has a denominator that depends on the cell's position in the
  crosstab — within a sex group it is that sex's share of the geography's encounters for the
  diagnosis; on the all-sex row it is the geography's share of national encounters for the
  diagnosis; on the national all-sex row it is the diagnosis's share of all encounters. All
  three readings were confirmed against the export (sex shares sum to 100 within a
  state-month-diagnosis, state shares sum to 100 within a month-diagnosis, and the national
  row reproduces the diagnosis mix). Because they are three different quantities the raw field
  is not publishable as one column, so it is rescaled onto a single definition: the percent of
  ED encounters in that geography, month and sex stratum carrying the diagnosis. See section 5
  of `ingest.R` for the algebra. Two independent checks support it — the national mix read
  directly off the export equals the median of the separately derived state shares, and the
  seven mental health buckets sum to a median 6.1% of ED encounters (IQR 5.2-7.5%), matching
  published estimates. Caveats: source percentages are rounded to two decimals, so rare
  buckets (eating disorders sits at the 0.01% national floor) carry large relative rounding
  error, and the buckets are not guaranteed mutually exclusive so they need not sum to a total.

- **Nothing is dropped or altered.** Every value the source published is carried through as
  published, including implausible ones. The only transformation applied to a value is the
  variance being emitted as its square root (`_sd`), so it is on the same scale in minutes as
  the other statistics. Consequently **length-of-stay columns can be negative**, and any
  consumer that needs a non-negative duration must filter for it explicitly.

- **`min` and `max` are unreliable and published only for completeness.** Both are extremes of
  a distribution contaminated by corrupted arrival/departure timestamps. About 17% of the
  reported minima are negative — not a possible elapsed time — and many others are 0. Roughly a
  third of the maxima exceed seven days and the largest exceed two years; no arithmetic test
  separates those from genuine extended boarding. Use `median` (and `avg`) for anything
  interpretive. `avg` carries four negative cells; `median`, `sd` and `max` carry none.

- **Suppression is per measure**, so every measure carries its own `<measure>_suppressed_flag`.
  Epic suppresses a cell built on 10 or fewer encounters by blanking it. Unlike the count
  measures elsewhere in this repository, none of these statistics can be imputed — there is no
  published denominator to impute a numerator against — so the value is left `NA` and only the
  flag is set. Suppression is heavy in the sparser buckets: ~83% of cells for eating disorders,
  ~47% for behavioral, against ~0% for all-cause. Check the flag before comparing geographies.

- **`NA` and flag `1` coincide exactly**, since nothing is dropped: the flag is equivalent to
  `is.na()` on the measure and exists so the reason for the gap is explicit in the file.

- **`ingest.R` validates rather than cleans.** It hard-fails on structural problems — duplicate
  index rows, missing expected columns, missingness disagreeing with the flag, sex shares that
  stop summing to 100, a derived mental health share outside 1–25%, or `min <= median <= max`
  failing in more than 1% of triples (a layout-misread tripwire; it currently fails in 0 of
  57,365). Implausible individual values are reported in the run log, never silently fixed.

- Incomplete leading/trailing periods (e.g. `Jul 1 - Jul 22`) average over only part of a
  month and are dropped, so the series runs on whole months: 2022-07 through 2026-06.

- **South Dakota and Wisconsin are absent from the 2026-08-03 export** and so from the output,
  which covers 48 states, DC, and national (`"00"`). The 2026-07-31 export had all 50 states,
  so this is a gap in the SlicerDicer session's state selection rather than a parsing failure
  — worth fixing on the next export.

- Territories (Puerto Rico, Virgin Islands, Guam, American Samoa, Northern Mariana Islands,
  Marshall Islands, Micronesia), Armed Forces regions, one foreign state (Michoacán), and
  `None of the above` (unknown state of residence) are dropped: `resources/all_fips.csv.gz`
  carries no `geography_name` for those codes.

- `None of the above` evaluated sex is dropped from the output — it is populated in under 1% of
  cells — but is read first, because the sex shares are used to validate the percentage field's
  semantics on every run. If those shares stop summing to 100 the ingest fails loudly rather
  than emitting a silently wrong `epic_ed_pct_visits_*`.

- Length of stay runs to ED **departure**, so it includes boarding time for patients awaiting an
  inpatient or psychiatric bed. That is the main reason mental health stays run several times the
  all-cause average, and it makes these measures boarding indicators as much as throughput ones.

You can use the `dcf` package to check the project:

```R
dcf_check()
```

And process it:

```R
dcf_process()
```
