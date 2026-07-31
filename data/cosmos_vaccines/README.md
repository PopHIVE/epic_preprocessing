# cosmos_vaccines

Epic Cosmos infant vaccination coverage: monthly RSV immunization (nirsevimab),
hepatitis B, and pneumococcal conjugate vaccine (PCV) uptake in the newborn birth
cohort, by state of residence.

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

## Updating

1. Re-run the SlicerDicer session (Session ID 2845781) in Epic Cosmos and export the
   crosstab as xlsx. Rows = Year / Month / State of Residence; measures = Number of
   Patients, Immunization after birth (%), Percentage with Immunizations: hepB (%),
   Percentage with Billed Procedures followed by Immunizations: Any PCV within 1 to
   182 days (%).
2. Drop the export into `raw/staging/`, replacing the previous file.
3. Run the ingest — `ingest.R` reprocesses only when a staging file hash changes.

Requires two things in the environment:

- `EPIC_XLSX_PASSWORD` in `.Renviron` (see `usethis::edit_r_environ()`) — SlicerDicer
  exports are password protected.
- `msoffcrypto-tool` on the Python used by R (`python -m pip install msoffcrypto-tool`),
  used to decrypt the xlsx.

`ingest.R` resolves the column layout from the export's own header rows rather than
hard-coding it, so a session whose stratification or measure labels differ still parses.
Unrecognized dimensions or measure labels raise an error instead of being silently mapped
onto the wrong column — if you add a measure or a stratification to the session, extend
`DIM_LABELS` / `MEASURE_PATTERNS` at the top of the script.

## Notes

- Incomplete leading/trailing periods (e.g. `Jun 23 - Jun 30`, `Jun 1 - Jun 22`) are
  dropped, so the series starts and ends on whole months.
- `None of the above` (unknown state of residence) is dropped; output covers 50 states,
  DC, and national (`"00"`).
- **Suppression is per measure.** Epic suppresses each measure independently, so every
  measure carries its own `<measure>_suppressed_flag`. A blank percentage against an
  observed denominator means the numerator was 10 or fewer (an explicit `0%` is never
  emitted, so a blank covers 0-10); the numerator is imputed as 5 and the value becomes
  `5 / epic_n_patients * 100`. Where `epic_n_patients` was itself suppressed the
  percentages are flagged but left `NA`, since the imputed denominator of 5 would force a
  meaningless 100%. Exclude flagged cells from precise estimates.
- **Right censoring.** `epic_pct_pcv_182d` cannot be fully observed for infants born
  within ~6 months of the export cutoff and is severely understated over the final
  months. `epic_pct_rsv_immunization` is affected over the last 2-3 months, since
  nirsevimab is given seasonally (~Oct-Mar). `epic_pct_hepb_vaccination` is not
  materially affected, as the birth dose is given during the birth hospitalization.
- Population base is billed-procedure based (CPT 99460-99465, 99468, 99477), which is
  narrower than the liveborn-diagnosis (ICD-10-CM Z38.\*) base used by
  `cosmos_hepb_vax`. The two HepB series are not directly comparable.
- State-level `epic_n_patients` reflects Epic's footprint among birthing facilities, not
  the underlying birth count. Arizona in particular has months suppressed at 10 or fewer.

You can use the `dcf` package to check the project:

```R
dcf_check()
```

And process it:

```R
dcf_process()
```