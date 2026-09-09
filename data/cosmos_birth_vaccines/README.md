# cosmos_birth_vaccines

Epic Cosmos vaccination coverage among patients with birthing parent information: Vitamin
K, RSV immunization (nirsevimab), and hepatitis B, by year and state of residence. Two
population bases are produced -- see below.

This is a dcf data source project, initialized with `dcf::dcf_add_source`.

## Updating

The source is two Epic Cosmos SlicerDicer sessions using the "Patients with Birthing
Parent Information" data model, each exported as a crosstab xlsx:

1. Re-run each session in Epic Cosmos and export the crosstab as xlsx.
   - All patients (`raw/staging/`): Population Base = "All Patients with Birthing Parent
     Information", no additional population filter.
   - CPT birth cohort (`raw/staging_cpt_birth/`): same data model, additionally filtered
     to a Billed Procedures criterion indicating birth (CPT 99460-99465, 99468, 99477 --
     "1st hosp/birthing center care", "normal newborn care", etc.).
   - Both sessions share the same layout: rows = Year x State of Residence; measures =
     Vit K (%), RSV (%), Hep B (%), Percentage of Population (%), Number of Patients.
     "Percentage of Population" is read but not carried into the standardized output.
2. Drop each export into its raw folder, replacing the previous file. Each folder should
   contain exactly one xlsx file.
3. Run the ingest — `ingest.R` reprocesses each population base only when its staging
   file hash changes, writing `standard/data.csv.gz` (all patients) and
   `standard/data_cpt_birth.csv.gz` (CPT birth cohort).

Requires two things in the environment:

- `EPIC_XLSX_PASSWORD` in `.Renviron` (see `usethis::edit_r_environ()`) — SlicerDicer
  exports are password protected.
- `msoffcrypto-tool` on the Python used by R (`python -m pip install msoffcrypto-tool`),
  used to decrypt the xlsx.

## Notes

- **Two population bases, same measures.** `standard/data.csv.gz` covers all Epic Cosmos
  patients with birthing parent information; `standard/data_cpt_birth.csv.gz` is the
  subset with a billed procedure indicating birth. The CPT-filtered population runs
  roughly half the patient count of the all-patients population nationally. The two are
  not meant to be summed or compared row-for-row -- they are alternative denominators for
  the same measures.
- **RSV is missing, not suppressed, before 2023.** `epic_pct_rsv` is `NA` for 2018-2022 in
  both population bases because nirsevimab (the RSV immunization this measure tracks) was
  not FDA-approved until 2023 -- the measure did not exist yet, so
  `epic_pct_rsv_suppressed_flag` is 0 for those rows even though the value is missing.
- **Suppression.** Epic Cosmos suppresses patient counts of 10 or fewer as `"10 or
  fewer"` and percentages as `"-"`. Both are imputed rather than left missing: counts as
  5, percentages as 5 divided by the row's `epic_n_patients`. `suppressed_flag` stays 1
  either way, to mark the value as imputed. As of the 2026-09-08 export neither session
  has any suppressed cells at this year x state granularity, but both markers are still
  handled defensively.
- **Coverage.** Both exports cover 49 states (no Rhode Island or DC in either session,
  across all years 2018-2025) plus national (`"00"`). State-level `epic_n_patients`
  reflects Epic's footprint among birthing facilities, not the underlying birth count.
- Time resolution is annual; `time` is `12-31-<year>`.

You can use the `dcf` package to check the project:

```R
dcf_check()
```

And process it:

```R
dcf_process()
```
