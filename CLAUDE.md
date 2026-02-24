# PopHIVE/Ingest - Claude Code Configuration

## Project Overview

This repository standardizes public health surveillance data for the PopHIVE platform (pophive.org). Data sources are transformed into a consistent format and combined into bundles for visualization. The project uses the `dcf` R package from Yale's Data-Intensive Social Science Center (DISSC) for workflow management.

**Repository**: https://github.com/PopHIVE/Ingest  
**Documentation**: https://pophive.github.io/processing-documentation/  
**Data Status**: https://dissc-yale.github.io/dcf/report/?repo=PopHIVE/Ingest

---

## Standard Data Format Specification

All standardized output files must conform to these column specifications:

### Required Columns

| Column | Description | Format | Examples |
|--------|-------------|--------|----------|
| `geography` | Geographic identifier | FIPS code string | `"00"` (national), `"06"` (California), `"06037"` (LA County) |
| `time` | Time period end date | `MM-DD-YYYY` | `"01-04-2025"` (Saturday for weekly data) |

### Common Optional Columns

| Column | Description | Values |
|--------|-------------|--------|
| `age` | Age group | `"0-4"`, `"5-17"`, `"18-49"`, `"50-64"`, `"65+"`, `"Overall"` |
| `race_ethnicity` | Race/ethnicity category | `"White"`, `"Black"`, `"Hispanic"`, `"Asian"`, `"Overall"` |
| `sex` | Sex category | `"Male"`, `"Female"`, `"Overall"` |
| `virus` | Pathogen (respiratory data) | `"rsv"`, `"influenza"`, `"covid"` |

### Value Columns

| Column | Description |
|--------|-------------|
| `value` | Primary measure (closest to source data) |
| `value_smooth` | 3-week moving average |
| `value_smooth_scale` | Smoothed value scaled 0-100 |
| `suppressed_flag` | `1` if value was suppressed and imputed, `0` otherwise |

### Geography Standards

- **National**: Use `"00"` (not `"US"` or `"0"`)
- **State**: 2-digit FIPS code as string (`"06"` not `6`)
- **County**: 5-digit FIPS code as string (`"06037"`)
- **PREFERRED**: Convert state/county names to FIPS codes using merge with `resources/all_fips.csv.gz` (much faster than `cdlTools::fips()`)

#### all_fips.csv.gz Structure

The file contains three columns:
| Column | Description | Examples |
|--------|-------------|----------|
| `geography` | FIPS code | `"06"` (state), `"06037"` (county), `"00"` (national) |
| `geography_name` | Full name | `"California"`, `"Los Angeles County"`, `"United States"` |
| `state` | State abbreviation | `"CA"`, `"US"` |

**Important notes:**
- County names include their suffix (e.g., "Los Angeles County", "Orleans Parish", "Anchorage Municipality")
- State-level entries have 2-digit FIPS codes; county-level have 5-digit codes
- National entry uses `"00"` for geography and `"United States"` for geography_name

#### State-level FIPS lookup
  ```r
  # Load FIPS crosswalk (do this once per script)
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

  # For state abbreviations (e.g., "CA", "TX"):
  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    select(geography, state)

  # For full state names (e.g., "California", "Texas"):
  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    select(geography, geography_name)

  # Merge to get FIPS codes
  data <- data %>%
    left_join(state_fips_lookup, by = c("state_column" = "state"))
  ```

#### County-level FIPS lookup
  ```r
  # County names in all_fips include suffixes that must be handled
  county_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 5) %>%
    select(geography, geography_name, state) %>%
    mutate(
      # Strip common suffixes to match raw data formats
      county_name = sub(" County$", "", geography_name),
      county_name = sub(" Parish$", "", county_name),      # Louisiana
      county_name = sub(" Borough$", "", county_name),     # Alaska
      county_name = sub(" Census Area$", "", county_name), # Alaska
      county_name = sub(" Municipality$", "", county_name), # Alaska
      state_fips = substr(geography, 1, 2)
    )

  # Merge using both state and county name to avoid ambiguity
  data <- data %>%
    left_join(county_fips_lookup, by = c("state_fips", "county_name"))
  ```

- **Alternative**: Use `cdlTools::fips(state, to='FIPS')` only if merge approach is not feasible (note: this is very slow)

### Time Standards

- **Format**: `MM-DD-YYYY` (with leading zeros)
- **Weekly data**: Use Saturday at end of week (epiweek convention)
- **Monthly data**: Use last day of month
- **Annual data**: Use `12-31-YYYY`

---

## Directory Structure

```
PopHIVE/Ingest/
├── data/
│   ├── {source_name}/           # Individual data source
│   │   ├── raw/                 # Downloaded source files (compressed)
│   │   ├── standard/            # Standardized output files
│   │   │   ├── data.csv.gz      # Main standardized file
│   │   │   ├── data_state.csv.gz    # State-level (if separate)
│   │   │   └── data_county.csv.gz   # County-level (if separate)
│   │   ├── ingest.R             # Transformation script (SINGLE FILE PER SOURCE)
│   │   ├── measure_info.json    # Variable metadata
│   │   └── process.json         # Processing state (auto-generated)
│   │
│   ├── bundle_{category}/       # Combined datasets
│   │   ├── build.R              # Bundle assembly script
│   │   ├── process.json         # Lists source files used
│   │   └── dist/                # Final outputs for visualization
│   │       └── *.parquet        # Parquet format only (no CSV)
│   │
│   ├── epic/                    # Epic Cosmos data
│   ├── gtrends/                 # Google Health Trends
│   ├── wastewater/              # CDC NWSS
│   ├── nssp/                    # CDC NSSP ED visits
│   ├── respnet/                 # CDC RESP-NET hospitalizations
│   ├── abcs/                    # CDC ABCs pneumococcal
│   ├── NREVSS/                  # CDC lab testing
│   └── nis/                     # National Immunization Survey
│
├── scripts/                     # Utility scripts
├── resources/                   # Reference files (FIPS codes, etc.)
├── settings.json               # Project configuration
├── file_log.json               # File tracking
└── renv.lock                   # R package versions
```

---

## Key dcf Package Functions

```r
# Create new data source folder structure
dcf::dcf_add_source("source_name")

# Initialize processing record for tracking changes
process <- dcf::dcf_process_record()

# Download data from CDC data.gov (Socrata API)
raw_state <- dcf::dcf_download_cdc(
  "dataset-id",      # e.g., "kvib-3txy"
  "raw",             # output directory
  process$raw_state  # previous state for change detection
)

# Update processing record after changes
process$raw_state <- raw_state
dcf::dcf_process_record(updated = process)

# Create or update a bundle
dcf::dcf_process("bundle_respiratory", ".")

# Build all sources (run from project root)
dcf::dcf_build()
```

---

## Important Convention: Single ingest.R Per Data Source

**CRITICAL**: Each data source directory must contain exactly **ONE** `ingest.R` script.

### Multiple Data Sources in One Directory

When a single data source directory needs to process multiple related datasets (e.g., data from different URLs or APIs), **integrate all processing into the single `ingest.R` file** rather than creating separate scripts.

### Example: SchoolVaxView

The `schoolvaxview` directory processes data from two sources:
1. CDC SchoolVaxView (via Socrata API)
2. Washington Post School Vaccination Rates (via GitHub)

Both are integrated into a single `ingest.R` file:

```r
# ingest.R structure for multiple sources
process <- dcf::dcf_process_record()

# Source 1: CDC SchoolVaxView
raw_state <- dcf::dcf_download_cdc("ijqb-a7ye", "raw", process$raw_state)
if (!identical(process$raw_state, raw_state)) {
  # Process CDC data...
  # Write to standard/data.csv.gz and standard/data_exemptions.csv.gz
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}

# Source 2: Washington Post
download.file(wapo_url, "raw/wapo_file.csv")
current_wapo_state <- list(hash = tools::md5sum("raw/wapo_file.csv"))
if (!identical(process$wapo_state, current_wapo_state)) {
  # Process WaPo data...
  # Write to standard/data_wapo_counties.csv.gz and standard/data_wapo_schools.csv.gz
  process$wapo_state <- current_wapo_state
  dcf::dcf_process_record(updated = process)
}
```

### Key Points

- Use the `process` object to track multiple raw data states (e.g., `process$raw_state`, `process$wapo_state`)
- Each source can have its own change detection logic
- Output multiple standardized files with descriptive names (e.g., `data_wapo_counties.csv.gz`)
- All variables from secondary sources should use prefixes to avoid naming conflicts (e.g., `wapo_`)

### Why This Matters

- The `dcf` package expects one `ingest.R` per source directory
- Running `dcf::dcf_process("source_name", "..")` executes the single `ingest.R`
- Multiple scripts would require manual orchestration outside of `dcf`

---

## ingest.R Template

```r
# =============================================================================
# {SOURCE_NAME} Data Ingestion
# Source: {URL or description}
# =============================================================================

library(dplyr)

# Initialize process record (creates process.json if it doesn't exist)
if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

# -----------------------------------------------------------------------------
# 1. Download raw data
# -----------------------------------------------------------------------------
raw_state <- dcf::dcf_download_cdc(
  "{dataset-id}",
  "raw",
  process$raw_state
)

# Only process if data has changed
if (!identical(process$raw_state, raw_state)) {

  # ---------------------------------------------------------------------------
  # 2. Load FIPS lookup and read raw data
  # ---------------------------------------------------------------------------
  # Load FIPS crosswalk (preferred over cdlTools::fips())
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

  # State-level lookup (for full state names like "California")
  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    select(geography, geography_name)

  data_raw <- vroom::vroom("raw/{dataset-id}.csv.xz", show_col_types = FALSE)

  # ---------------------------------------------------------------------------
  # 3. Transform data
  # ---------------------------------------------------------------------------
  data_standard <- data_raw %>%
    # Filter to relevant subset
    filter(
      Type == "Unadjusted Rate",
      Sex == "Overall",
      `Race/Ethnicity` == "Overall"
    ) %>%
    # Rename to standard columns
    rename(
      time = `Week Ending Date`,
      age = `Age group`,
      state_name = Site
    ) %>%
    # Transform geography using FIPS lookup
    left_join(state_fips_lookup, by = c("state_name" = "geography_name")) %>%
    mutate(
      geography = case_when(
        state_name == "Overall" ~ "00",
        !is.na(geography) ~ geography,
        TRUE ~ NA_character_
      )
    ) %>%
    # Format time
    mutate(
      time = format(as.Date(time), "%m-%d-%Y")
    ) %>%
    # Select and order columns
    select(geography, time, age, value = `Weekly Rate`)

  # ---------------------------------------------------------------------------
  # 4. Write standardized output
  # ---------------------------------------------------------------------------
  vroom::vroom_write(
    data_standard,
    "standard/data.csv.gz",
    delim = ","
  )

  # ---------------------------------------------------------------------------
  # 5. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- raw_state
  dcf::dcf_process_record(updated = process)
}
```

---

## measure_info.json Template

**IMPORTANT**: Source information uses an ID-reference pattern. Each measure references sources by ID, and the full source details are defined once in a `_sources` block at the bottom of the file. Do NOT put inline `restrictions`, `name`, or `url` in each measure's `sources` array — only use `{ "id": "source_id" }`.

```json
{
  "variable_name": {
    "id": "variable_name",
    "short_name": "Brief description (< 100 chars)",
    "long_name": "Full descriptive name",
    "category": "respiratory|immunization|chronic|injury",
    "short_description": "One sentence description",
    "long_description": "Detailed description with methodology notes",
    "statement": "Template for narrative: 'In {location}, {value} cases were reported'",
    "measure_type": "Incidence|Prevalence|Rate|Percent|Count",
    "unit": "Cases per 100,000|Percent|Count",
    "time_resolution": "Week|Month|Year",
    "sources": [{ "id": "source_id" }],
    "citations": [
      {
        "title": "Publication title",
        "url": "https://doi.org/..."
      }
    ]
  },

  "_sources": {
    "source_id": {
      "name": "Source organization",
      "url": "https://data.source.url",
      "date_accessed": 2025,
      "organization": "Organization name",
      "organization_url": "https://org.url",
      "description": "Description of the data source.",
      "restrictions": "Attribution and usage restrictions."
    }
  }
}
```

---

## Common Data Source Patterns

### CDC data.gov (Socrata API)

```r
# Dataset ID is in the URL: data.cdc.gov/d/{dataset-id}
raw_state <- dcf::dcf_download_cdc("kvib-3txy", "raw", process$raw_state)
data <- vroom::vroom("raw/kvib-3txy.csv.xz")
```

### Epic Cosmos Source Metadata

All `measure_info.json` entries for Epic Cosmos data **must** use the `_sources` ID-reference pattern. Each measure references the source by ID only, and the full source details (including restrictions) are defined once in the `_sources` block at the bottom of the file.

**Per-measure source reference** (do NOT put inline name/url/restrictions):
```json
"sources": [{ "id": "epic_cosmos" }]
```

**`_sources` block** (include once at the bottom of the file):
```json
"_sources": {
  "epic_cosmos": {
    "name": "Epic Cosmos",
    "url": "https://cosmos.epic.com/",
    "date_accessed": 2025,
    "organization": "Epic Systems",
    "organization_url": "https://www.epic.com/",
    "description": "Epic Cosmos is a collaborative research platform containing de-identified patient data from over 300 million patients across more than 1,600 hospitals and health systems using Epic electronic health record systems. Data is accessed via SlicerDicer, a self-service analytics tool. The dataset includes emergency department visits, diagnoses, immunizations, laboratory results, and other clinical data. Due to privacy protections, counts fewer than 10 are suppressed and imputed. Coverage extends across all U.S. states and territories. Note that county-level and city-level stratifications could differ markedly in total sample size due to high levels of missingness of county data in some states.",
    "restrictions": "The data can be re-used with appropriate attribution. A suggested citation relating to this data is 'Results of research performed with Epic Cosmos were obtained from the PopHIVE platform (https://github.com/PopHIVE/Ingest).'"
  }
}
```

Additional context (for `long_description` or documentation):
- Epic Cosmos contains de-identified data from 300M+ patients across 1,600+ hospitals
- Data accessed via SlicerDicer self-service analytics tool
- Counts fewer than 10 are suppressed and imputed as 5
- County-level data may have high missingness in some states

---

### Epic Cosmos SlicerDicer Exports

#### Raw File Structure

SlicerDicer CSV exports have a multi-row header block before the actual data:

```
Row 1:  Session Title, <title>, ...
Row 2:  Session ID, <id>, ...
Row 3:  Data Model, ...
Row 4:  Population Base, ...
Row 5:  Population Criteria Filters, ...
Row 6:  Session Date Range, ...
Row 7:  Export User, ...
Row 8:  Date of Export, ...
Row 9:  (blank)
Row 10: (blank)
Row 11: (sub-header: column group labels, e.g. ",,Measures,Number of Patients,Percentage...")
Row 12: (column names, e.g. "Year,Month,State of Residence,,")
Row 13+: data rows
```

**Key parsing notes:**
- Skip 12 rows (`skip = 12, header = FALSE`) and assign column names manually — the true column names are split across rows 11 and 12, making `header = TRUE` unreliable
- **Merged cells**: grouping columns (Year, Month, State, etc.) are only filled in on the first row of each group; use `tidyr::fill(..., .direction = "down")` after converting blank strings to NA
- **Suppression**: counts ≤ 10 appear as `"10 or fewer"`; percentages for suppressed rows appear as `"-"` — treat `"-"` as NA
- **Non-US rows**: exports include foreign states/provinces (Canadian provinces, Mexican states, US territories) and catch-all rows like `"None of the above"` and `"Total"` — filter to `c(state.name, "District of Columbia", "Total")` for US-only data; `"Total"` maps to geography `"00"`
- **Non-UTF-8 characters**: Raw exports may contain non-UTF-8 byte sequences (especially in `month` and `state_name` columns); apply `iconv(column, to = "UTF-8", sub = "")` **before** `trimws()` / `na_if()` to strip invalid bytes — failure to do this can cause silent parsing errors or garbled strings
- **Time format**: Always call `format()` explicitly on Date objects before writing — do not rely on vroom's implicit Date serialization. Use `format(date, "%Y-%m-%d")` for ISO 8601 or `format(date, "%m-%d-%Y")` for the legacy project standard. Verify the written output matches the expected format.

```r
# Standard reading pattern for SlicerDicer state/month exports
data_raw <- read.csv(
  raw_file,
  skip = 12,
  header = FALSE,
  col.names = c("year", "month", "state_name", "n_patients", "pct_var"),
  stringsAsFactors = FALSE,
  check.names = FALSE
)

data_clean <- data_raw %>%
  mutate(
    # Strip non-UTF-8 bytes before trimws/na_if (common in SlicerDicer exports)
    month      = iconv(month,      to = "UTF-8", sub = ""),
    state_name = iconv(state_name, to = "UTF-8", sub = ""),
    year  = na_if(trimws(year), ""),
    month = na_if(trimws(month), "")
  ) %>%
  tidyr::fill(year, month, .direction = "down") %>%
  filter(trimws(state_name) %in% c(state.name, "District of Columbia", "Total")) %>%
  mutate(
    suppressed_flag = if_else(n_patients == "10 or fewer", 1L, 0L),
    n_patients      = as.numeric(if_else(n_patients == "10 or fewer", "5", n_patients)),
    value           = as.numeric(na_if(gsub("%", "", pct_var), "-")),
    # Map geography: "Total" -> "00", states -> FIPS via state_fips_lookup join
    geography_name_join = if_else(state_name == "Total", "United States", state_name)
  )
```

#### Month/Year Time Formatting

For monthly data with abbreviated month names (Jan, Feb, ..., Dec):

```r
mutate(
  date = as.Date(paste(year, month, "01"), format = "%Y %b %d"),
  date = lubridate::ceiling_date(date, "month") - lubridate::days(1),  # last day of month
  time = format(date, "%m-%d-%Y")
)
```

### Google Health Trends API

Requires adjustment for vaccination-related searches:
```r
# Adjusted RSV searches (removing vaccine signal)
data <- data %>%
  mutate(
    value_adjusted = rsv_volume - season * 2.72 * vax_volume - 
                     (1 - season) * 3.41 * vax_volume
  )
```

### National Averages from State Data

When national totals aren't provided, calculate population-weighted average:
```r
# Load state populations
state_pop <- read.csv("resources/state_populations.csv")

data_national <- data %>%
  left_join(state_pop, by = "geography") %>%
  group_by(time, age) %>%
  summarize(
    value = weighted.mean(value, population, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(geography = "00")

data_final <- bind_rows(data, data_national)
```

---

## Bundle build.R Template

```r
# =============================================================================
# Bundle: {bundle_name}
# Combines: {list of sources}
# =============================================================================

library(dplyr)
library(arrow)

# -----------------------------------------------------------------------------
# 1. Load standardized source files
# -----------------------------------------------------------------------------
epic <- vroom::vroom("../epic/standard/weekly.csv.gz")
nssp <- vroom::vroom("../nssp/standard/data.csv.gz")
wastewater <- vroom::vroom("../wastewater/standard/data.csv.gz")

# -----------------------------------------------------------------------------
# 2. Harmonize and combine
# -----------------------------------------------------------------------------
combined <- bind_rows(
  epic %>% mutate(source = "epic_cosmos"),
  nssp %>% mutate(source = "nssp_ed"),
  wastewater %>% mutate(source = "wastewater")
)

# -----------------------------------------------------------------------------
# 3. Create derived measures
# -----------------------------------------------------------------------------
combined <- combined %>%
  group_by(geography) %>%
  arrange(time) %>%
  mutate(
    # 3-week moving average
    value_smooth = zoo::rollmean(value, k = 3, fill = NA, align = "right"),
    # Scale to 0-100
    value_smooth_scale = scales::rescale(value_smooth, to = c(0, 100))
  ) %>%
  ungroup()

# -----------------------------------------------------------------------------
# 4. Write outputs (parquet only, no CSV)
# -----------------------------------------------------------------------------
arrow::write_parquet(
  combined,
  "dist/overall_trends.parquet",
  compression = "snappy"  # Use "gzip" for smaller files
)
```

---

## Validation Checklist

When creating or reviewing ingestion scripts, verify:

- [ ] **Geography**: All values are valid FIPS codes; national = `"00"`
- [ ] **Time**: Format is `MM-DD-YYYY`; weekly data uses Saturday
- [ ] **Column names**: Use standard names (lowercase, underscores)
- [ ] **Missing data**: Handled appropriately (NA, not empty strings)
- [ ] **Suppression**: Flagged with `suppressed_flag` column if imputed
- [ ] **measure_info.json**: Entry exists for each variable
- [ ] **Compression**: Standard output files are gzip compressed (`.csv.gz`)
- [ ] **Bundle outputs**: Dist files are parquet only (`.parquet`), no CSV
- [ ] **process.json**: Updated in bundle with source file paths

---

## Common Issues and Solutions

### Issue: State names instead of FIPS codes

See the **Geography Standards** section above for the preferred approach using `all_fips.csv.gz`.

```r
# SLOWER Alternative: Use cdlTools only if merge is not feasible
mutate(geography = cdlTools::fips(state_name, to = "FIPS"))
```

### Issue: Date in wrong format
```r
# Solution: Parse and reformat
mutate(time = format(as.Date(time, "%Y-%m-%d"), "%m-%d-%Y"))
```

### Issue: National data missing
```r
# Solution: Calculate population-weighted average (see pattern above)
```

### Issue: Weekly dates not on Saturday
```r
# Solution: Adjust to end-of-week Saturday
mutate(time = ceiling_date(as.Date(time), "week", week_start = 7) - 1)
```

### Issue: Multiple records per geography/time
```r
# Solution: Check for duplicates, aggregate if needed
data %>%
  group_by(geography, time, age) %>%
  summarize(value = sum(value), .groups = "drop")
```

### Issue: Error "process file process.json does not exist"
```r
# Problem: dcf::dcf_process_record() fails on first run of new data source
# Solution: Check if process.json exists before calling dcf_process_record()
if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}
```

### Issue: Error "raw/file.csv.gz does not exist in current working directory"
```r
# Problem: ingest.R uses relative paths and must be run from source directory
# Solution: Change to source directory before running, then change back
setwd("data/source_name")
source("ingest.R")
setwd("../..")
```

### Issue: Error "missing process file: ../ingest/process.json" when using dcf_process
```r
# Problem: Wrong directory parameter or missing project files
# Solution: From project root, just use the source name (no directory parameter)
dcf::dcf_process("source_name")

# Also ensure project.Rproj and README.md exist in the source folder
```

### Issue: County names not matching FIPS lookup (SAINT vs ST., apostrophes, Alaska, etc.)

Epic Cosmos county data uses naming conventions that differ from standard FIPS lookup tables. Common mismatches include:

**Problem patterns and solutions:**

| Source Data Pattern | FIPS Format | Solution |
|---------------------|-------------|----------|
| `SAINT CROIX` | `St. Croix` | Replace `SAINT ` with `ST. ` |
| `SAINTE GENEVIEVE` | `Ste. Genevieve` | Replace `SAINTE ` with `STE. ` |
| `PRINCE GEORGES` | `Prince George's` | Add apostrophe: `PRINCE GEORGE'S` |
| `QUEEN ANNES` | `Queen Anne's` | Add apostrophe: `QUEEN ANNE'S` |
| `SAINT MARYS` | `St. Mary's` | Both: `ST. MARY'S` |
| `ST JOHN THE BAPTIST` | `St. John the Baptist` | Add period: `ST. JOHN THE BAPTIST` |
| `OBRIEN` | `O'Brien` | Add apostrophe: `O'BRIEN` |
| `LA SALLE` | `LaSalle` | Remove space: `LASALLE` |
| `LA PORTE` | `LaPorte` | Remove space: `LAPORTE` |
| `ANCHORAGE` (Alaska) | `Anchorage Municipality` | Virginia independent cities like `SALEM, VA` and `RADFORD, VA` need city suffix |
| `Total`, `None of the above` | N/A | Filter these out |

**Solution approach:**

Create a county name normalization function before the FIPS join:

```r
# Normalize county names for FIPS matching
normalize_county_name <- function(county) {
  county <- toupper(county)

  # SAINT/SAINTE → ST./STE. (must be done before other transformations)
  county <- gsub("^SAINTE ", "STE. ", county)
  county <- gsub("^SAINT ", "ST. ", county)
  county <- gsub("^ST ", "ST. ", county)  # Add period if missing

  # Add apostrophes to possessive names
  county <- gsub("PRINCE GEORGES", "PRINCE GEORGE'S", county)
  county <- gsub("QUEEN ANNES", "QUEEN ANNE'S", county)
  county <- gsub("ST\\. MARYS", "ST. MARY'S", county)
  county <- gsub("O'BRIENS", "O'BRIEN", county)  # Some have plural
  county <- gsub("^OBRIEN", "O'BRIEN", county)
  county <- gsub("^DEKALB", "DE KALB", county)  # or DEKALB depending on state

  # Handle LA prefix names (varies by location)
  county <- gsub("^LA SALLE", "LASALLE", county)
  county <- gsub("^LA PORTE", "LAPORTE", county)

  return(county)
}

# Apply normalization before join
heat1 <- heat1 %>%
  mutate(county_normalized = normalize_county_name(county)) %>%
  left_join(all_fips_county, by = c('county_normalized' = 'geography_name'))
```

**Alternative: Use `stringdist` for fuzzy matching on remaining NAs:**

```r
library(stringdist)

# For rows that still have NA after direct join
unmatched <- heat1 %>% filter(is.na(geography))
matched <- heat1 %>% filter(!is.na(geography))

if (nrow(unmatched) > 0) {
  # Find closest match using string distance
  for (i in 1:nrow(unmatched)) {
    county_to_match <- unmatched$county_normalized[i]
    distances <- stringdist(county_to_match, all_fips_county$geography_name, method = "jw")
    best_match_idx <- which.min(distances)
    if (distances[best_match_idx] < 0.15) {  # Threshold for match quality
      unmatched$geography[i] <- all_fips_county$geography[best_match_idx]
    }
  }
  heat1 <- bind_rows(matched, unmatched)
}
```

**For Epic Cosmos specifically in data/cosmos/ingest.R:**

The `all_fips_county` lookup should also be normalized to handle these edge cases. See the `normalize_county_name` function defined in that file.

### Issue: Connecticut county FIPS codes not matching

Connecticut abolished its 8 counties in 2022 and replaced them with 9 planning regions as county-equivalents. This means:

- **Old FIPS codes (pre-2022)**: `09001`-`09015` (8 counties: Fairfield, Hartford, Litchfield, etc.)
- **New FIPS codes (2022+)**: `09110`-`09190` (9 planning regions: Capitol, Greater Bridgeport, etc.)

The Census 2021 data uses old codes, while newer datasets (2022+) use new codes. Both sets are included in `resources/all_fips.csv.gz`.

```r
# For datasets using new CT planning region codes, load supplemental population data:
ct_pop <- vroom::vroom("../../resources/ct_planning_regions_pop_under5.csv.gz",
                        show_col_types = FALSE)

# Combine with standard county population data
pop_county <- bind_rows(pop_county, ct_pop)
```

**Resource files:**
- `resources/all_fips.csv.gz` - Contains both old (09001-09015) and new (09110-09190) CT codes
- `resources/ct_planning_regions_pop_under5.csv.gz` - Population under 5 years for CT planning regions (source: 2022 ACS 1-year estimates)

| New FIPS | Planning Region |
|----------|-----------------|
| 09110 | Capitol |
| 09120 | Greater Bridgeport |
| 09130 | Lower Connecticut River Valley |
| 09140 | Naugatuck Valley |
| 09150 | Northeastern Connecticut |
| 09160 | Northwest Hills |
| 09170 | South Central Connecticut |
| 09180 | Southeastern Connecticut |
| 09190 | Western Connecticut |

---

## Quick Reference Commands

```r
# Check current data status
dcf::dcf_status()

# Rebuild single source (from project root)
dcf::dcf_process("source_name")

# Rebuild single bundle
dcf::dcf_process("bundle_measles")

# Full rebuild
dcf::dcf_build()

# Validate standard file format
source("scripts/validate_standard.R")
validate_standard_file("data/source_name/standard/data.csv.gz")
```

---

## Adding a New Data Source: Step-by-Step

1. **Create folder structure**
   ```r
   dcf::dcf_add_source("new_source")
   ```

2. **Edit `ingest.R`**: Follow template above, adapting for source format

3. **Create `measure_info.json`**: Add entries for all output variables

4. **Test transformation**
   ```r
   source("data/new_source/ingest.R")
   ```

5. **Validate output**
   ```r
   validate_standard_file("data/new_source/standard/data.csv.gz")
   ```

6. **Add to bundle**: Update relevant `bundle_*/build.R` and `process.json`

7. **Rebuild bundle**
   ```r
   dcf::dcf_process("bundle_category", ".")
   ```

8. **Commit changes**: Include raw data sample, ingest.R, measure_info.json, standard output

---

## Contact and Resources

- **Processing Documentation**: https://pophive.github.io/processing-documentation/
- **dcf Package**: https://dissc-yale.github.io/dcf/
- **Data Status Report**: https://dissc-yale.github.io/dcf/report/?repo=PopHIVE/Ingest
- **Feedback Form**: https://docs.google.com/forms/d/e/1FAIpQLSchAasiq7ovCCNz9ussb7C2ntkZ-8Rjc7-tNSglkf5boS-A0w/viewform