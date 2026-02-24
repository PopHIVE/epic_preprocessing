# =============================================================================
# Epic Cosmos HepB Birth Dose Vaccination Data Ingestion
# Source: Epic SlicerDicer export - HepB at birth visit, by state and month
# =============================================================================

library(dplyr)
library(tidyr)
library(lubridate)

# Initialize process record
if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

# -----------------------------------------------------------------------------
# 1. Check for updates via file hash
# -----------------------------------------------------------------------------
raw_file <- "raw/HepB_state_month.csv"
current_hash <- list(hash = as.character(tools::md5sum(raw_file)))

if (!identical(process$raw_state, current_hash)) {

  # ---------------------------------------------------------------------------
  # 2. Load FIPS lookup
  # ---------------------------------------------------------------------------
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

  state_fips_lookup <- all_fips %>%
    filter(nchar(geography) == 2) %>%
    select(geography, geography_name)

  # ---------------------------------------------------------------------------
  # 3. Read raw data
  # ---------------------------------------------------------------------------
  # Skip 12 rows (11 metadata + 1 split-header row); assign column names manually
  data_raw <- read.csv(
    raw_file,
    skip = 12,
    header = FALSE,
    col.names = c("year", "month", "state_name", "n_patients", "pct_hepb"),
    stringsAsFactors = FALSE,
    check.names = FALSE
  )

  # ---------------------------------------------------------------------------
  # 4. Transform
  # ---------------------------------------------------------------------------
  valid_states <- c(state.name, "District of Columbia")

  data_standard <- data_raw %>%
    # Fill year and month downward (merged cells in original export)
    mutate(
      #year  = na_if(trimws(year), ""),
      month = iconv(month, to = "UTF-8", sub = ""),
      state_name = iconv(state_name, to = "UTF-8", sub = ""),
      month = na_if(trimws(month), "")
    ) %>%
    fill(year, month, .direction = "down") %>%
    mutate(state_name = trimws(state_name)) %>%
    # Keep only US states, DC, and national total
    filter(state_name %in% c(valid_states, "Total")) %>%
    # Handle suppression: Epic suppresses counts <= 10 as "10 or fewer"
    mutate(
      suppressed_flag = if_else(n_patients == "10 or fewer", 1L, 0L),
      n_patients      = if_else(n_patients == "10 or fewer", "5", n_patients),
      n_patients      = as.numeric(n_patients),
      # Convert percentage: strip "%" and treat "-" as NA
      value = as.numeric(na_if(gsub("%", "", pct_hepb), "-"))
    ) %>%
    # Map geography: "Total" → "00", states → FIPS codes
    mutate(
      geography_name_join = if_else(state_name == "Total", "United States", state_name)
    ) %>%
    left_join(state_fips_lookup, by = c("geography_name_join" = "geography_name")) %>%
    filter(month %in% c('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')) %>%
    # Build time: last day of month in YYYY-MM-DD format
    mutate(
      date = as.Date(paste(year, month, "01"), format = "%Y %b %d"),
      time = format(ceiling_date(date, "month") - days(1), "%Y-%m-%d"),
    ) %>%
    filter(!is.na(geography)) %>%
    select(geography, time, n_patients, value, suppressed_flag) %>%
    rename(n_births = n_patients,
              pct_hepb_birth = value)

  # ---------------------------------------------------------------------------
  # 5. Write standardized output
  # ---------------------------------------------------------------------------
  if (!dir.exists("standard")) dir.create("standard")

  vroom::vroom_write(
    data_standard,
    "standard/data.csv.gz",
    delim = ","
  )

  # ---------------------------------------------------------------------------
  # 6. Record processed state
  # ---------------------------------------------------------------------------
  process$raw_state <- current_hash
  dcf::dcf_process_record(updated = process)
}
