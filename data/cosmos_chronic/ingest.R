# =============================================================================
# Epic Cosmos Chronic Disease Data Ingestion (v2 - xlsx multi-year format)
# Source: Epic SlicerDicer exports — Diabetes A1C/CCW and Obesity BMI/CCW
#         State-level by age, annual 2016-2025 (+ partial 2026) — xlsx
#         County-level by age, annual 2023-2025 — CSV
# Files:  raw/staging_chronic_year_state_age/diabetes.xlsx
#         raw/staging_chronic_year_state_age/obesity.xlsx
#         raw/staging_chronic/C02_DM_A1C_County_*.csv
#         raw/staging_chronic/C03_OBESITY_ICD_BMI_County_*.csv
# =============================================================================

library(tidyverse)
library(dcf)

STAGING_DIR        <- "raw/staging_chronic_year_state_age"
STAGING_COUNTY_DIR <- "raw/staging_chronic"
readRenviron(file.path(Sys.getenv("USERPROFILE"), ".Renviron"))
EXCEL_PASSWORD <- Sys.getenv("EXCEL_PASSWORD")

# Export multiple password-protected xlsx files to CSV in a single Excel session.
# pairs: list of list(xlsx = "...", csv = "...") entries.
export_xlsx_batch <- function(pairs, password) {
  open_lines <- vapply(pairs, function(p) {
    xlsx_abs <- normalizePath(p$xlsx, winslash = "\\", mustWork = TRUE)
    csv_abs  <- normalizePath(p$csv,  winslash = "\\", mustWork = FALSE)
    paste0(
      '$wb = $e.Workbooks.Open("', xlsx_abs, '", 0, $true, 5, "', password, '")\n',
      '$wb.SaveAs("', csv_abs, '", 62)\n',
      '$wb.Close($false)\n'
    )
  }, character(1))

  ps_script <- paste0(
    '$e = New-Object -ComObject Excel.Application\n',
    '$e.Visible = $false\n',
    '$e.DisplayAlerts = $false\n',
    paste(open_lines, collapse = ""),
    '$e.Quit()\n'
  )
  ps_file <- tempfile(fileext = ".ps1")
  writeLines(ps_script, ps_file)
  on.exit(unlink(ps_file))
  out <- system2("powershell", c("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", ps_file),
                 stdout = TRUE, stderr = TRUE)
  for (p in pairs) {
    if (!file.exists(p$csv))
      stop("Failed to export ", p$xlsx, "\n", paste(out, collapse = "\n"))
  }
  invisible(out)
}

# -----------------------------------------------------------------------------
# Initialize process record
# -----------------------------------------------------------------------------
if (!file.exists("process.json")) {
  process <- list(raw_state = NULL)
} else {
  process <- dcf::dcf_process_record()
}

dm_path <- file.path(STAGING_DIR, "diabetes.xlsx")
ob_path <- file.path(STAGING_DIR, "obesity.xlsx")

stage_a1c_county     <- list.files(STAGING_COUNTY_DIR, pattern = "C02_DM_A1C_County",          full.names = TRUE)
stage_obesity_county <- list.files(STAGING_COUNTY_DIR, pattern = "C03_OBESITY_ICD_BMI_County",  full.names = TRUE)

current_state <- list(
  hashes = unname(tools::md5sum(c(dm_path, ob_path, stage_a1c_county, stage_obesity_county)))
)

if (!identical(process$raw_state, current_state)) {

  # Load FIPS lookup
  all_fips <- vroom::vroom("../../resources/all_fips.csv.gz", show_col_types = FALSE)

  # ---------------------------------------------------------------------------
  # State FIPS lookup
  # ---------------------------------------------------------------------------
  all_fips_state <- all_fips %>%
    filter(geography_name %in% c("United States", "District of Columbia", state.name)) %>%
    filter(geography != "11001") %>%
    select(geography, geography_name)

  # ---------------------------------------------------------------------------
  # County FIPS lookup
  # ---------------------------------------------------------------------------
  all_fips_county <- all_fips %>%
    filter(!(geography_name %in% state.name)) %>%
    mutate(
      geography_name_clean = geography_name,
      geography_name_clean = gsub(' City and Borough$', '', geography_name_clean),
      geography_name_clean = gsub(' County$',           '', geography_name_clean),
      geography_name_clean = gsub(' Parish$',           '', geography_name_clean),
      geography_name_clean = gsub(' Borough$',          '', geography_name_clean),
      geography_name_clean = gsub(' Census Area$',      '', geography_name_clean),
      geography_name_clean = gsub(' Municipality$',     '', geography_name_clean),
      geography_name_clean = paste0(geography_name_clean, ', ', state),
      geography_name_clean = toupper(geography_name_clean)
    ) %>%
    dplyr::select(geography, geography_name = geography_name_clean) %>%
    distinct(geography_name, .keep_all = TRUE)

  county_manual_crosswalk <- tibble::tribble(
    ~epic_name,                        ~fips_name,
    "SALEM, VA",                       "SALEM CITY, VA",
    "RADFORD, VA",                     "RADFORD CITY, VA",
    "ANCHORAGE, AK",                   "ANCHORAGE, AK",
    "FAIRBANKS NORTH STAR, AK",        "FAIRBANKS NORTH STAR, AK",
    "JUNEAU, AK",                      "JUNEAU, AK",
    "KENAI PENINSULA, AK",             "KENAI PENINSULA, AK",
    "MATANUSKA SUSITNA, AK",           "MATANUSKA-SUSITNA, AK",
    "BETHEL, AK",                      "BETHEL, AK",
    "NOME, AK",                        "NOME, AK",
    "NORTH SLOPE, AK",                 "NORTH SLOPE, AK",
    "NORTHWEST ARCTIC, AK",            "NORTHWEST ARCTIC, AK",
    "SOUTHEAST FAIRBANKS, AK",         "SOUTHEAST FAIRBANKS, AK",
    "YUKON KOYUKUK, AK",               "YUKON-KOYUKUK, AK",
    "VALDEZ CORDOVA, AK",              "VALDEZ-CORDOVA, AK",
    "KUSILVAK, AK",                    "KUSILVAK, AK",
    "PRINCE OF WALES HYDER, AK",       "PRINCE OF WALES-HYDER, AK",
    "HOONAH ANGOON, AK",               "HOONAH-ANGOON, AK",
    "LAKE AND PENINSULA, AK",          "LAKE AND PENINSULA, AK",
    "KODIAK ISLAND, AK",               "KODIAK ISLAND, AK",
    "KETCHIKAN GATEWAY, AK",           "KETCHIKAN GATEWAY, AK",
    "ALEUTIANS EAST, AK",              "ALEUTIANS EAST, AK",
    "ALEUTIANS WEST, AK",              "ALEUTIANS WEST, AK",
    "BRISTOL BAY, AK",                 "BRISTOL BAY, AK",
    "DILLINGHAM, AK",                  "DILLINGHAM, AK",
    "DENALI, AK",                      "DENALI, AK",
    "SITKA, AK",                       "SITKA, AK",
    "WRANGELL, AK",                    "WRANGELL, AK",
    "PETERSBURG, AK",                  "PETERSBURG, AK",
    "HAINES, AK",                      "HAINES, AK",
    "SKAGWAY, AK",                     "SKAGWAY, AK",
    "YAKUTAT, AK",                     "YAKUTAT, AK",
    "ST JOSEPH, MI",                   "ST. JOSEPH, MI",
    "ST JOSEPH, IN",                   "ST. JOSEPH, IN",
    "LA SALLE, IL",                    "LASALLE, IL",
    "LA PORTE, IN",                    "LAPORTE, IN"
  )

  normalize_county_name <- function(county) {
    county <- toupper(trimws(county))
    county <- ifelse(county %in% c("TOTAL", "NONE OF THE ABOVE", ""), NA_character_, county)
    county <- gsub("^SAINTE ", "STE. ", county)
    county <- gsub("^SAINT ",  "ST. ",  county)
    county <- gsub("^ST ",     "ST. ",  county)
    county <- gsub("PRINCE GEORGES,",     "PRINCE GEORGE'S,", county)
    county <- gsub("QUEEN ANNES,",        "QUEEN ANNE'S,",    county)
    county <- gsub("ST\\. MARYS,",        "ST. MARY'S,",      county)
    county <- gsub("OBRIEN,",             "O'BRIEN,",         county)
    county <- gsub("^DE KALB,",           "DEKALB,",          county)
    county <- gsub("MATANUSKA SUSITNA,",  "MATANUSKA-SUSITNA,",    county)
    county <- gsub("PRINCE OF WALES HYDER,", "PRINCE OF WALES-HYDER,", county)
    county <- gsub("YUKON KOYUKUK,",      "YUKON-KOYUKUK,",   county)
    county <- gsub("HOONAH ANGOON,",      "HOONAH-ANGOON,",   county)
    return(county)
  }

  # ---------------------------------------------------------------------------
  # 1. Decrypt xlsx -> CSV
  # ---------------------------------------------------------------------------
  dm_csv <- "raw/diabetes_decrypted.csv"
  ob_csv <- "raw/obesity_decrypted.csv"
  export_xlsx_batch(
    list(list(xlsx = dm_path, csv = dm_csv),
         list(xlsx = ob_path, csv = ob_csv)),
    EXCEL_PASSWORD
  )

  # ---------------------------------------------------------------------------
  # 2. Parsing helpers (state-level xlsx)
  # ---------------------------------------------------------------------------
  # 11 time periods: full years 2016-2025 + partial Jan 1 - Apr 14 2026
  year_dates <- c(
    "2016-01-01", "2017-01-01", "2018-01-01", "2019-01-01", "2020-01-01",
    "2021-01-01", "2022-01-01", "2023-01-01", "2024-01-01", "2025-01-01",
    "2026-01-01"
  )

  clean_age <- function(x) {
    x %>%
      sub("^Less than (\\d+) Years$",             "<\\1 Years",    .) %>%
      sub("^≥ (\\d+) and < (\\d+) Years$",   "\\1-\\2 Years", .) %>%
      sub("^(\\d+) Years or more$",               "\\1+ Years",    .) %>%
      sub("^≥ (\\d+) Years$",                 "\\1+ Years",    .)
  }

  # Read a decrypted CSV and return a long-format data frame.
  #
  # File layout (after skipping header rows):
  #   col 1:    age (merged cells — fill down)
  #   col 2:    state name
  #   cols 3-13:  measure group 1, one column per year (11 years)
  #   cols 14-24: measure group 2
  #   cols 25-35: measure group 3
  #
  # measure_names: length-3 character vector naming the three output columns.
  parse_xlsx_csv <- function(csv_path, skip_rows, measure_names) {
    n_yrs     <- 11
    col_names <- c(
      "age", "state_name",
      paste0(measure_names[1], "_yr", 1:n_yrs),
      paste0(measure_names[2], "_yr", 1:n_yrs),
      paste0(measure_names[3], "_yr", 1:n_yrs)
    )

    raw <- readr::read_csv(
      csv_path,
      skip           = skip_rows,
      col_names      = FALSE,
      col_types      = readr::cols(.default = readr::col_character()),
      show_col_types = FALSE
    )

    raw <- raw[, 1:min(35, ncol(raw))]
    names(raw) <- col_names[1:ncol(raw)]

    raw %>%
      mutate(
        age        = na_if(trimws(age),        ""),
        state_name = na_if(trimws(state_name), "")
      ) %>%
      tidyr::fill(age, .direction = "down") %>%
      filter(
        state_name %in% c(state.name, "District of Columbia") |
          grepl("^Total:", state_name)
      ) %>%
      mutate(
        age            = if_else(grepl("Total\\*", state_name), "Total", age),
        age            = if_else(grepl("Total", age), "Total", age),
        age            = clean_age(age),
        age            = case_when(
          age == "1-5 Years"   ~ "1-4 Years",
          age == "5-18 Years"  ~ "5-17 Years",
          age == "18-25 Years" ~ "18-24 Years",
          age == "18-50 Years" ~ "18-49 Years",
          age == "25-35 Years" ~ "25-34 Years",
          age == "35-45 Years" ~ "35-44 Years",
          age == "45-55 Years" ~ "45-54 Years",
          age == "50-65 Years" ~ "50-64 Years",
          age == "55-65 Years" ~ "55-64 Years",
          TRUE                 ~ age
        ),
        geography_name = if_else(grepl("^Total:", state_name),
                                 "United States", state_name)
      ) %>%
      left_join(all_fips_state, by = "geography_name") %>%
      filter(!is.na(geography)) %>%
      pivot_longer(
        cols          = matches("_yr\\d+$"),
        names_to      = c(".value", "yr_idx"),
        names_pattern = "^(.+)_(yr\\d+)$"
      ) %>%
      mutate(
        yr_num          = as.integer(sub("yr", "", yr_idx)),
        time            = year_dates[yr_num],
        # suppressed_flag = as.integer(
        #   trimws(as.character(.data[[measure_names[3]]])) == "10 or fewer"
        # )
      ) %>%
      mutate(across(
        all_of(measure_names),
        ~ {
          v <- na_if(na_if(trimws(as.character(.x)), ""), "-")
          v[!is.na(v) & v == "10 or fewer"] <- "5"
          as.numeric(gsub("%", "", v))
        }
      )) %>%
      select(age, geography, time, all_of(measure_names))
  }

  # ---------------------------------------------------------------------------
  # 3. Parse diabetes  (skip 13 header rows; data starts at row 14)
  #    Measure groups: A1c >= 6.5% | Diabetes CCW definition % | N patients
  # ---------------------------------------------------------------------------
  dm <- parse_xlsx_csv(
    dm_csv,
    skip_rows     = 13,
    measure_names = c("diabetes_a1c_6_5", "diabetes_dx_ccw", "n_patients_chronic")
  )

  # ---------------------------------------------------------------------------
  # 4. Parse obesity  (skip 14 header rows; file has extra "Session Description" row)
  #    Measure groups: Obesity CCW definition % | BMI >= 30% | N patients
  # ---------------------------------------------------------------------------
  ob <- parse_xlsx_csv(
    ob_csv,
    skip_rows     = 14,
    measure_names = c("obesity_dx_ccw", "obesity_bmi", "n_patients_ob")
  ) # %>%
    # rename(suppressed_flag_ob = suppressed_flag)

  # ---------------------------------------------------------------------------
  # 5. Combine state data and write
  # ---------------------------------------------------------------------------
  combined_state <- full_join(dm, ob, by = c("age", "geography", "time")) %>%
    select(age, geography, time, diabetes_a1c_6_5, diabetes_dx_ccw,
           obesity_bmi, obesity_dx_ccw, n_patients_chronic, n_patients_ob) %>%
    mutate(
      suppressed_diabetes_a1c_6_5    = as.integer(is.na(diabetes_a1c_6_5)),
      suppressed_diabetes_dx_ccw     = as.integer(is.na(diabetes_dx_ccw)),
      suppressed_obesity_bmi         = as.integer(is.na(obesity_bmi)),
      suppressed_obesity_dx_ccw      = as.integer(is.na(obesity_dx_ccw)),
      suppressed_n_patients_chronic  = as.integer(is.na(n_patients_chronic)),
      suppressed_n_patients_ob       = as.integer(is.na(n_patients_ob)),
      across(c(diabetes_a1c_6_5, diabetes_dx_ccw, obesity_bmi, obesity_dx_ccw,
               n_patients_chronic, n_patients_ob),
             ~ replace(.x, is.na(.x), 0))
    ) %>%
    filter(!startsWith(time, "2026")) %>%
    arrange(geography, age, time)

  vroom::vroom_write(combined_state, "standard/state_year.csv.gz", ",")

tmp <- combined_state %>% select(diabetes_a1c_6_5, suppressed_diabetes_a1c_6_5, 
                                diabetes_dx_ccw, suppressed_diabetes_dx_ccw, 
                                obesity_bmi, suppressed_obesity_bmi,
                                obesity_dx_ccw, suppressed_obesity_dx_ccw,
                                n_patients_chronic, suppressed_n_patients_chronic,
                                n_patients_ob, suppressed_n_patients_ob
)

  # ---------------------------------------------------------------------------
  # 6. County-level import function (CSV files from staging_chronic)
  # ---------------------------------------------------------------------------
  chronic_import_county <- function(fileset) {
    col_spec <- readr::cols(.default = readr::col_character())
    a1 <- read_csv(fileset, skip = 11, show_col_types = FALSE,
                   col_types = col_spec) |>
      rename(
        lab                = "...3",
        ccw                = "...4",
        n_patients_chronic = "...5",
        geography_name     = "County of Residence",
        age                = "Age at Encounter in Years"
      ) %>%
      tidyr::fill(geography_name, age, .direction = "down")

    a1 %>%
      mutate(
        age = gsub("≥ ", "", age),
        age = gsub(" and < ", "-", age),
        age = gsub("Less than ", "<", age),
        age = gsub("65 Years or more", "65+ Years", age),
        age = if_else(grepl("Total", age), "Total", age),

       # suppressed_flag    = as.integer(
       #  trimws(n_patients_chronic) == "10 or fewer"
       #),
        ccw                = as.numeric(na_if(gsub("%", "", ccw), "-")),
        lab                = as.numeric(na_if(gsub("%", "", lab), "-")),
        n_patients_chronic = as.numeric(if_else(
          trimws(n_patients_chronic) == "10 or fewer", "5",
          n_patients_chronic
        )),

        geography_name            = gsub("Total", "United States", geography_name),
        geography_name_normalized = normalize_county_name(geography_name)
      ) %>%
      filter(!is.na(geography_name_normalized)) %>%
      left_join(county_manual_crosswalk, by = c("geography_name_normalized" = "epic_name")) %>%
      mutate(county_for_fips = coalesce(fips_name, geography_name_normalized)) %>%
      left_join(all_fips_county, by = c("county_for_fips" = "geography_name"),
                relationship = "many-to-one") %>%
      mutate(
        yearset = str_extract(fileset, "\\d{4}(?=\\.csv)"),
        time    = paste0(yearset, "-01-01")
      ) %>%
      dplyr::select(age, geography, time, ccw, lab,
                    n_patients_chronic) %>% #, suppressed_flag
      filter(!is.na(geography))
  }

  # ---------------------------------------------------------------------------
  # 7. Process county files and combine
  # ---------------------------------------------------------------------------
  all_diabetes_county <- lapply(stage_a1c_county, chronic_import_county) %>%
    bind_rows() %>%
    rename(diabetes_a1c_6_5 = lab,
           diabetes_dx_ccw  = ccw)

  all_obesity_county <- lapply(stage_obesity_county, chronic_import_county) |>
    bind_rows() |>
    rename(obesity_bmi          = lab,
           obesity_dx_ccw       = ccw,
           n_patients_ob_county = n_patients_chronic) |> #,           suppressed_flag_ob   = suppressed_flag
    unique()

  combined_county <- all_obesity_county |>
    full_join(all_diabetes_county, by = c("geography", "time", "age")) |>
    select(age, geography, time, diabetes_a1c_6_5, diabetes_dx_ccw,
           obesity_bmi, obesity_dx_ccw, n_patients_chronic,
           n_patients_ob_county) |>
    mutate(
      suppressed_diabetes_a1c_6_5      = as.integer(is.na(diabetes_a1c_6_5)),
      suppressed_diabetes_dx_ccw       = as.integer(is.na(diabetes_dx_ccw)),
      suppressed_obesity_bmi           = as.integer(is.na(obesity_bmi)),
      suppressed_obesity_dx_ccw        = as.integer(is.na(obesity_dx_ccw)),
      suppressed_n_patients_chronic    = as.integer(is.na(n_patients_chronic)),
      suppressed_n_patients_ob_county  = as.integer(is.na(n_patients_ob_county)),
      across(c(diabetes_a1c_6_5, diabetes_dx_ccw, obesity_bmi, obesity_dx_ccw,
               n_patients_chronic, n_patients_ob_county),
             ~ replace(.x, is.na(.x), 0))
    ) |>
    filter(!startsWith(time, "2026")) |>
    arrange(geography, age, time)

  vroom::vroom_write(combined_county, "standard/county_year.csv.gz", ",")

  # ---------------------------------------------------------------------------
  # 8. Update process record
  # ---------------------------------------------------------------------------
  process$raw_state <- current_state
  dcf::dcf_process_record(updated = process)
}
