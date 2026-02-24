# =============================================================================
# Epic Cosmos Chronic Disease Data Ingestion
# Source: Epic SlicerDicer exports - Diabetes A1C and Obesity BMI
#         (state-level and county-level, annual)
# =============================================================================

library(tidyverse)
library(dcf)

# Load FIPS lookup
all_fips <- vroom::vroom('https://github.com/PopHIVE/Ingest/raw/refs/heads/main/resources/all_fips.csv.gz')

# Process staging data
raw3 <- dcf::dcf_process_epic_staging(cleanup=F, staging_dir = "raw/staging_chronic")

process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# 1. Setup: FIPS lookups and file lists
# -----------------------------------------------------------------------------
stage_files <- list.files('./raw/staging_chronic', full.names = T)
stage_files <- stage_files[grep('.csv', stage_files)]

stage_a1c <- stage_files[grep('A1C', stage_files)]
stage_a1c_county <- stage_a1c[grep('County', stage_a1c)]

stage_obesity <- stage_files[grep('OBESITY', stage_files)]
stage_obesity_county <- stage_obesity[grep('County', stage_obesity)]

data_years <- 2018:2025

chronic_files <- list.files('raw/staging_chronic', full.names=T)
chronic_files_state_age_dm <- chronic_files[grep('C02_DM_A1C_State_Age_',chronic_files)]
chronic_files_state_age_obesity <- chronic_files[grep('C03_OBESITY_ICD_BMI_State_Age',chronic_files)]

all_fips_state <- all_fips %>%
  filter(geography_name %in% c('United States','District of Columbia', state.name)) %>%
  filter(geography != '11001')

# -----------------------------------------------------------------------------
# 2. State-level chronic import function
# -----------------------------------------------------------------------------
chronic_import <- function(yearset){
  file_dm = chronic_files_state_age_dm[grep(paste0('_',yearset),chronic_files_state_age_dm)]
  file_obesity = chronic_files_state_age_obesity[grep(paste0('_',yearset),chronic_files_state_age_obesity)]

   a1 <- read_csv(file_dm, skip = 11) %>%
    rename(diabetes_a1c_6_5 = "...3",
           diabetes_dx_ccw = "...4",
           n_patients_chronic = "...5",
           age = "Age at Encounter in Years"
    ) %>%
    rename_with(~ "geography_name",
                .cols = matches("^State")) %>%
    tidyr::fill(geography_name, age, .direction='down') %>%
    mutate(  n_patients_chronic = as.numeric(n_patients_chronic)
             )

  b1 <- read_csv(file_obesity, skip = 11) %>%
    rename(obesity_bmi = "...3",
           obesity_dx_ccw = "...4",
           n_patients_chronic = "...5",
           age = "Age at Encounter in Years"
    ) %>%
    rename_with(~ "geography_name",
                .cols = matches("^State")) %>%
    tidyr::fill(geography_name, age, .direction='down') %>%
    mutate(  n_patients_chronic = as.numeric(n_patients_chronic)
             ) %>%
    dplyr::select(-n_patients_chronic) #avoids merge issues with slight variations in denominator between different datasets

  combined <- full_join(a1, b1)  %>%
    mutate(age = gsub("≥ ","", age),
           age = gsub(" and < ","-", age),
           age = gsub("Less than ", "<", age),
           age = gsub("65 Years or more","65+ Years", age),
           age = if_else(grepl('Total', age),'Total', age),
           diabetes_dx_ccw = as.numeric(gsub('%','', diabetes_dx_ccw)),
           diabetes_a1c_6_5 = as.numeric(gsub('%','', diabetes_a1c_6_5)),
           obesity_dx_ccw = as.numeric(gsub('%','', obesity_dx_ccw)),
           obesity_bmi = as.numeric(gsub('%','', obesity_bmi)),
           geography_name = gsub('Total','United States',geography_name)

    ) %>%
    filter(geography_name %in% c('United States','District of Columbia', state.name))%>%
    left_join(all_fips_state, by='geography_name'
    ) %>%
    mutate(time = paste0(yearset,'-01-01') ) %>%
    dplyr::select(age, geography, time, diabetes_a1c_6_5,diabetes_dx_ccw, obesity_bmi,obesity_dx_ccw,
                  n_patients_chronic) %>%
    filter(!is.na(geography))
  return(combined)
}

# -----------------------------------------------------------------------------
# 3. County FIPS normalization
# -----------------------------------------------------------------------------
all_fips_county = all_fips %>%
  filter(!(geography_name %in% state.name)) %>%
  mutate(
    geography_name_clean = geography_name,
    geography_name_clean = gsub(' City and Borough$','', geography_name_clean),
    geography_name_clean = gsub(' County$','', geography_name_clean),
    geography_name_clean = gsub(' Parish$','', geography_name_clean),
    geography_name_clean = gsub(' Borough$','', geography_name_clean),
    geography_name_clean = gsub(' Census Area$','', geography_name_clean),
    geography_name_clean = gsub(' Municipality$','', geography_name_clean),
    geography_name_clean = paste0(geography_name_clean,', ', state),
    geography_name_clean = toupper(geography_name_clean)
  ) %>%
  dplyr::select(geography, geography_name = geography_name_clean) %>%
  distinct(geography_name, .keep_all = TRUE)

county_manual_crosswalk <- tibble::tribble(
  ~epic_name, ~fips_name,
  "SALEM, VA", "SALEM CITY, VA",
  "RADFORD, VA", "RADFORD CITY, VA",
  "ANCHORAGE, AK", "ANCHORAGE, AK",
  "FAIRBANKS NORTH STAR, AK", "FAIRBANKS NORTH STAR, AK",
  "JUNEAU, AK", "JUNEAU, AK",
  "KENAI PENINSULA, AK", "KENAI PENINSULA, AK",
  "MATANUSKA SUSITNA, AK", "MATANUSKA-SUSITNA, AK",
  "BETHEL, AK", "BETHEL, AK",
  "NOME, AK", "NOME, AK",
  "NORTH SLOPE, AK", "NORTH SLOPE, AK",
  "NORTHWEST ARCTIC, AK", "NORTHWEST ARCTIC, AK",
  "SOUTHEAST FAIRBANKS, AK", "SOUTHEAST FAIRBANKS, AK",
  "YUKON KOYUKUK, AK", "YUKON-KOYUKUK, AK",
  "VALDEZ CORDOVA, AK", "VALDEZ-CORDOVA, AK",
  "KUSILVAK, AK", "KUSILVAK, AK",
  "PRINCE OF WALES HYDER, AK", "PRINCE OF WALES-HYDER, AK",
  "HOONAH ANGOON, AK", "HOONAH-ANGOON, AK",
  "LAKE AND PENINSULA, AK", "LAKE AND PENINSULA, AK",
  "KODIAK ISLAND, AK", "KODIAK ISLAND, AK",
  "KETCHIKAN GATEWAY, AK", "KETCHIKAN GATEWAY, AK",
  "ALEUTIANS EAST, AK", "ALEUTIANS EAST, AK",
  "ALEUTIANS WEST, AK", "ALEUTIANS WEST, AK",
  "BRISTOL BAY, AK", "BRISTOL BAY, AK",
  "DILLINGHAM, AK", "DILLINGHAM, AK",
  "DENALI, AK", "DENALI, AK",
  "SITKA, AK", "SITKA, AK",
  "WRANGELL, AK", "WRANGELL, AK",
  "PETERSBURG, AK", "PETERSBURG, AK",
  "HAINES, AK", "HAINES, AK",
  "SKAGWAY, AK", "SKAGWAY, AK",
  "YAKUTAT, AK", "YAKUTAT, AK",
  "ST JOSEPH, MI", "ST. JOSEPH, MI",
  "ST JOSEPH, IN", "ST. JOSEPH, IN",
  "LA SALLE, IL", "LASALLE, IL",
  "LA PORTE, IN", "LAPORTE, IN"
)

normalize_county_name <- function(county) {
  county <- toupper(trimws(county))
  county <- ifelse(county %in% c("TOTAL", "NONE OF THE ABOVE", ""), NA_character_, county)
  county <- gsub("^SAINTE ", "STE. ", county)
  county <- gsub("^SAINT ", "ST. ", county)
  county <- gsub("^ST ", "ST. ", county)
  county <- gsub("PRINCE GEORGES,", "PRINCE GEORGE'S,", county)
  county <- gsub("QUEEN ANNES,", "QUEEN ANNE'S,", county)
  county <- gsub("ST\\. MARYS,", "ST. MARY'S,", county)
  county <- gsub("OBRIEN,", "O'BRIEN,", county)
  county <- gsub("^DE KALB,", "DEKALB,", county)
  county <- gsub("MATANUSKA SUSITNA,", "MATANUSKA-SUSITNA,", county)
  county <- gsub("PRINCE OF WALES HYDER,", "PRINCE OF WALES-HYDER,", county)
  county <- gsub("YUKON KOYUKUK,", "YUKON-KOYUKUK,", county)
  county <- gsub("HOONAH ANGOON,", "HOONAH-ANGOON,", county)
  return(county)
}

# -----------------------------------------------------------------------------
# 4. County-level chronic import function
# -----------------------------------------------------------------------------
chronic_import_county <- function(fileset){
  a1 <- read_csv(fileset, skip = 11) %>%
    rename(lab =  "...3",
           ccw = "...4",
           n_patients_chronic = "...5",
           geography_name ="County of Residence",
           age = "Age at Encounter in Years"
    ) %>%
    tidyr::fill(geography_name, age, .direction='down')


  combined <-   a1 %>%
    mutate(age = gsub("≥ ","", age),
           age = gsub(" and < ","-", age),
           age = gsub("Less than ", "<", age),
           age = gsub("65 Years or more","65+ Years", age),
           age = if_else(grepl('Total', age),'Total', age),

           ccw = as.numeric(gsub('%','', ccw)),
           lab = as.numeric(gsub('%','', lab)),

           n_patients_chronic = as.numeric(n_patients_chronic),
           geography_name = gsub('Total','United States',geography_name),
           geography_name_normalized = normalize_county_name(geography_name)

    ) %>%
    filter(!is.na(geography_name_normalized)) %>%
    left_join(county_manual_crosswalk, by = c('geography_name_normalized' = 'epic_name')) %>%
    mutate(
      county_for_fips = coalesce(fips_name, geography_name_normalized)
    ) %>%
    left_join(all_fips_county, by = c('county_for_fips' = 'geography_name'), relationship = "many-to-one"
    ) %>%
    mutate(
      yearset = str_extract(fileset, "\\d{4}(?=\\.csv)"),
      time = paste0(yearset,'-01-01') ) %>%
    dplyr::select(age, geography, time, ccw, lab,n_patients_chronic) %>%
    filter(!is.na(geography))
  return(combined)
}

# -----------------------------------------------------------------------------
# 5. Process all years
# -----------------------------------------------------------------------------
all_chronic <- lapply(data_years, chronic_import) %>%
  bind_rows()

vroom::vroom_write(all_chronic,'./raw/hba1c.csv.xz')

all_diabetes_county <- lapply(stage_a1c_county, chronic_import_county) %>%
  bind_rows() %>%
  rename(diabetes_a1c_6_5 = lab,
         diabetes_dx_ccw = ccw)

all_obesity_county <- lapply(stage_obesity_county, chronic_import_county) %>%
  bind_rows() %>%
  rename(obesity_bmi = lab,
         obesity_dx_ccw = ccw)%>%
  unique()

all_chronic_county <- all_obesity_county %>%
  dplyr::select(-n_patients_chronic) %>%
  full_join(all_diabetes_county, by=c('geography', 'time', 'age')
  )

vroom::vroom_write(all_chronic_county,'./raw/hba1c_county.csv.xz')

# -----------------------------------------------------------------------------
# 6. Standardize raw files via dcf_standardize_epic
# -----------------------------------------------------------------------------
files <- list.files("raw", "\\.csv\\.xz", full.names = TRUE)

data <- lapply(files, function(file) {
  d <- vroom::vroom(file, show_col_types = FALSE, guess_max = Inf)
  d2 <- dcf::dcf_standardize_epic(d)

  if('Year' %in% names(d2) & !('time' %in% names(d2)) ){
    first_date <- lubridate::mdy(strsplit(d2$Year, " - ")[[1]][1])
    d2$time = paste0(lubridate::year(first_date), '-01-01')
  }

  if ("geography" %in% names(d2)) {
    d2 <- d2 %>%
      mutate(geography = if_else(geography=='0','00', geography))
  }

  if ("n_obesity_state" %in% names(d2)) {
    d2 <- d2 %>%
      rename(n_patients = n_obesity_state)
  }

  if ("age" %in% names(d2)) {
    d2 <- d2 %>%
      mutate(age = stringr::str_replace(age, "^Less than\\s+(\\d+)", "<\\1 Years"),
             age = stringr::str_replace(age, "^(\\d+) or more$", "≥\\1 Years"),
             age = stringr::str_replace(age, "^≥\\s*(\\d+) and <\\s*(\\d+)$", "\\1-\\2 Years"),
             age = if_else(
               age=="1-5 Years", "1-4 Years",
               if_else( age=="5-18 Years", "5-17 Years",
                        if_else(age=="18-25 Years", "18-24 Years",
                                if_else(age=="18-50 Years", "18-49 Years",
                                        if_else( age=="25-35 Years" , "25-34 Years",
                                                 if_else( age=="35-45 Years", "35-44 Years",
                                                          if_else( age=="45-55 Years", "45-54 Years",
                                                                   if_else( age=="50-65 Years", "50-64 Years",
                                                                            if_else( age=="55-65 Years", "55-64 Years",
                                                                                     if_else( age=="≥65 Years", "65+ Years",
                                                                                              age
                                                                                     ))))))))))
      )
  }

  return(d2)
})
names(data) <- sub("\\..*", "", basename(files))

# -----------------------------------------------------------------------------
# 7. Write standardized outputs
# -----------------------------------------------------------------------------
vroom::vroom_write(data[[c("hba1c")]], "standard/state_year.csv.gz", ",")
vroom::vroom_write(data[[c("hba1c_county")]], "standard/county_year.csv.gz", ",")

vroom::vroom_write(
  Reduce(
    function(a, b) merge(a, b, all = TRUE, sort = FALSE),
    data[c("obesity_state")]
  ),
  "standard/state_no_time.csv.gz",
  ","
)
vroom::vroom_write(data$obesity_county, "standard/county_no_time.csv.gz", ",")

# -----------------------------------------------------------------------------
# 8. Record processed state
# -----------------------------------------------------------------------------
dcf::dcf_process_record(updated = process)
