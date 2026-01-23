#https://github.com/DISSC-yale/dcf
#remotes::install_github("dissc-yale/dcf")
library(tidyverse)
library(dcf)

# Process staging data
all_fips <- vroom::vroom('https://github.com/PopHIVE/Ingest/raw/refs/heads/main/resources/all_fips.csv.gz')

# if there was staging data, make new standard version from it..this function will automaticaly save relevant file
raw1 <- dcf::dcf_process_epic_staging(cleanup=F, staging_dir = "raw/staging_resp_infections",)

raw2 <- dcf::dcf_process_epic_staging(cleanup=F, staging_dir = "raw/staging_injury_OD",
                                      standard_names = c(
  opioid_od="OPIOID OD",
  heat_related ="Heat",
  firearms = "firearms initial"
)
)


raw2a <- dcf::dcf_process_epic_staging(cleanup=F, staging_dir = "raw/staging_heat_year_county",
                                      standard_names = c(
                                        opioid_od="OPIOID OD",
                                        heat_related ="Heat",
                                        firearms = "firearms initial"
                                      )
)


list.files( "./raw/staging_chronic")
raw3 <- dcf::dcf_process_epic_staging(cleanup=F, staging_dir = "raw/staging_chronic")

raw4 <- dcf::dcf_process_epic_staging(cleanup=F, staging_dir = "raw/staging_injury_OD_year",
                                      standard_names = c(
                                        opioid_od="OPIOID OD",
                                        heat_related ="Heat",
                                        firearms = "firearms initial"
                                      ),
                                      out_dir = "raw_year",
)



process <- dcf::dcf_process_record()
meta_files <- list.files("raw", "json", full.names = TRUE)
raw_vintages <- lapply(
  structure(
    meta_files,
    names = gsub(".*/|\\..*", "", meta_files)
  ),
  function(meta_file) {
    meta <- jsonlite::read_json(meta_file)[[1L]]
    as.character(as.Date(meta[["Date of Export"]], "%m/%d/%Y"))
  }
)


######################################
### PROCESS CHRONIC
######################################
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

# Create a more robust county FIPS lookup that handles name variations
all_fips_county = all_fips %>%
  filter(!(geography_name %in% state.name)) %>%
  mutate(
    # Remove suffixes - ORDER MATTERS! More specific patterns first
    geography_name_clean = geography_name,
    # Alaska consolidated city-boroughs MUST come before Borough removal
    # (e.g., "Juneau City and Borough" -> "Juneau", not "Juneau City and")
    geography_name_clean = gsub(' City and Borough$','', geography_name_clean),
    geography_name_clean = gsub(' County$','', geography_name_clean),
    geography_name_clean = gsub(' Parish$','', geography_name_clean),
    geography_name_clean = gsub(' Borough$','', geography_name_clean),
    geography_name_clean = gsub(' Census Area$','', geography_name_clean),
    geography_name_clean = gsub(' Municipality$','', geography_name_clean),
    # Keep " city" for VA independent cities (e.g., "Salem city" -> "SALEM CITY, VA")
    # The crosswalk maps Epic's "SALEM, VA" to "SALEM CITY, VA" for matching
    geography_name_clean = paste0(geography_name_clean,', ', state),
    geography_name_clean = toupper(geography_name_clean)
  ) %>%
  dplyr::select(geography, geography_name = geography_name_clean) %>%
  # Use distinct() to handle any duplicate geography_name entries
  distinct(geography_name, .keep_all = TRUE)

# Manual crosswalk for county names that can't be fixed with simple text transformations
# These include Virginia independent cities, Alaska census areas, and other special cases
county_manual_crosswalk <- tibble::tribble(
  ~epic_name, ~fips_name,
  # Virginia independent cities (these have their own FIPS codes as county-equivalents)
  "SALEM, VA", "SALEM CITY, VA",
  "RADFORD, VA", "RADFORD CITY, VA",
  # Alaska census areas and boroughs with complex naming
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
  "VALDEZ CORDOVA, AK", "VALDEZ-CORDOVA, AK",  # Note: split into Chugach & Copper River in 2019
  "KUSILVAK, AK", "KUSILVAK, AK",  # Renamed from Wade Hampton in 2015
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
  # St./Saint variations - some may need exact matches
  "ST JOSEPH, MI", "ST. JOSEPH, MI",
  "ST JOSEPH, IN", "ST. JOSEPH, IN",
  # La Salle/La Porte - some states use one-word spelling in FIPS
  "LA SALLE, IL", "LASALLE, IL",
  "LA PORTE, IN", "LAPORTE, IN"
)

# Function to normalize county names from Epic Cosmos data to match FIPS lookup format
# This function is vectorized for use with dplyr::mutate()
normalize_county_name <- function(county) {
  county <- toupper(trimws(county))

  # Mark non-county entries as NA (vectorized)
  county <- ifelse(county %in% c("TOTAL", "NONE OF THE ABOVE", ""), NA_character_, county)

  # SAINT/SAINTE → ST./STE. (order matters - SAINTE before SAINT)
  county <- gsub("^SAINTE ", "STE. ", county)
  county <- gsub("^SAINT ", "ST. ", county)
  county <- gsub("^ST ", "ST. ", county)  # Add period if missing after ST

  # Handle possessive names - add apostrophes
  county <- gsub("PRINCE GEORGES,", "PRINCE GEORGE'S,", county)
  county <- gsub("QUEEN ANNES,", "QUEEN ANNE'S,", county)
  county <- gsub("ST\\. MARYS,", "ST. MARY'S,", county)
  county <- gsub("OBRIEN,", "O'BRIEN,", county)

  # Handle space variations in compound names
  # Note: LA SALLE and LA PORTE are kept as-is since all_fips uses "La Salle" and "La Porte"
  county <- gsub("^DE KALB,", "DEKALB,", county)

  # Alaska borough/census areas - these need special handling
  # Some were renamed: Valdez-Cordova split into Chugach and Copper River (2019)
  # Wade Hampton renamed to Kusilvak (2015)
  county <- gsub("MATANUSKA SUSITNA,", "MATANUSKA-SUSITNA,", county)
  county <- gsub("PRINCE OF WALES HYDER,", "PRINCE OF WALES-HYDER,", county)
  county <- gsub("YUKON KOYUKUK,", "YUKON-KOYUKUK,", county)
  county <- gsub("HOONAH ANGOON,", "HOONAH-ANGOON,", county)

  return(county)
}

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
           # Normalize county names to match FIPS lookup format
           geography_name_normalized = normalize_county_name(geography_name)

    ) %>%
    # Filter out non-county rows
    filter(!is.na(geography_name_normalized)) %>%
    # First try manual crosswalk for special cases
    left_join(county_manual_crosswalk, by = c('geography_name_normalized' = 'epic_name')) %>%
    mutate(
      # Use fips_name from crosswalk if available, otherwise use normalized name
      county_for_fips = coalesce(fips_name, geography_name_normalized)
    ) %>%
    # Then join to FIPS lookup (many-to-one: each county name maps to one FIPS)
    left_join(all_fips_county, by = c('county_for_fips' = 'geography_name'), relationship = "many-to-one"
    ) %>%
    mutate(
      yearset = str_extract(fileset, "\\d{4}(?=\\.csv)"),
      time = paste0(yearset,'-01-01') ) %>%
    dplyr::select(age, geography, time, ccw, lab,n_patients_chronic) %>%
    filter(!is.na(geography))
  return(combined)
}


all_chronic <- lapply(data_years, chronic_import) %>%
  bind_rows()

vroom::vroom_write(all_chronic,'./raw/hba1c.csv.xz') #overwrites incorrect file generated by package while keeping original JSON


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
  dplyr::select(-n_patients_chronic) %>% #there is sometimes a slight mismatch in denominator between intages of data due to system updates; only use denominator from one
  full_join(all_diabetes_county, by=c('geography', 'time', 'age')
  )

vroom::vroom_write(all_chronic_county,'./raw/hba1c_county.csv.xz') #overwrites incorrect file generated by package while keeping original JSON


files <- list.files("raw", "\\.csv\\.xz", full.names = TRUE)

#files <- files[-grep('rsv_tests', files)] #exclude rsv tests for now due to formatting issues


data <- lapply(files, function(file) {
  
  d <- vroom::vroom(file, show_col_types = FALSE, guess_max = Inf)
  
  if(grepl('self_harm',file)){
    d <- d %>%
      rename( n_all_encounters = n_self_harm,
              pct_self_harm = "percent_with_self-harm_dx_(%)")
  }
  
  d2 <- dcf::dcf_standardize_epic(d)
  
  if('month' %in% names(d)){
    d2$time = paste0(d2$time, '-01')
  }
  
  if('Year' %in% names(d2) & !('time' %in% names(d2)) ){
    first_date <- mdy(strsplit(d2$Year, " - ")[[1]][1])
    d2$time = paste0(lubridate::year(first_date), '-01-01')
  }
  
  if ("geography" %in% names(d2)) {
    d2 <- d2 %>%
      mutate(geography = if_else(geography=='0','00', geography) 
      )
  }
  
  if ("time" %in% names(d2)) {
    
    d2 <- d2 %>%
      mutate(time = as.Date(time),
             weekday = weekdays(time),
             time = if_else(weekday=='Sunday', time+6, time), #week end date
             time = as.character(time)
      ) %>%
      dplyr::select(-weekday) 
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


merged_weekly <- Reduce(
  function(a, b) merge(a, b, all = TRUE, sort = FALSE),
  data[c("all_encounters_weekly", "covid", "flu", "rsv")]
)


# add epic_ prefix to all columns except geography, time, age
merged_weekly <- merged_weekly %>%
  filter(!is.na(age)) %>%
  mutate(pct_rsv = 100*n_rsv/n_all_encounters_weekly,
         pct_flu = 100*n_flu/n_all_encounters_weekly,
         pct_covid = 100*n_covid/n_all_encounters_weekly,
         suppressed_flag_rsv = if_else(n_rsv<10,1,0),
         suppressed_flag_flu = if_else(n_flu<10,1,0),
         suppressed_flag_covid = if_else(n_covid<10,1,0),
  ) %>%
  rename_with(~ paste0("epic_", .x), 
              .cols = -c(geography, time, age))%>%
  arrange(geography, age, time) %>%
  group_by(geography, age) %>%
  mutate(time= as.Date(time),
         epic_n_all_encounters_lag1 = lag(epic_n_all_encounters_weekly,1),
         remove = if_else(epic_n_all_encounters_weekly/epic_n_all_encounters_lag1<0.5 &
                            time == max(time, na.rm=T),1,0)
  ) %>%
  filter(remove != 1) %>%
  dplyr::select(-remove, -epic_n_all_encounters_lag1) %>%
  dplyr::select(geography, age, time, starts_with('epic_n'), starts_with('epic_pct'), starts_with('epic_suppressed')  )


vroom::vroom_write(
  merged_weekly,
  "standard/weekly.csv.gz",
  ","
)

process$vintages <- list(
  weekly.csv.gz = max(unlist(raw_vintages[c(
    "all_encounters",
    "covid",
    "flu",
    "rsv"
  )])),
  state_no_time.csv.gz = raw_vintages$self_harm,
  county_no_time.csv.gz = raw_vintages$obesity_county,
  no_geo.csv.gz = raw_vintages$rsv_tests,
  children.csv.gz = raw_vintages$vaccine_mmr
)
dcf::dcf_process_record(updated = process)


#Monthly data ; injury and test data use different age groups so don't merge

state_fips <- all_fips %>%
  filter(geography_name %in% c(state.name,'District of Columbia', 'United States') & geography != '11001')

monthly_injury <-vroom::vroom(
  "raw/opioid_od.csv.xz",
    ) %>%
  mutate(time = as.Date(paste(year, month, '01', sep='-'), format='%Y-%b-%d' ),
         state = if_else(state =='Total', 'United States', state)
  ) %>%
  rename(epic_n_ed_opioid = ed_opioid,
         epic_n_ed_firearm = firearms_initial,
         epic_n_ed_heat = heat,
         all_cause = total
  ) %>%
  mutate(
    all_cause = as.numeric(all_cause),
    epic_n_ed_opioid = if_else(epic_n_ed_opioid == '10 or fewer', '5', epic_n_ed_opioid ),
    epic_n_ed_opioid = as.numeric(epic_n_ed_opioid),
    
    epic_n_ed_firearm = if_else(epic_n_ed_firearm == '10 or fewer', '5', epic_n_ed_firearm ),
    epic_n_ed_firearm = as.numeric(epic_n_ed_firearm),
    
    epic_n_ed_heat = if_else(epic_n_ed_heat == '10 or fewer', '5', epic_n_ed_heat ),
    epic_n_ed_heat = as.numeric(epic_n_ed_heat),
    
    suppressed_opioid = if_else(epic_n_ed_opioid == 5, 1, 0),
    suppressed_firearm = if_else(epic_n_ed_firearm == 5, 1, 0),
    suppressed_heat= if_else(epic_n_ed_heat == 5, 1, 0),
    
    epic_rate_ed_opioid = 100000* epic_n_ed_opioid/all_cause,
    epic_rate_ed_firearm = 100000* epic_n_ed_firearm/all_cause,
    epic_rate_ed_heat = 100000* epic_n_ed_heat/all_cause,
    
        
  ) %>%
  left_join(state_fips, by=c('state'='geography_name')) %>%
  dplyr::select(time, geography, age,epic_n_ed_firearm, epic_rate_ed_firearm,epic_n_ed_opioid,epic_n_ed_heat, epic_rate_ed_opioid,epic_rate_ed_heat, starts_with('suppressed')) %>%
  filter(!is.na(age) & !is.na(time) & !is.na(geography))
  


vroom::vroom_write(
  monthly_injury,
  "standard/monthly_injury.csv.gz",
  ","
)

yearly_injury <-vroom::vroom(
  "raw_year/opioid_od.csv.xz",
) %>%
  mutate(year = stringr::str_extract(year, "\\b\\d{4}\\b"),
         
         time = as.Date(paste(year, '01', '01', sep='-'), format='%Y-%m-%d' ),
         state = if_else(state =='Total', 'United States', state)
  ) %>%
  rename(epic_n_ed_opioid = ed_opioid,
         epic_n_ed_firearm = firearms_initial,
         epic_n_ed_heat = heat,
         all_cause = total
  ) %>%
  mutate(
    all_cause = as.numeric(all_cause),
    epic_n_ed_opioid = if_else(epic_n_ed_opioid == '10 or fewer', '5', epic_n_ed_opioid ),
    epic_n_ed_opioid = as.numeric(epic_n_ed_opioid),
    
    epic_n_ed_firearm = if_else(epic_n_ed_firearm == '10 or fewer', '5', epic_n_ed_firearm ),
    epic_n_ed_firearm = as.numeric(epic_n_ed_firearm),
    
    epic_n_ed_heat = if_else(epic_n_ed_heat == '10 or fewer', '5', epic_n_ed_heat ),
    epic_n_ed_heat = as.numeric(epic_n_ed_heat),
    
    suppressed_opioid = if_else(epic_n_ed_opioid == 5, 1, 0),
    suppressed_firearm = if_else(epic_n_ed_firearm == 5, 1, 0),
    suppressed_heat= if_else(epic_n_ed_heat == 5, 1, 0),
    
    epic_rate_ed_opioid = 100000* epic_n_ed_opioid/all_cause,
    epic_rate_ed_firearm = 100000* epic_n_ed_firearm/all_cause,
    epic_rate_ed_heat = 100000* epic_n_ed_heat/all_cause,
    
    
  ) %>%
  left_join(state_fips, by=c('state'='geography_name')) %>%
  filter(!is.na(age) & !is.na(time) & !is.na(geography))


yearly_injury %>%
  dplyr::select(time, geography, age,epic_n_ed_firearm, epic_rate_ed_firearm,epic_n_ed_opioid,epic_n_ed_heat, epic_rate_ed_opioid,epic_rate_ed_heat, starts_with('suppressed')) %>%
  vroom::vroom_write(
    .,
    "standard/yearly_injury.csv.gz",
    ","
  )


yearly_heat <- yearly_injury %>%
  dplyr::select(time, geography, state, age, epic_n_ed_heat, epic_rate_ed_heat, suppressed_heat) %>%
  write_csv('./resources/heat_year.csv')


monthly_tests <- data[["rsv_tests"]] %>%
  filter(!is.na(age))%>%
  rename(
    epic_pct_j12_j18_tested_rsv = rsv_tests ,
    epic_pct_rsv_pos_tests ='positive_rsv_tests_(%)',
    epic_n_ed_j12_j18 = n_rsv_tests
    
  ) %>%
  mutate(
    #epic_n_ed_j12_j18 = if_else(epic_n_ed_j12_j18 == '10 or fewer', '5', epic_n_ed_j12_j18 ),
    #epic_n_ed_j12_j18 = as.numeric(epic_n_ed_j12_j18),
    suppressed_rsv_test =     if_else(epic_n_ed_j12_j18 == 5, 1, 0),
    
    
    epic_pct_rsv_pos_tests = gsub('%','',epic_pct_rsv_pos_tests),
    epic_pct_rsv_pos_tests = as.numeric(epic_pct_rsv_pos_tests),
    
    epic_pct_j12_j18_tested_rsv = gsub('%','',epic_pct_j12_j18_tested_rsv),
    epic_pct_j12_j18_tested_rsv = as.numeric(epic_pct_j12_j18_tested_rsv)
    
  ) %>%
  dplyr::select(time, geography, age,epic_pct_rsv_pos_tests, epic_pct_j12_j18_tested_rsv,epic_n_ed_j12_j18, suppressed_rsv_test)


vroom::vroom_write(
  monthly_tests,
  "standard/monthly_tests.csv.gz",
  ","
)

merged_yearly <- bind_rows(
  data[[c("hba1c")]], data[[c("hba1c_county")]],
)



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
vroom::vroom_write(data$rsv_tests, "standard/no_geo.csv.gz", ",")



#############
##Heat related annual rate by county
#############

heat1 <- vroom::vroom('./raw/heat_related.csv.xz') %>%
  mutate(
    year = sub(".*(\\d{4})$", "\\1", year),
    heat = if_else(heat=='10 or fewer', '5', heat),
    heat = as.numeric(heat),
    total=as.numeric(total),
    heat_suppressed = if_else(heat==5,1,0),
    heat_incidence = heat / total*100000,
    time = paste(year,'01','01', sep='-'),
    # Normalize county names to match FIPS lookup format
    county_normalized = normalize_county_name(county)
  ) %>%
  # Filter out non-county rows (Total, None of the above)
  filter(!is.na(county_normalized)) %>%
  # First try manual crosswalk for special cases
  left_join(county_manual_crosswalk, by = c('county_normalized' = 'epic_name')) %>%
  mutate(
    # Use fips_name from crosswalk if available, otherwise use normalized name
    county_for_fips = coalesce(fips_name, county_normalized)
  ) %>%
  # Then join to FIPS lookup (many-to-one: each county name maps to one FIPS)
  left_join(all_fips_county, by = c('county_for_fips' = 'geography_name'), relationship = "many-to-one") %>%
  rename(heat_ed_patients = heat,
         total_ed_patients = total,
         heat_ed_incidence = heat_incidence
         ) %>%
  dplyr::select(geography, time, heat_ed_patients, total_ed_patients, heat_ed_incidence, heat_suppressed, county) %>%
  # Rename geography to geography_name for backward compatibility with existing output format
  rename(geography_name = county)

# Report any remaining unmatched counties for debugging
unmatched_counties <- heat1 %>%
  filter(is.na(geography)) %>%
  distinct(geography_name) %>%
  pull(geography_name)
if (length(unmatched_counties) > 0) {
  message("Warning: ", length(unmatched_counties), " unique county names still unmatched after normalization:")
  message(paste(head(unmatched_counties, 10), collapse = ", "))
  if (length(unmatched_counties) > 10) message("... and ", length(unmatched_counties) - 10, " more")
}

write_csv(heat1, './standard/heat_year_county.csv')
vroom::vroom_write(heat1, './standard/heat_year_county.csv.gz')

#vroom::vroom_write(data$vaccine_mmr, "standard/children.csv.gz", ",")

