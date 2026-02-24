# =============================================================================
# Epic Cosmos Injury Data Ingestion
# Source: Epic SlicerDicer exports - Opioid OD, Firearm, Heat ED visits
#         (monthly state, yearly state, annual heat county-level)
# =============================================================================

library(tidyverse)
library(dcf)

# Load FIPS lookup
all_fips <- vroom::vroom('https://github.com/PopHIVE/Ingest/raw/refs/heads/main/resources/all_fips.csv.gz')

# Process staging data
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

raw4 <- dcf::dcf_process_epic_staging(cleanup=F, staging_dir = "raw/staging_injury_OD_year",
                                      standard_names = c(
                                        opioid_od="OPIOID OD",
                                        heat_related ="Heat",
                                        firearms = "firearms initial"
                                      ),
                                      out_dir = "raw_year",
)

process <- dcf::dcf_process_record()

# -----------------------------------------------------------------------------
# 1. County FIPS normalization (for heat county data)
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
# 2. State FIPS lookup
# -----------------------------------------------------------------------------
state_fips <- all_fips %>%
  filter(geography_name %in% c(state.name,'District of Columbia', 'United States') & geography != '11001')

# -----------------------------------------------------------------------------
# 3. Monthly injury data (opioid, firearm, heat - state level)
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# 4. Yearly injury data (opioid, firearm, heat - state level)
# -----------------------------------------------------------------------------
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

# Export heat year data for use by other modules
yearly_heat <- yearly_injury %>%
  dplyr::select(time, geography, state, age, epic_n_ed_heat, epic_rate_ed_heat, suppressed_heat) %>%
  write_csv('./resources/heat_year.csv')

# -----------------------------------------------------------------------------
# 5. Heat-related annual rate by county
# -----------------------------------------------------------------------------
heat1 <- vroom::vroom('./raw/heat_related.csv.xz') %>%
  mutate(
    year = sub(".*(\\d{4})$", "\\1", year),
    heat = if_else(heat=='10 or fewer', '5', heat),
    heat = as.numeric(heat),
    total=as.numeric(total),
    heat_suppressed = if_else(heat==5,1,0),
    heat_incidence = heat / total*100000,
    time = paste(year,'01','01', sep='-'),
    county_normalized = normalize_county_name(county)
  ) %>%
  filter(!is.na(county_normalized)) %>%
  left_join(county_manual_crosswalk, by = c('county_normalized' = 'epic_name')) %>%
  mutate(
    county_for_fips = coalesce(fips_name, county_normalized)
  ) %>%
  left_join(all_fips_county, by = c('county_for_fips' = 'geography_name'), relationship = "many-to-one") %>%
  rename(heat_ed_patients = heat,
         total_ed_patients = total,
         heat_ed_incidence = heat_incidence
         ) %>%
  dplyr::select(geography, time, heat_ed_patients, total_ed_patients, heat_ed_incidence, heat_suppressed, county) %>%
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

# -----------------------------------------------------------------------------
# 6. Record processed state
# -----------------------------------------------------------------------------
dcf::dcf_process_record(updated = process)
