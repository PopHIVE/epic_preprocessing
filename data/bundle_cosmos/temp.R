library(tidyverse)
library(dcf)
# Process staging data
all_fips <- vroom::vroom('https://github.com/PopHIVE/Ingest/raw/refs/heads/main/resources/all_fips.csv.gz')

raw3 <- dcf::dcf_process_epic_staging(cleanup=F, staging_dir = "raw/staging_injury_OD_year",
                                      standard_names = c(
                                        opioid_od="OPIOID OD",
                                        heat_related ="Heat",
                                        firearms = "firearms initial"
                                      ),
                                      out_dir = "raw_year",
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
    
    epic_pct_ed_opioid = 100* epic_n_ed_opioid/all_cause,
    epic_pct_ed_firearm = 100* epic_n_ed_firearm/all_cause,
    epic_pct_ed_heat = 100* epic_n_ed_heat/all_cause,
    
    
  ) %>%
  left_join(state_fips, by=c('state'='geography_name')) %>%
  filter(!is.na(age) & !is.na(time) & !is.na(geography))


yearly_injury %>%
  dplyr::select(time, geography, age,epic_n_ed_firearm, epic_pct_ed_firearm,epic_n_ed_opioid,epic_n_ed_heat, epic_pct_ed_opioid,epic_pct_ed_heat, starts_with('suppressed')) %>%
vroom::vroom_write(
  .,
  "standard/yearly_injury.csv.gz",
  ","
)


yearly_heat <- yearly_injury %>%
  dplyr::select(time, geography, state, age, epic_n_ed_heat, epic_pct_ed_heat, suppressed_heat) %>%
  write_csv('./resources/heat_year.csv')














