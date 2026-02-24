# =============================================================================
# Epic Cosmos Respiratory Infections Data Ingestion
# Source: Epic SlicerDicer exports - Weekly ED visits (COVID, Flu, RSV)
#         and monthly RSV test positivity
# =============================================================================

library(tidyverse)
library(dcf)

# Load FIPS lookup
all_fips <- vroom::vroom('https://github.com/PopHIVE/Ingest/raw/refs/heads/main/resources/all_fips.csv.gz')

# Process staging data
raw1 <- dcf::dcf_process_epic_staging(cleanup=F, staging_dir = "raw/staging_resp_infections",)

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

# -----------------------------------------------------------------------------
# 1. Load and standardize all raw .csv.xz files
# -----------------------------------------------------------------------------
files <- list.files("raw", "\\.csv\\.xz", full.names = TRUE)

data <- lapply(files, function(file) {

  d <- vroom::vroom(file, show_col_types = FALSE, guess_max = Inf)

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
# 2. Merge weekly respiratory data
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# 3. RSV test data (monthly)
# -----------------------------------------------------------------------------
monthly_tests <- data[["rsv_tests"]] %>%
  filter(!is.na(age))%>%
  rename(
    epic_pct_j12_j18_tested_rsv = rsv_tests ,
    epic_pct_rsv_pos_tests ='positive_rsv_tests_(%)',
    epic_n_ed_j12_j18 = n_rsv_tests

  ) %>%
  mutate(
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

vroom::vroom_write(data$rsv_tests, "standard/no_geo.csv.gz", ",")

# -----------------------------------------------------------------------------
# 4. Record processed state
# -----------------------------------------------------------------------------
process$vintages <- list(
  weekly.csv.gz = max(unlist(raw_vintages[c(
    "all_encounters",
    "covid",
    "flu",
    "rsv"
  )])),
  no_geo.csv.gz = raw_vintages$rsv_tests
)
dcf::dcf_process_record(updated = process)
