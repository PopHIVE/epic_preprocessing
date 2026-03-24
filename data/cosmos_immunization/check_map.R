# =============================================================================
# check_map.R
# PCV vaccination coverage choropleths
#   Part 1: ZIP code level       (standard/data_zip.csv.gz)
#   Part 2: County level         (standard/data_county.csv.gz)
#   Part 3: County base-pt level (standard/data_county_base_pt.csv.gz)
# One PNG per state, faceted by age group with independent color scales
# Run from: data/cosmos_immunization/
# =============================================================================

library(dplyr)
library(ggplot2)
library(tigris)
library(sf)
library(patchwork)
library(vroom)

options(tigris_use_cache = TRUE)

if (!dir.exists("maps")) dir.create("maps")

# Shared plot helper: one panel per age group, assembled into a state PNG
make_state_map <- function(map_data, age_groups, state, geo_label, time_label) {

  state_data <- map_data %>% filter(state_name == state)

  panels <- lapply(age_groups, function(ag) {
    ag_data   <- state_data %>% filter(age == ag)
    val_range <- range(ag_data$pct_pcv, na.rm = TRUE)
    if (any(is.infinite(val_range))) val_range <- c(0, 100)

    ggplot(ag_data) +
      geom_sf(aes(fill = pct_pcv), color = "white", linewidth = 0.1) +
      scale_fill_viridis_c(
        name     = "PCV (%)",
        option   = "plasma",
        na.value = "grey85",
        limits   = val_range,
        labels   = scales::label_number(accuracy = 1)
      ) +
      labs(title = ag) +
      theme_void(base_size = 10) +
      theme(
        plot.title       = element_text(face = "bold", size = 10, hjust = 0.5),
        legend.position  = "bottom",
        legend.key.width = unit(1.5, "cm"),
        legend.title     = element_text(size = 8),
        legend.text      = element_text(size = 7)
      )
  })

  wrap_plots(panels, ncol = 3) +
    plot_annotation(
      title    = paste0("PCV Vaccination Coverage by ", geo_label, " — ", state),
      subtitle = paste("Epic Cosmos |", time_label),
      caption  = paste0(
        "Color scales vary by age group. ",
        "Grey = no data or <10 patients (suppressed)."
      ),
      theme = theme(
        plot.title    = element_text(face = "bold", size = 14),
        plot.subtitle = element_text(color = "grey40"),
        plot.caption  = element_text(color = "grey50", size = 8)
      )
    )
}

# =============================================================================
# PART 1: ZIP code maps
# =============================================================================

message("--- ZIP code maps ---")

data_zip <- vroom::vroom("standard/data_zip.csv.gz", show_col_types = FALSE) %>%
  filter(!is.na(pct_pcv)) %>%
  mutate(geography = sprintf("%05s", geography))

time_label_zip <- unique(data_zip$time)[1]
zips_in_data   <- unique(data_zip$geography)

message("Fetching ZCTA shapefiles...")
zctas_sf <- tigris::zctas(year = 2020, cb = TRUE) %>%
  filter(ZCTA5CE20 %in% zips_in_data) %>%
  select(geography = ZCTA5CE20, geometry) %>%
  st_transform(4326)

message("Fetching state boundaries for ZIP assignment...")
states_sf <- tigris::states(cb = TRUE, year = 2020) %>%
  select(state_name = NAME, geometry) %>%
  st_transform(4326)

# Assign state to each ZCTA via centroid-in-state join
zcta_centroids <- zctas_sf %>%
  st_centroid() %>%
  st_join(states_sf, join = st_within) %>%
  st_drop_geometry() %>%
  select(geography, state_name)

zctas_sf <- zctas_sf %>%
  left_join(zcta_centroids, by = "geography")

map_data_zip   <- zctas_sf %>% left_join(data_zip, by = "geography")
states_zip     <- sort(unique(na.omit(map_data_zip$state_name)))
age_groups_zip <- unique(na.omit(map_data_zip$age))

message("ZIP states: ",     paste(states_zip,     collapse = ", "))
message("ZIP age groups: ", paste(age_groups_zip, collapse = ", "))

for (state in states_zip) {
  p        <- make_state_map(map_data_zip, age_groups_zip, state,
                             "ZIP Code", time_label_zip)
  out_file <- file.path(
    "maps", paste0("pcv_", gsub(" ", "_", tolower(state)), ".png")
  )
  ggsave(out_file, p, width = 15, height = 10, dpi = 150)
  message("Saved: ", out_file)
}

# =============================================================================
# PART 2: County maps
# =============================================================================

message("--- County maps ---")

data_county <- vroom::vroom("standard/data_county.csv.gz", show_col_types = FALSE) %>%
  filter(!is.na(pct_pcv))

time_label_county <- unique(data_county$time)[1]
fips_in_data      <- unique(data_county$geography)

message("Fetching county shapefiles...")
counties_sf <- tigris::counties(cb = TRUE, year = 2020) %>%
  mutate(fips = paste0(STATEFP, COUNTYFP)) %>%
  filter(fips %in% fips_in_data) %>%
  select(geography = fips, state_fips = STATEFP, geometry) %>%
  st_transform(4326)

# Join state names via STATEFP
state_fips_names <- tigris::states(cb = TRUE, year = 2020) %>%
  st_drop_geometry() %>%
  select(state_fips = STATEFP, state_name = NAME)

counties_sf <- counties_sf %>%
  left_join(state_fips_names, by = "state_fips")

map_data_county   <- counties_sf %>% left_join(data_county, by = "geography")
states_county     <- sort(unique(na.omit(map_data_county$state_name)))
age_groups_county <- unique(na.omit(map_data_county$age))

message("County states: ",     paste(states_county,     collapse = ", "))
message("County age groups: ", paste(age_groups_county, collapse = ", "))

for (state in states_county) {
  p        <- make_state_map(map_data_county, age_groups_county, state,
                             "County", time_label_county)
  out_file <- file.path(
    "maps", paste0("pcv_", gsub(" ", "_", tolower(state)), "_county.png")
  )
  ggsave(out_file, p, width = 15, height = 10, dpi = 150)
  message("Saved: ", out_file)
}

# =============================================================================
# PART 3: County base-pt maps
# =============================================================================

message("--- County base-pt maps ---")

data_county_base_pt <- vroom::vroom(
  "standard/data_county_base_pt.csv.gz", show_col_types = FALSE
) %>%
  filter(!is.na(pct_pcv))

time_label_cbp    <- unique(data_county_base_pt$time)[1]
fips_in_data_cbp  <- unique(data_county_base_pt$geography)

message("Fetching county shapefiles for base-pt data...")
counties_sf_cbp <- tigris::counties(cb = TRUE, year = 2020) %>%
  mutate(fips = paste0(STATEFP, COUNTYFP)) %>%
  filter(fips %in% fips_in_data_cbp) %>%
  select(geography = fips, state_fips = STATEFP, geometry) %>%
  st_transform(4326) %>%
  left_join(state_fips_names, by = "state_fips")

map_data_cbp   <- counties_sf_cbp %>% left_join(data_county_base_pt, by = "geography")
states_cbp     <- sort(unique(na.omit(map_data_cbp$state_name)))
age_groups_cbp <- unique(na.omit(map_data_cbp$age))

message("County base-pt states: ",     paste(states_cbp,     collapse = ", "))
message("County base-pt age groups: ", paste(age_groups_cbp, collapse = ", "))

for (state in states_cbp) {
  p        <- make_state_map(map_data_cbp, age_groups_cbp, state,
                             "County (base-pt)", time_label_cbp)
  out_file <- file.path(
    "maps", paste0("pcv_", gsub(" ", "_", tolower(state)), "_county_base_pt.png")
  )
  ggsave(out_file, p, width = 15, height = 10, dpi = 150)
  message("Saved: ", out_file)
}
