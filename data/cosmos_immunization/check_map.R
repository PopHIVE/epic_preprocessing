# =============================================================================
# check_map.R
# ZIP code choropleth of pneumococcal (PCV) vaccination coverage
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

# -----------------------------------------------------------------------------
# 1. Load standard data
# -----------------------------------------------------------------------------
data <- vroom::vroom("standard/data.csv.gz", show_col_types = FALSE) %>%
  filter(!is.na(pct_pcv)) %>%
  mutate(geography = sprintf("%05s", geography))

time_label <- unique(data$time)[1]

# -----------------------------------------------------------------------------
# 2. Get ZCTA shapefiles and assign state via spatial join
# -----------------------------------------------------------------------------
zips_in_data <- unique(data$geography)

message("Fetching ZCTA shapefiles...")
zctas_sf <- tigris::zctas(year = 2020, cb = TRUE) %>%
  filter(ZCTA5CE20 %in% zips_in_data) %>%
  select(geography = ZCTA5CE20, geometry) %>%
  st_transform(4326)

message("Fetching state boundaries...")
states_sf <- tigris::states(cb = TRUE, year = 2020) %>%
  select(state_name = NAME, geometry) %>%
  st_transform(4326)

# Assign state to each ZCTA via centroid-in-state join (faster than full polygon join)
zcta_centroids <- zctas_sf %>%
  st_centroid() %>%
  st_join(states_sf, join = st_within) %>%
  st_drop_geometry() %>%
  select(geography, state_name)

zctas_sf <- zctas_sf %>%
  left_join(zcta_centroids, by = "geography")

# -----------------------------------------------------------------------------
# 3. Join coverage data
# -----------------------------------------------------------------------------
map_data <- zctas_sf %>%
  left_join(data, by = "geography")

states_present <- sort(unique(na.omit(map_data$state_name)))
age_groups     <- unique(na.omit(map_data$age))

message("States in data: ", paste(states_present, collapse = ", "))
message("Age groups: ",     paste(age_groups,     collapse = ", "))

# -----------------------------------------------------------------------------
# 4. Build one plot per state, each age group on its own color scale
# -----------------------------------------------------------------------------
if (!dir.exists("maps")) dir.create("maps")

for (state in states_present) {

  state_data <- map_data %>% filter(state_name == state)

  # One panel per age group with its own color scale
  panels <- lapply(age_groups, function(ag) {

    ag_data <- state_data %>% filter(age == ag)

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

  combined <- wrap_plots(panels, ncol = 3) +
    plot_annotation(
      title    = paste0("PCV Vaccination Coverage by ZIP Code — ", state),
      subtitle = paste("Epic Cosmos |", time_label),
      caption  = "Color scales vary by age group. Grey = no data or <10 patients (suppressed).",
      theme    = theme(
        plot.title    = element_text(face = "bold", size = 14),
        plot.subtitle = element_text(color = "grey40"),
        plot.caption  = element_text(color = "grey50", size = 8)
      )
    )

  out_file <- file.path("maps", paste0("pcv_", gsub(" ", "_", tolower(state)), ".png"))
  ggsave(out_file, combined, width = 15, height = 10, dpi = 150)
  message("Saved: ", out_file)
}
