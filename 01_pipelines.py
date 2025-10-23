# %% [markdown]
# # DemandRegio Disaggregator
#
# The DamendRegio project aims to disaggregate the final energy consumption of the sectors:
# - Industry
# - Commercial, Trade and Services (CTS)
# - Private Household
# into a high spacial and temporal resolution.
#
# This project includes:
# - The energy carriers: power, gas, petrol products for the industry and CTS sector
# - Electricity consumption of the private households for electric vehicles
#
#
# ## Structure of the Disaggregator
# ### General
# The Disaggregator is structured in the following way:
# - src/pipeline/*: Contains the main functions of the Disaggregator. Pipeline functions combining the data_processing functions to generate wanted the data
# - src/data_processing/*: Contains the data manipulation functions
# - src/configs/*: Contains the configuration files and mappings
# - data/raw/*: Contains the raw input data
# - src/data_access/*: Contains the data access functions (API client and local file reader)
# - src/utils/*: Contains the utility and execution functions
# - data/processed/*: Contains cached data to avoid recomputing the data
# - data/output/*: Contains the output data


# %% [markdown]
# # Pipelines
# src/pipeline/*: Contains the main functions of the Disaggregator. Pipeline functions combining the data_processing functions to generate wanted the data

# %% [markdown]
# ## Consumption: `src/pipeline/pipe_consumption.py`:
# This files contain the functionalitie to disaggregate the consumption on a level of industry sectors and regional_ids.
# - `get_consumption_data()`: Get the consumption data for a specific year and specific energy carrier.
# - `get_consumption_data_per_indsutry_sector_energy_carrier()`: Get the consumption data for a specific year and specific energy carrier for a specific industry sector (CTS or industry).

# %%
from src.pipeline.pipe_consumption import (
    get_consumption_data,
    get_consumption_data_per_indsutry_sector_energy_carrier,
)

# %%
df_consumption = get_consumption_data(
    year=2020,  # 2000-2050
    energy_carrier="power",
)
df_consumption.head()
# df_consumption["6633"]

# %%
df_filtered = get_consumption_data_per_indsutry_sector_energy_carrier(
    year=2020,
    cts_or_industry="industry",
    energy_carrier="power",
)
df_filtered.head()


# %%
df_kassel = df_

# %% [markdown]
# ## Temporal: `src/pipeline/pipe_temporal.py`:
# Contains the functionalities to disaggregate the consumption on a level of temporal resolution.
# `disaggregate_temporal(...)`: Disaggregates the results from the application pipeline to a temporal resolution. Differentiating between the different sectors and energy carriers.

# %%
# %%time

from src.pipeline.pipe_temporal import disaggregate_temporal

df_temporal = disaggregate_temporal(
    sector="cts",
    energy_carrier="power",
    year=2020,
)
df_temporal.head()

# %% [markdown]
# ## Heat/Fuel Switch Pipeline: `src/pipeline/pipe_heat.py`:
# Handles the transition from fossil fuels (gas and petrol) to clean energy carriers (electricity and hydrogen) for CTS and industry sectors.
#
# Main function:
# - `temporal_elec_load_from_fuel_switch()`: Main entry point that calculates temporal electricity demand profiles needed to replace gas/petrol consumption

# %%
from src.pipeline.pipe_heat import (
    temporal_elec_load_from_fuel_switch,
)

# Calculates the electricity demand that is needed to replace the gas or petrol consumption.
df_elec_fuel_switch = temporal_elec_load_from_fuel_switch(
    year=2030,
    state="HE",
    energy_carrier="petrol",
    sector="industry",
    switch_to="power",  # power or hydrogen
)

# %%
df_elec_fuel_switch.head()

# %% [markdown]
# In other words, 0.000980 MW of electricity is needed to replace the petrol consumption that used for heating water in industry sector "10" in region 6411 at midnight on January 1st, 2030.

# %%
city = "6633"  # Kassel
cols_city = [col for col in df_elec_fuel_switch.columns if str(col).startswith(city)]
df_city = df_elec_fuel_switch[cols_city]
df_city.head()

# %%
# industries in city
industries = df_city.iloc[0, :].unique()
industries

# %%
# sector 10
# Get column names for sector 10
sector = "10"
sector_10_cols = df_city.columns[df_city.iloc[0] == sector]
sector_10 = df_city[sector_10_cols]
sector_10.head()

# %% [markdown]
# ## Applications: `src/pipeline/pipe_applications.py`:
# Contains the functionalities to disaggregate the consumption on a level of applications.
#
# `disagg_applications_efficiency_factor()`: Dissaggregate the consumption data based on the applications and apply efficiency enhancement factors. The function for the effect is in `src/data_processing/effects.py` and called `apply_efficiency_factor()`.

# %%
from src.pipeline.pipe_applications import disagg_applications_efficiency_factor

df_applications = disagg_applications_efficiency_factor(
    sector="industry",
    energy_carrier="power",
    year=2020,
)
df_applications.head()

# %%
df_region = df_applications.loc[9162]  # München
df_region.loc["29"].head(12)  # cars, trucks, vans

# %% [markdown]
# We can see that 45088.920760 MWh is the amount of electrical energy consumed specifically for lighting purposes in industry sector 29 (majufacturing of motor vehicles) in region 9162 (München) for 2020, after accounting for efficiency improvements.
#
# Here's how this value is derived:
#
# 1. Base consumption data: The function starts with total power consumption for industry sector 29 in region 9162
# 2. Application disaggregation: Using get_application_dissaggregation_factors(), it applies decomposition factors that split the total consumption into different applications (lighting, mechanical energy, process heat, etc.)
# 3. Efficiency factors: The apply_efficiency_factor() function then adjusts these values based on year-specific efficiency improvements


# %% [markdown]
# ## EV regional consumption: `src/pipeline/pipe_ev_regional_consumption.py`:
# Contains two approaches to disaggregate and project the power consumption of electric vehicles in private households on a level of 400 regional_ids.
# The KBA approaches (also referred as s1 and s2) are calculating the consumption based on: vehicle stock data * average km driven by EVs * average mwh per km. For the historical calculations both KBA approaches using the same data.The KBA_1 (s1) approach uses the normative goal (15mio EVs by 2030 and only EVs by 2045) and the KBA_2 (s2) approach uses the projected number of EVs from literature to estimate the future vehicle stock.

# %%
from src.pipeline.pipe_ev_regional_consumption import (
    electric_vehicle_consumption_by_regional_id,
)

df_ev_consumption = electric_vehicle_consumption_by_regional_id(
    year=2020,
    szenario="KBA_1",  # KBA_1, KBA_2, UGR
)
df_ev_consumption.head()

# %% [markdown]
# ## EV temporal consumption: `src/pipeline/pipe_ev_temporal.py`:
# Contains the functionalities to disaggregate the consumption on a level of temporal resolution.
# KBA approaches are using the charging profiles of all locations (home, work, public), since the EVs can be charged at all locations.
# The UGR approach is using the charging profiles of the home location since it already only contains the electricity demand in private households.

# %%
from src.pipeline.pipe_ev_temporal import (
    electric_vehicle_consumption_by_region_id_and_temporal_resolution,
)

df_ev_temporal = electric_vehicle_consumption_by_region_id_and_temporal_resolution(
    year=2020,
    szenario="KBA_1",  # KBA_1, KBA_2, UGR
)
df_ev_temporal.head()
