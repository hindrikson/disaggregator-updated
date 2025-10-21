# %% [markdown]
# # DemandRegio Disaggregator

# The DamendRegio project aims to disaggregate the final energy consumption of the sectors:
# - Industry
# - Commercial, Trade and Services (CTS)
# - Private Household
# into a high spacial and temporal resolution.
#
# This project includes:
# - The energy carriers: power, gas, petrol products for the industry and CTS sector
# - Electricity consumption of the private households for electric vehicles

# %% [markdown]
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

