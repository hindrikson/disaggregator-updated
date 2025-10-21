import sys
import os
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))


from src.data_processing.employees import *
from src.data_processing.consumption import *
from src.data_access.api_reader import *
from src.pipeline.pipe_consumption import *
from src.pipeline.pipe_applications import *
from src.data_access.local_reader import *
from src.data_processing.application import *
from src.data_processing.temporal import *
from src.data_processing.temperature import *
from src.pipeline.pipe_temporal import *
from src.pipeline.pipe_heat import *
from src.data_processing.heat import *
from src.data_processing.cop import *


x = 27


if x == 1:


    df1= load_decomposition_factors_temperature_industry()
    print(df1)

elif x == 2:
    df = get_ugr_data_ranges(year=2021)

elif x == 3:
    df = get_manufacturing_energy_consumption(year=2002)

elif x == 4:
    df = get_historical_employees(year=2002)

elif x == 5:
    df = get_historical_employees_by_industry_sector_and_regional_id(year=2015)
    """ checked 
    get_historical_employees_by_industry_sector_and_regional_id(year=2015) returns the same as data.employees_per_branch(year=2015)
    -> 31326316.267379172
    """

elif x == 6:
    df = get_future_employees(year=2040)

elif x == 7:
    df = get_future_employees_by_industry_sector_and_regional_id(year=2033, force_preprocessing=True)
    """ checked
    get_future_employees_by_industry_sector_and_regional_id(year=2033, force_preprocessing=True)  -> 30544075.88298098
    data.employees_per_branch(year=2033) -> 30544075.88298098 
    """

elif x == 8:
    df = get_employees_per_industry_sector_groups_and_regional_ids(year=2033)

elif x == 9:
    df = get_employees_per_industry_sector_and_regional_ids(year=2015)

elif x == 10:
    df = get_consumption_data(year=2018, energy_carrier="gas")

elif x == 11:
    None

elif x == 12:
    df = disaggregate_temporal(year=2035, sector="cts", energy_carrier="gas")

elif x == 13:
    df = get_total_gas_industry_self_consuption(2015, force_preprocessing=True)

elif x == 14:
    df = get_consumption_data_historical_and_future(year=2035)

elif x == 15:
    df = get_CTS_power_slp(state="BW", year=2015)

elif x == 16:
    #df = disaggregate_temporal_industry(energy_carrier="power", year=2015)

    df = disaggregate_temporal(year=2029, sector="cts", energy_carrier="gas")
    print(df)

elif x == 20: # disagg_daily_gas_slp_cts
    
    year = 2015
    consumption_data = disagg_applications_efficiency_factor(sector="cts", energy_carrier="gas", year=year)
    consumption_data = consumption_data.T.groupby(level=0).sum().T

    daily_temperature_allocation = allocation_temperature_by_day(year=year)

    df = disagg_daily_gas_slp_cts(gas_consumption=consumption_data, state="NI", temperatur_df=daily_temperature_allocation, year=year)
    print(df)

elif x == 21:
    df = sector_fuel_switch_fom_gas_petrol(sector="cts", switch_to="power", year=2029)
    print(df)

elif x == 22:
    df = get_consumption_data(energy_carrier="gas", year=2006, force_preprocessing=True)
    print(df)

elif x == 23:
    switch_to = "power"
    year = 2025
    state = "SL"
    df = temporal_cts_elec_load_from_fuel_switch(year=year, state=state, switch_to=switch_to)
    print(df)

elif x == 24: # create_heat_norm_cts#
    df = create_heat_norm_industry(state="SL", year=2030)
    print(df)

elif x == 25:
    switch_to = "hydrogen"
    year = 2030
    state = "SL"
    sector = "industry"

    #df_gas_switch = sector_fuel_switch_fom_gas_petrol(sector=sector, switch_to=switch_to, year=year)
    #df_temp_gas_switch = disagg_temporal_industry_fuel_switch(df_gas_switch=df_gas_switch, state=state, year=year)
    #print(df_gas_switch)
    df_gas_switch = temporal_industry_elec_load_from_fuel_switch(state=state, switch_to=switch_to, year=year)


elif x == 26:
    year = 2030
    state = "SL"
    df = hydrogen(year=year)
    print(df)

elif x == 27:
    df = disaggregate_temporal(energy_carrier="gas", sector="cts", year=2015, force_preprocessing=True)
    print(df)


else:
    print("x is not 1")
    
print("python version: ", sys.version)
print("pandas version: ", pd.__version__)

    