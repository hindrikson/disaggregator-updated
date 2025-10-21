import pandas as pd

from src.data_processing.effects import *
from src.pipeline.pipe_consumption import *
from src.data_processing.application import *
from src.data_access.local_reader import *
"""
Dissaggregating the consumption data (per industry sector and regional_id) based on their applications.
"""

# main function (with cache)
def disagg_applications_efficiency_factor(sector: str, energy_carrier: str, year: int, force_preprocessing: bool = False) -> pd.DataFrame:
    """    
    Takes the current consumption data and dissaggragates it for applications and applies efficiency enhancement factors
    (equals spacial.disagg_applications_eff() in old code)

    Args:
        sector (str): 'cts' or 'industry'
        energy_carrier (str): 'power' or 'gas'
        year (int): Year from 2000 to 2050

    Returns:
        pd.DataFrame: consumption data with efficiency enhancement factors applied
            Index: regional_id
            MultiIndex columns: 
                level=0: industry_sector
                level=1: application
    """


    # 0. validate the input
    if sector not in ['cts', 'industry']:
        raise ValueError("`sector` must be 'cts' or 'industry'")
    if energy_carrier not in ['power', 'gas', 'petrol']:
        raise ValueError("`energy_carrier` must be 'power', 'gas' or 'petrol'")
    

    # 1. load from cache if not force_preprocessing and cache exists
    if not force_preprocessing:
         consumption_data_with_efficiency_factor = load_consumption_data_with_efficiency_factor_cache(sector=sector, energy_carrier=energy_carrier, year=year)

         if consumption_data_with_efficiency_factor is not None:
            logger.info(f"Loading from cache: disagg_applications_efficiency_factor(sector={sector}, energy_carrier={energy_carrier}, year={year})")
            return consumption_data_with_efficiency_factor

    
    # 2. get consumption data dissaggregated by industry sector and regional_id for a year and energy carrier[power, gas, petrol]
    consumption_data_sectors_regional = get_consumption_data_per_indsutry_sector_energy_carrier(year=year, cts_or_industry=sector, energy_carrier=energy_carrier, force_preprocessing=force_preprocessing)


    # 4. dissaggregate for applications - consumption data is already filtered to contain only relevant industry_sectors(cts/industry)
    consumption_data_dissaggregated = dissaggregate_for_applications(consumption_data=consumption_data_sectors_regional, year=year, sector=sector, energy_carrier=energy_carrier)

    # 5. sanity check
    if not np.isclose(consumption_data_dissaggregated.sum().sum(), consumption_data_sectors_regional.sum().sum()):
        raise ValueError("The sum of the disaggregated consumption must be the same as the sum of the initial consumption data!")

    # 6. apply efficiency effect, for petrol we use gas efficiency enhancement factors
    energy_carrier_eff = energy_carrier
    if energy_carrier == "petrol":
        energy_carrier_eff = "gas"

    consumption_data_with_efficiency_factor = apply_efficiency_factor(consumption_data=consumption_data_dissaggregated, sector=sector, energy_carrier=energy_carrier_eff, year=year)


    # 7. save to cache
    logger.info("Saving to cache...")
    processed_dir = load_config("base_config.yaml")['consumption_data_with_efficiency_factor_cache_dir']
    processed_file = os.path.join(processed_dir, load_config("base_config.yaml")['consumption_data_with_efficiency_factor_cache_file'].format(sector=sector, energy_carrier=energy_carrier, year=year))
    os.makedirs(processed_dir, exist_ok=True)
    consumption_data_with_efficiency_factor.to_csv(processed_file)
    logger.info(f"Cached: disagg_applications_efficiency_factor(sector={sector}, energy_carrier={energy_carrier}, year={year})")
       

    return consumption_data_with_efficiency_factor



