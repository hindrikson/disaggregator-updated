# Main functions

## Consumption
For the consumption data we use the *get_consumption_data()*. This function has as the main function the
*get_consumption_data_historical_and_future()*, which calls several sub-functions
to load and process data from various sources. We break down this function below:

- get_consumption_data_historical_and_future()
    * get_ugr_data_ranges()
        - loads the GENESIS file (data/raw/dimensionless/ugr_2000to2020.csv) with the official national-level starting point for 48 sector ranges, 
          which then later in the main function gets refined and disaggregated into the final 88 sectors × 400 regions output. 
    * get_employees_per_industry_sector_and_regional_ids()
        - get_historical_employees_by_industry_sector_and_regional_id()
            * get_historical_employees()
                - returns a dataframe from opendata.ffe API (id_spatial=18) with historical number of employees per industry sector (WZ2008) and regional id, observed on the moth 9 
                  of the given year. If the year is between 2000 and 2008, data from from 2008 is used. Max year is 2018.
            * get_future_employees()
                - returns a dataframe from opendata.ffe API (id_spatial=27) with number of employees py district and enconomic sector from 2012 to 2035. If
                  the year is bigger than 2035, it returns data from 2035.
        - returns a dataframe with number of employees per industry sector (88) and regional id (400) for a given year.
    * resolve_ugr_industry_sector_ranges_by_employees()
        - distributes the enery consumption from the 48 UGR industry sectors to the 88 WZ2008 industry sectors, 
          based on the share of employees.
            - You have the total evergy between a sector range, and you distribute to the sectors inside the range based on the share of employees.
        - returns a dataframe with wz code and their consumption values for the given year.
    * load_decomposition_factors_gas() 
        - skipping...
    * get_total_gas_industry_self_consuption()
        - skipping...
    * load_decomposition_factors_power()
        - the power consumption in each wz is distributed according to share 
          of certain applications within the industry (lighting, heating, IT equipment, air conditioning, etc.)
        - this decomposition are loaded from data/raw/dimensionless/decomposition_factors.xlsx and sheet "Endenergieverbrauch Strom".
          I do not know yet where these data come from.
          **Sample values:**
            | WZ | Beleuchtung | IKT      | Klimakälte | Prozesskälte | Mechanische Energie |
            |----|-------------|----------|------------|--------------|---------------------|
            | 1  | 0.255814    | 0.046512 | 0.093023   | 0.023256     | 0.418605            |
    * calculate_self_generation()
        - returns a dataframe with an extra column for self-generation (power_incl_selfgen MWh and gas_no_selfgen MWh).
          The self generation is based on decomposition_factors dataframe from the function above,
          which includes the share of "electricity_self_generation"
    * get_regional_energy_consumption()
        - get_manufacturing_energy_consumption
            - requests the the table 15 from demandregion_spatial API - Energy consumption by manufacturing, mining and quarrying industries (German Districts)
              or "Jahreserhebung über die Energieverwendung im Verarbeitenden Gewerbe sowie im Bergbau und in der Gewinnung von Steinen und Erden (Landkreise)"
              The table includes data from 2003 up till 2017 (below 2003, uses data from 2003; above 2017, uses data from 2017)
        - returns a dataframe with regional energy consumption for gas and power per region_id
    * calculate_iteratively_industry_regional_consumption()
        - Resolves the consumption per industry_sector (from UGR) to regional_ids (with the help of JEVI) in an iterative approach.
          This applies only to the industry sector with heavy energy consumption; CTS industry sector is resolved by the employees data.
        - The function distributes national industry sector consumption to 400 regions while simultaneously satisfying two constraints:
          National constraint (UGR): Total consumption per industry sector must match UGR data
          Regional constraint (JEVI): Total consumption per region must match JEVI data. It's essentially solving a bi-proportional fitting problem
        - This iterative proportional fitting method ensures consistency with two independent statistical sources:
          Top-down: National sector totals (UGR) - authoritative for industry structure.
          Bottom-up: Regional totals (JEVI) - authoritative for geographic distribution.
          By iterating between these two constraints, the algorithm finds a balanced solution 
          that respects both data sources while using employee distribution as the spatial allocation key.
              - regional_energy_consumption_jevi provides the regional totals - columns total, power, gas, coal, heating_oil, etc, indexed by region_id
              - consumption_data provides the national industry sector totals from UGR - indexed by industry_sector
          - The function first initializes a consumption matrix based on employee distribution, then iteratively scales rows and columns to match UGR and JEVI totals.

