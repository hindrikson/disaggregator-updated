# Temporal

The function **disaggregate_temporal()** disaggregates the temporal data for a given energy carrier and sector. Its main functions are: 
- disagg_applications_efficiency_factor()
- disaggregate_temporal_industry()
- specific functions for disaggregating for CTS

Below is a detailed explanation from the top-level function down to its sub-functions.

- disaggregate_temporal()
    - disagg_applications_efficiency_factor()
        - get_consumption_data_per_indsutry_sector_energy_carrier()
            - Uses the get_consumption_data() function as explained in the conumption.md documentation.
            - Filter the consumption data for the given energy carrier and sector (industry or CTS).
        - dissagregate_for_applications()
            - get_application_dissaggregation_factors()
                * returns the application dissaggregation factors for a fiven industry and energy carrier. 
                * For the industry sector == 'industry', and energy_carrier == 'power' the funcion calls:
                    * load_decomposition_factors_power()
                        - the power consumption in each wz is distributed according to share 
                          of certain applications within the industry (lighting, heating, IT equipment, air conditioning, etc.)
                        - this decomposition is loaded from data/raw/dimensionless/decomposition_factors.xlsx and sheet "Endenergieverbrauch Strom".
                          I do not know yet where these data come from.
                          **Sample values:**
                            | WZ | Beleuchtung | IKT      | Klimakälte | Prozesskälte | Mechanische Energie |
                            |----|-------------|----------|------------|--------------|---------------------|
                            | 1  | 0.255814    | 0.046512 | 0.093023   | 0.023256     | 0.418605            |
                    * load_decomposition_factors_temperature_industry()
                        - the power consumption of industries from 5-33 is distributed according to temperature-dependent applications (heating, cooling, etc.)
                        - this decomposition is loaded from data/raw/dimensionless/decomposition_factors.xlsx and sheet "Endenergieverbrauch Strom".
                          I do not know yet where these data come from.
                          **sample_values:**:
                        - | industry_sectors | process_heat_below_100C | process_heat_100_to_200C | process_heat_200_to_500C | process_heat_above_500C |
                          |---|----------|----------|----------|-----|
                          | 5 | 0.103753 | 0.666667 | 0.229581 | 0.0 |
             The **dissagregate_for_applications()** then returns a dataframe with both the power fators and temperature factors merged.
             Sample:
             
                 | industry_sectors | lighting | information_communication_technology | space_cooling | process_cooling | mechanical_energy | space_heating | hot_water | process_heat_below_100C | process_heat_100_to_200C | process_heat_200_to_500C | process_heat_above_500C |
                 |------------------|----------|--------------------------------------|---------------|-----------------|-------------------|---------------|-----------|-------------------------|--------------------------|--------------------------|-------------------------|
                 | 5                | 0.031746 | 0.015873                             | 0.015873      | 0.0             | 0.888889          | 0.0           | 0.0       | 0.004941                | 0.031746                 | 0.010932                 | 0.0                     |
        
        - apply_efficiency_factor()

