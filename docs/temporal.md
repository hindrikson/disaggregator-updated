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
            - The *dissagregate_for_applications()* then returns a dataframe with both the power fators and temperature factors merged.
                - | industry_sectors | lighting | information_communication_technology | space_cooling | process_cooling | mechanical_energy | space_heating | hot_water | process_heat_below_100C | process_heat_100_to_200C | process_heat_200_to_500C | process_heat_above_500C |
                  |---|----------|----------|----------|-----|----------|-----|-----|----------|----------|----------|-----|
                  | 5 | 0.031746 | 0.015873 | 0.015873 | 0.0 | 0.888889 | 0.0 | 0.0 | 0.004941 | 0.031746 | 0.010932 | 0.0 |
        
        - apply_efficiency_factor()
            - Returns the consumption with efficiency factors applied.
            * load_efficiency_rate()
                - This file loads the file *data/raw/temporal/Efficiency_Enhancement_Rates_Applications.xlsx*, and returns a dataframe with the effiency rates for a sector (wz) and energy_carrier.
            * The effieciency rates are then used to adjust the consumption data.
        - disaggregate_temporal_industry()
            * get_shift_load_profiles_by_year()
                * get_shift_load_profiles_by_state_and_year()
                    - this function creates load shift prifiles based states holidays, weekdays, weekends days, for predifined shifts: 
                    - s1 (single shift) 08:00:00-16:00:00 for:
                        - S1_WT: working days only
                        - S1_WT_SA: working days + Saturdays
                        - S1_WT_SA_SO: working days + Saturdays + Sundays
                    - and the same for s2 (two shifts) 06:00:00-23:00:00 and s3 24/7
                    - hours outside these shifts receive also a proportion of the load, but much smaller.
            * E.g., for Hessen in 2020, the load shift profiles at 14:00:00 are:
                * | Timestamp           |    S1_WT | S1_WT_SA | S1_WT_SA_SO |    S2_WT | S2_WT_SA | S2_WT_SA_SO |    S3_WT | S3_WT_SA | S3_WT_SA_SO |
                  | ------------------- | -------: | -------: | ----------: | -------: | -------: | ----------: | -------: | -------: | ----------: |
                  | 2020-01-03 14:00:00 | 0.000046 | 0.000044 | 0.000042    | 0.000038 | 0.000036 | 0.000033    | 0.000034 | 0.000031 | 0.000028    |

        - if "cts":
            - skipping...

