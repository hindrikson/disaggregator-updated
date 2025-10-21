import pandas as pd

from src.pipeline.pipe_consumption import get_consumption_data_historical_and_future
from src.pipeline.pipe_temporal import disaggregate_temporal, get_consumption_data

pd.options.display.max_columns = 50


df = get_consumption_data(2050, "power", force_preprocessing=True)
print(df.head())
print(df.dtypes)

# below it returns the consumption per industry sector (row) and regional id (columns)
# for the whole year (not temporally disaggregated!).
df_power, df_gas, df_petrol = get_consumption_data_historical_and_future(2020)
print(df_power.head())
print(df_power.info())
print(df_power.columns.nlevels)

df_kassel = df_power[6633]
print(df_kassel.head())

# Industry
# returns a multiindex dataframe
df_industry = disaggregate_temporal(
    energy_carrier="power",
    sector="industry",
    year=2020,
    force_preprocessing=True,
    float_precision=10,
)
print(df_industry.columns.nlevels)
df_kassel_industry = df_industry.xs(6633, level="regional_id", axis=1)
df_kassel_industry = df_kassel_industry.sum(axis=1)
df_kassel_industry.name = "industry"
df_kassel_industry.columns = ["industry"]
print(df_kassel_industry.info())
print(df_kassel_industry.head())

# CTS
df_cts = disaggregate_temporal(
    energy_carrier="power",
    sector="cts",
    year=2020,
    force_preprocessing=True,
    float_precision=10,
)
print(df_cts.columns.nlevels)
print(df_cts.head())


df_kassel_cts = df_cts.xs(6633, level="LK", axis=1)
df_kassel_cts = df_kassel_cts.sum(axis=1)
df_kassel_cts.name = "cts"
print(df_kassel_cts.head())

df_kassel = pd.concat([df_kassel_industry, df_kassel_cts], axis=1)
print(df_kassel.tail())

# save df as "kassel_power_consumption"
df_kassel.to_csv("kassel_power_consumption.csv")
