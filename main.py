from datetime import date

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from influxdb import DataFrameClient
from sklearn.linear_model import LinearRegression

from config import *
from outliers import check_outliers

influx = DataFrameClient(
    host=INFLUXDB_HOST,
    port=INFLUXDB_PORT,
    username=INFLUXDB_USER,
    password=INFLUXDB_PASS,
    database=INFLUXDB_DBSE,
)

check_outliers(influx)

todaystr = date.today().isoformat()

degdays: pd.DataFrame = influx.query(
    f"""
        SELECT sum(degdays)/24/6 as "degdays" FROM
            (
                SELECT 65-mean("temperature_F") as "degdays" 
                FROM "rtl433"."autogen"."FT-004B" 
                WHERE time > '2021-01-01'
                AND TIME < '{todaystr}'
                GROUP BY time(10m) fill(linear) 
                TZ('America/New_York')
            )
        GROUP BY time(24h) TZ('America/New_York')
    """,
)['FT-004B']

# approx area under the temperature curve
degrees: pd.DataFrame = influx.query(
    f"""
        SELECT sum(degrees)/24/6 as "degrees" FROM
            (
                SELECT mean("temperature_F") as "degrees" 
                FROM "rtl433"."autogen"."FT-004B" 
                WHERE time >= '2021-01-01'
                AND TIME < '{todaystr}'
                GROUP BY time(10m) fill(linear) 
                TZ('America/New_York')
            )
        GROUP BY time(24h) TZ('America/New_York')
    """,
)['FT-004B']

gasusage: pd.DataFrame = influx.query(
    f"""
        SELECT difference(last("consumption")) / 100 as "usage_ccf"
        FROM "rtlamr"."autogen"."rtlamr"
        WHERE "endpoint_id"='11135205' 
        AND time >= '2021-01-01'
        AND TIME < '{todaystr}'
        GROUP BY time(24h) TZ('America/New_York')
    """,
)['rtlamr']

elecusage: pd.DataFrame = influx.query(
    f"""
        SELECT difference(last("consumption")) / 100 as "usage_kwh"
        FROM "rtlamr"."autogen"."rtlamr"
        WHERE "endpoint_id"='21212177' 
        AND time >= '2021-01-01'
        AND TIME < '{todaystr}'
        GROUP BY time(24h) TZ('America/New_York')
    """,
)['rtlamr']

hvacheat: pd.DataFrame = influx.query(
    f"""
        SELECT sum("auxHeat1") as "heat_secs"
        FROM "ecobee"."autogen"."usage"
        WHERE time >= '2021-01-01'
        AND TIME < '{todaystr}'
        GROUP BY time(24h) TZ('America/New_York')
    """,
)['usage']

hvaccool: pd.DataFrame = influx.query(
    f"""
        SELECT sum("compCool1") as "cool_secs"
        FROM "ecobee"."autogen"."usage"
        WHERE time >= '2021-01-01'
        AND TIME < '{todaystr}'
        GROUP BY time(24h) TZ('America/New_York')
    """,
)['usage']

df = degdays.merge(gasusage, how='left', left_index=True, right_index=True)
df = df.merge(elecusage, how='left', left_index=True, right_index=True)
df = df.merge(degrees, how='left', left_index=True, right_index=True)
df = df.merge(hvacheat, how='left', left_index=True, right_index=True)
df = df.merge(hvaccool, how='left', left_index=True, right_index=True)
df.dropna(inplace=True)

df.loc[(df['heat_secs'] > 0) & (df['cool_secs'] > 0), 'mode'] = 'both'
df.loc[(df['heat_secs'] > 0) & (df['cool_secs'] == 0), 'mode'] = 'heat'
df.loc[(df['heat_secs'] == 0) & (df['cool_secs'] > 0), 'mode'] = 'cool'
df.loc[(df['heat_secs'] == 0) & (df['cool_secs'] == 0), 'mode'] = 'off'

heating_days = df[df['mode'] == 'heat']
heating_off_days = df[df['mode'] != 'heat']

l = LinearRegression().fit(np.array(heating_days['degrees']).reshape(-1, 1), heating_days['usage_ccf'])
DAILY_CCF_USAGE_WITH_NO_HEATING = 0.376  #obtained from 2020 summer gas readings
balance_point = (DAILY_CCF_USAGE_WITH_NO_HEATING - l.intercept_) / l.coef_[0]

print(f'Balance point: {balance_point:.1f}° F')
sns.set(rc={'figure.figsize': (12, 8)})
fig, ax = plt.subplots()
ax.set(xlim=(0, 70), ylim=(0, 7))
sns.regplot(
    x=heating_days['degrees'],
    y=heating_days['usage_ccf'],
    truncate=False,
    scatter_kws=dict(s=50, linewidth=0, color='#BB6600'),
)
sns.scatterplot(
    x=heating_off_days['degrees'],
    y=heating_off_days['usage_ccf'],
    color='#888888',
    s=50,
    linewidth=0,
)
fig.savefig('./balpoint.png', bbox_inches='tight')
plt.show()

sns.scatterplot(data=df, x='degrees', y='usage_kwh', hue='mode', s=50)
