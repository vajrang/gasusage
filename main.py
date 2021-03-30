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

df = degdays.merge(gasusage, how='left', left_index=True, right_index=True)
df = df.merge(degrees, how='left', left_index=True, right_index=True)
df.dropna(inplace=True)

l = LinearRegression().fit(np.array(df['degrees']).reshape(-1, 1), df['usage_ccf'])
DAILY_CCF_USAGE_WITH_NO_HEATING = 0.376  #obtained from 2020 summer gas readings
balance_point = (DAILY_CCF_USAGE_WITH_NO_HEATING - l.intercept_) / l.coef_[0]
print(f'Balance point: {balance_point:.1f}° F')

sns.set(rc={'figure.figsize': (12, 8)})
fig, ax = plt.subplots()
ax.set(xlim=(0, 70), ylim=(0, 7))
sns.regplot(x=df['degrees'], y=df['usage_ccf'], truncate=False)
fig.savefig('./balpoint.png')
plt.show()
