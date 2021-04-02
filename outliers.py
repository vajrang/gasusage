from influxdb import DataFrameClient

from config import *


def check_outliers(influx: DataFrameClient):
    all_df = influx.query(
        f"""
            SELECT temperature_F
            FROM "rtl433"."autogen"."FT-004B"
            TZ('America/New_York') 
        """
    )['FT-004B']
    all_df['diff'] = all_df['temperature_F'].diff()
    all_df['pct_change'] = all_df['temperature_F'].pct_change()

    # values determined as of 3/30/2021, adjust if necessary
    assert all_df['diff'].abs().max() < 5.5
    assert all_df['pct_change'].abs().max() < 0.35
