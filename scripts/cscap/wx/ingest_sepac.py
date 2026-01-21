"""
http://www.isws.illinois.edu/warm/datatype.asp
"""

import pandas as pd
import psycopg2
from pyiem.datatypes import speed

pgconn = psycopg2.connect(database="sustainablecorn", host="iemdb")

station = "SEPAC"
fn = "sepac.xlsx"

df = pd.read_excel(fn)
print(df.columns)
df["sknt"] = speed(
    pd.to_numeric(df["Wind Speed(mph)"], errors="coerce"), "MPH"
).value("KT")
df["high"] = pd.to_numeric(
    df["Maximum Air Temperature(degF)"], errors="coerce"
)
df["low"] = pd.to_numeric(df["Minimum Air Temperature(degF)"], errors="coerce")
df["pday"] = df["Precipitation(inch)"]
df["srad"] = df["Solar Radiation(MJsqm)"]

print("fn: %s valid: %s - %s" % (fn, df["date"].min(), df["date"].max()))
cursor = pgconn.cursor()
cursor.execute(
    """DELETE from weather_data_daily where station = %s
    and valid >= %s and valid <= %s
    """,
    (station, df["date"].min(), df["date"].max()),
)
if cursor.rowcount > 0:
    print("Deleted %s rows" % (cursor.rowcount,))

for _i, row in df.iterrows():
    cursor.execute(
        """INSERT into weather_data_daily
    (station, valid, high, low, precip, sknt, srad_mj) VALUES (%s,
    %s, %s, %s, %s, %s, %s)
    """,
        (
            station,
            row["date"],
            row["high"],
            row["low"],
            row["pday"],
            row["sknt"],
            row["srad"],
        ),
    )
cursor.close()
pgconn.commit()
