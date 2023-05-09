"""
http://www.isws.illinois.edu/warm/datatype.asp
"""
import datetime

import pandas as pd
import psycopg2
from pyiem.datatypes import speed

pgconn = psycopg2.connect(database="sustainablecorn", host="iemdb")

station = "ORR"
fn = "ORRDAY.txt"

df = pd.read_table(fn, skiprows=[1])
df["sknt"] = speed(
    pd.to_numeric(df["avg_wind_speed"], errors="coerce"), "MPH"
).value("KT")
df["high"] = pd.to_numeric(df["max_air_temp"], errors="coerce")
df["low"] = pd.to_numeric(df["min_air_temp"], errors="coerce")
df["pday"] = df["precip"]
df["srad"] = df["sol_rad"]
df["date"] = df[["year", "month", "day"]].apply(
    lambda x: datetime.date(x[0], x[1], x[2]), axis=1
)
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

for i, row in df.iterrows():
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
