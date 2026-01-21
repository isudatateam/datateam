import datetime
import sys

import pandas as pd
import psycopg2
from pyiem.datatypes import distance, speed, temperature

pgconn = psycopg2.connect(database="sustainablecorn", host="iemdb")


def do_daily(fn):
    df = pd.read_table(fn, sep=" ")
    df["sknt"] = speed(df["WINDSPEED"], "MPS").value("KT")
    df["high"] = temperature(df["TMAX"], "C").value("F")
    df["low"] = temperature(df["TMIN"], "C").value("F")
    df["pday"] = distance(df["PRECIP"], "MM").value("IN")
    df["date"] = df[["YEAR", "MONTH", "DAY"]].apply(
        lambda x: datetime.date(x[0], x[1], x[2]), axis=1
    )
    print("fn: %s valid: %s - %s" % (fn, df["date"].min(), df["date"].max()))
    cursor = pgconn.cursor()
    cursor.execute(
        """DELETE from weather_data_daily where station = 'DPAC'
    and valid >= %s and valid <= %s""",
        (df["date"].min(), df["date"].max()),
    )
    if cursor.rowcount > 0:
        print("Deleted %s rows" % (cursor.rowcount,))
    for _i, row in df.iterrows():
        cursor.execute(
            """INSERT into weather_data_daily
        (station, valid, high, low, precip, sknt) VALUES ('DPAC',
        %s, %s, %s, %s, %s)""",
            (row["date"], row["high"], row["low"], row["pday"], row["sknt"]),
        )
    cursor.close()
    pgconn.commit()


def d(year, month, day, hour):
    delta = datetime.timedelta(days=1)
    ts = datetime.datetime(year, month, day, hour if hour < 24 else 0) + delta
    return ts


def do_hourly(fn):
    df = pd.read_table(fn, sep=" ")
    df["sknt"] = speed(df["WINDSPEED"], "MPS").value("KT")
    df["tmpf"] = temperature(df["TAIR"], "C").value("F")
    df["precip"] = distance(df["PREC"], "MM").value("IN")
    df["valid"] = df[["YEAR", "MONTH", "DAY", "HOUR"]].apply(
        lambda x: d(*x), axis=1
    )
    print("fn: %s valid: %s - %s" % (fn, df["valid"].min(), df["valid"].max()))
    cursor = pgconn.cursor()
    cursor.execute(
        """DELETE from weather_data_obs where station = 'DPAC'
    and valid >= %s and valid <= %s""",
        (df["valid"].min(), df["valid"].max()),
    )
    if cursor.rowcount > 0:
        print("Deleted %s rows" % (cursor.rowcount,))
    for _i, row in df.iterrows():
        cursor.execute(
            """INSERT into weather_data_obs
        (station, valid, tmpf, sknt, precip, srad) VALUES ('DPAC',
        %s, %s, %s, %s, %s)""",
            (
                row["valid"].strftime("%Y-%m-%d %H:%M-05"),
                row["tmpf"],
                row["sknt"],
                row["precip"],
                row["RADIATION"],
            ),
        )
    cursor.close()
    pgconn.commit()


def main():
    # do_daily(sys.argv[1])
    do_hourly(sys.argv[1])


if __name__ == "__main__":
    main()
