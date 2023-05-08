from __future__ import print_function

import sys

import pandas as pd
import psycopg2
from pyiem.datatypes import distance, humidity, speed, temperature
from pyiem.meteorology import dewpoint

pgconn = psycopg2.connect(database="sustainablecorn")


def process(sheets):
    resdf = pd.DataFrame(
        {
            "precip": sheets["RainOut"]["Rain_mm_Tot"],
            "tmpf": sheets["TempRHVPOut"]["AirT_C_Avg"],
            "rh": sheets["TempRHVPOut"]["RH"],
            "drct": sheets["WindOut"]["WindDir_D1_WVT"],
            "sknt": sheets["WindOut"]["WS_ms_S_WVT"],
            "srad": sheets["SolarRad1Out"]["Slr_kW_Avg"],
        }
    )
    # Do unit conversion
    resdf["srad"] = resdf["srad"] * 1000.0
    resdf["precip"] = distance(resdf["precip"], "MM").value("IN")
    resdf["tmpf"] = temperature(resdf["tmpf"], "C").value("F")
    resdf["dwpf"] = dewpoint(
        temperature(resdf["tmpf"], "F"), humidity(resdf["rh"], "%")
    ).value("F")
    resdf["sknt"] = speed(resdf["sknt"], "MPS").value("KT")
    print(resdf.describe())
    minval = resdf.index.min()
    maxval = resdf.index.max()
    cursor = pgconn.cursor()
    cursor.execute(
        """DELETE from weather_data_obs WHERE
    valid between '%s-06' and '%s-06' and station = 'HICKS.P'
    """
        % (
            minval.strftime("%Y-%m-%d %H:%M"),
            maxval.strftime("%Y-%m-%d %H:%M"),
        )
    )
    print(
        "DELETED %s rows between %s and %s" % (cursor.rowcount, minval, maxval)
    )
    for valid, row in resdf.iterrows():
        if pd.isnull(valid):
            continue
        cursor.execute(
            """INSERT into weather_data_obs
        (station, valid, tmpf, dwpf, drct, precip, srad, sknt) VALUES
        ('HICKS.P', %s, %s, %s, %s, %s, %s, %s)
        """,
            (
                valid.strftime("%Y-%m-%d %H:%M-06"),
                row["tmpf"],
                row["dwpf"],
                row["drct"],
                row["precip"],
                row["srad"],
                row["sknt"],
            ),
        )
    cursor.close()
    pgconn.commit()


def main():
    sheets = pd.read_excel(sys.argv[1], sheetname=None, skiprows=[0, 2, 3])
    for sheetlabel, df in sheets.iteritems():
        print(sheetlabel)
        df.drop_duplicates(subset="TIMESTAMP", keep="last", inplace=True)
        # if sheetlabel not in ['SoilMoistOut1', 'SoilMoistOut2']:
        #    df['valid'] = [datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
        #                   for x in df['TIMESTAMP']]
        # else:
        #    df['valid'] = df['TIMESTAMP']
        df["valid"] = pd.to_datetime(df["TIMESTAMP"], errors="coerce")
        df.set_index("valid", inplace=True)
        df.sort_index(inplace=True)
        for col in df.columns.values:
            if col in ["TIMESTAMP", "RECORD"] or col.startswith("Unnamed"):
                df.drop(col, axis=1, inplace=True)
        cols = df.columns.values
        print(
            "%s %s"
            % (
                sheetlabel,
                ",".join(
                    cols,
                ),
            )
        )

    process(sheets)


if __name__ == "__main__":
    main()
