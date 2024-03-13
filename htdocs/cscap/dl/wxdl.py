"""Download weather data, please"""

import os

import pandas as pd
from pyiem.datatypes import distance, temperature
from pyiem.util import get_dbconnstr
from pyiem.webutil import ensure_list, iemapp
from sqlalchemy import text

VARDF = {
    "uniqueid": "",
    "day": "",
    "doy": "Day Of Year",
    "sknt": "Wind Speed",
    "drct": "Wind Direction",
    "high": "High Temperature",
    "highc": "High Temperature",
    "low": "Low Temperature",
    "lowc": "Low Temperature",
    "srad_mj": "Solar Radiation",
    "precip": "Precipitation",
    "precipmm": "Precipitation",
}
UVARDF = {
    "uniqueid": "",
    "day": "",
    "doy": "",
    "sknt": "knots",
    "drct": "degrees N",
    "high": "F",
    "highc": "C",
    "low": "F",
    "lowc": "C",
    "srad_mj": "MJ/day",
    "precip": "inch",
    "precipmm": "mm",
}


@iemapp()
def application(environ, start_response):
    """do great things"""
    pgconn = get_dbconnstr("sustainablecorn")
    stations = ensure_list(environ, "stations")
    df = pd.read_sql(
        text(
            """
    SELECT station as uniqueid, valid as day, extract(doy from valid) as doy,
    high, low, precip, sknt,
    srad_mj, drct from weather_data_daily WHERE station = ANY(:sites)
    and valid >= :sts and valid <= :ets
    ORDER by station ASC, valid ASC
    """
        ),
        pgconn,
        params={
            "sites": stations,
            "sts": environ["sts"],
            "ets": environ["ets"],
        },
        index_col=None,
    )
    df["highc"] = temperature(df["high"].values, "F").value("C")
    df["lowc"] = temperature(df["low"].values, "F").value("C")
    df["precipmm"] = distance(df["precip"].values, "IN").value("MM")

    metarows = [{}, {}]
    cols = df.columns
    for i, colname in enumerate(cols):
        if i == 0:
            metarows[0][colname] = "description"
            metarows[1][colname] = "units"
            continue
        metarows[0][colname] = VARDF.get(colname, "")
        metarows[1][colname] = UVARDF.get(colname, "")
    df = pd.concat([pd.DataFrame(metarows), df], ignore_index=True)
    # re-establish the correct column sorting
    df = df.reindex(cols, axis=1)

    writer = pd.ExcelWriter("/tmp/ss.xlsx", engine="xlsxwriter")
    df.to_excel(writer, "Daily Weather", index=False)
    worksheet = writer.sheets["Daily Weather"]
    worksheet.freeze_panes(3, 0)
    writer.close()

    fn = ",".join(stations)
    if len(stations) > 3:
        fn = "cscap"
    headers = [
        ("Content-type", "application/vnd.ms-excel"),
        ("Content-Disposition", f"attachment;Filename=wx_{fn}.xls"),
    ]
    start_response("200 OK", headers)
    payload = open("/tmp/ss.xlsx", "rb").read()
    os.unlink("/tmp/ss.xlsx")
    return [payload]
