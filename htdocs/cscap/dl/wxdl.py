#!/usr/bin/env python
"""Download weather data, please"""
import os
import cgi
import datetime

import pandas as pd
from pandas.io.sql import read_sql
from pyiem.util import get_dbconn, ssw
from pyiem.datatypes import distance, temperature

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


def sane_date(year, month, day):
    """Attempt to account for usage of days outside of the bounds for
    a given month"""
    # Calculate the last date of the given month
    nextmonth = datetime.date(year, month, 1) + datetime.timedelta(days=35)
    lastday = nextmonth.replace(day=1) - datetime.timedelta(days=1)
    return datetime.date(year, month, min(day, lastday.day))


def get_cgi_dates(form):
    """Figure out which dates are requested via the form, we shall attempt
    to account for invalid dates provided!"""
    y1 = int(form.getfirst("year1"))
    m1 = int(form.getfirst("month1"))
    d1 = int(form.getfirst("day1"))
    y2 = int(form.getfirst("year2"))
    m2 = int(form.getfirst("month2"))
    d2 = int(form.getfirst("day2"))

    ets = sane_date(y2, m2, d2)
    archive_end = datetime.date.today() - datetime.timedelta(days=1)
    if ets > archive_end:
        ets = archive_end

    return [sane_date(y1, m1, d1), ets]


def do_work(form):
    """do great things"""
    pgconn = get_dbconn("sustainablecorn")
    stations = form.getlist("stations")
    if not stations:
        stations.append("XXX")
    sts, ets = get_cgi_dates(form)
    df = read_sql(
        """
    SELECT station as uniqueid, valid as day, extract(doy from valid) as doy,
    high, low, precip, sknt,
    srad_mj, drct from weather_data_daily WHERE station in %s
    and valid >= %s and valid <= %s
    ORDER by station ASC, valid ASC
    """,
        pgconn,
        params=(tuple(stations), sts, ets),
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
    ssw("Content-type: application/vnd.ms-excel\n")
    ssw(f"Content-Disposition: attachment;Filename=wx_{fn}.xls\n\n")
    ssw(open("/tmp/ss.xlsx", "rb").read())
    os.unlink("/tmp/ss.xlsx")


def main():
    """Do Stuff"""
    form = cgi.FieldStorage()
    do_work(form)


if __name__ == "__main__":
    main()
