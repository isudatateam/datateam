"""Download weather data, please"""

# pylint: disable=abstract-class-instantiated
import datetime
import os

import pandas as pd
from paste.request import parse_formvars
from pyiem.util import get_dbconnstr
from sqlalchemy import text

VARDF = {
    "siteid": "",
    "date": "",
    "doy": "Day Of Year",
    "precipitation": "Precipitation",
    "relative_humidity": "Relative Humidity",
    "air_temp_avg": "Average Air Temperature",
    "air_temp_min": "Low Air Temperature",
    "air_temp_max": "High Air Temperature",
    "solar_radiation": "Solar Radiation",
    "wind_speed": "Wind Speed",
    "wind_direction": "Wind Direction",
    "et": "Evapotranspiratin",
    "et_method": "Evapotranspiration Method",
}
UVARDF = {
    "siteid": "",
    "date": "",
    "doy": "Day Of Year",
    "precipitation": "mm",
    "relative_humidity": "%",
    "air_temp_avg": "C",
    "air_temp_min": "C",
    "air_temp_max": "C",
    "solar_radiation": "MJ",
    "wind_speed": "m/s",
    "wind_direction": "degrees N",
    "et": "mm",
    "et_method": "",
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
    y1 = int(form.get("year1"))
    m1 = int(form.get("month1"))
    d1 = int(form.get("day1"))
    y2 = int(form.get("year2"))
    m2 = int(form.get("month2"))
    d2 = int(form.get("day2"))

    ets = sane_date(y2, m2, d2)
    archive_end = datetime.date.today() - datetime.timedelta(days=1)
    if ets > archive_end:
        ets = archive_end

    return [sane_date(y1, m1, d1), ets]


def do_work(form):
    """do great things"""
    pgconn = get_dbconnstr("td")
    stations = form.getall("stations")
    if not stations:
        stations.append("XXX")
    sts, ets = get_cgi_dates(form)
    df = pd.read_sql(
        text(
            "SELECT *, extract(doy from date) as doy from weather_data "
            "WHERE siteid = ANY(:sites) and date >= :sts and date <= :ets "
            "ORDER by siteid ASC, date ASC"
        ),
        pgconn,
        params={"sites": stations, "sts": sts, "ets": ets},
        index_col=None,
    )

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

    with pd.ExcelWriter("/tmp/ss.xlsx", engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="Daily Weather", index=False)
        worksheet = writer.sheets["Daily Weather"]
        worksheet.freeze_panes(3, 0)

    fn = ",".join(stations)
    res = open("/tmp/ss.xlsx", "rb").read()
    os.unlink("/tmp/ss.xlsx")
    return res, fn


def application(environ, start_response):
    """Do Stuff"""
    form = parse_formvars(environ)
    res, fn = do_work(form)
    headers = [
        ("Content-type", "application/vnd.ms-excel"),
        ("Content-Disposition", f"attachment;filename=wx_{fn}.xls"),
    ]
    start_response("200 OK", headers)
    return [res]
