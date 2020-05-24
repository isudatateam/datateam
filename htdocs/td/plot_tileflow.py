"""Plot!"""
# pylint: disable=abstract-class-instantiated
import sys
import datetime
import os

import pandas as pd
from pandas.io.sql import read_sql
import numpy as np
from paste.request import parse_formvars
from pyiem.util import get_dbconn

sys.path.append("/opt/datateam/htdocs/td")
from common import CODES, getColor, send_error

LINESTYLE = [
    "-",
    "-",
    "-",
    "-",
    "-",
    "-",
    "-",
    "-",
    "-.",
    "-.",
    "-.",
    "-.",
    "-.",
    "-",
    "-.",
    "-.",
    "-.",
    "-.",
    "-.",
    "-.",
    "-.",
    "-.",
    "-.",
    "-.",
]


def get_weather(pgconn, uniqueid, sts, ets):
    """Retreive the daily precipitation"""
    # Convert ids
    dbid = uniqueid
    if uniqueid not in ["SERF_IA", "SERF_SD", "DEFI_R"]:
        dbid = uniqueid.split("_")[0]
    df = read_sql(
        "SELECT valid, precip_mm from weather_daily "
        "WHERE siteid = %s and valid >= %s and valid <= %s ORDER by valid ASC",
        pgconn,
        index_col="valid",
        params=(dbid, sts.date(), ets.date()),
    )
    df.index = pd.DatetimeIndex(df.index.values)
    df["ticks"] = (
        df.index.values.astype("datetime64[ns]").astype(np.int64) // 10 ** 6
    )
    return df


def make_plot(form, start_response):
    """Make the plot"""
    pgconn = get_dbconn("td")
    uniqueid = form.get("site", "ISUAG")

    sts = datetime.datetime.strptime(
        form.get("date", "2014-01-01"), "%Y-%m-%d"
    )
    days = int(form.get("days", 1))
    group = int(form.get("group", 0))
    ets = sts + datetime.timedelta(days=days)
    wxdf = get_weather(pgconn, uniqueid, sts, ets)
    tzname = (
        "America/Chicago"
        if uniqueid in ["ISUAG", "SERF", "GILMORE"]
        else "America/New_York"
    )
    viewopt = form.get("view", "plot")
    ptype = form.get("ptype", "1")
    missing = form.get("missing", "M")
    custom_missing = form.get("custom_missing", "M")
    missing = missing if missing != "__custom__" else custom_missing
    if ptype == "1":
        df = read_sql(
            """SELECT valid at time zone 'UTC' as v, plotid,
        discharge_mm_qc as discharge,
        coalesce(discharge_mm_qcflag, '') as discharge_f
        from tileflow_data WHERE uniqueid = %s
        and valid between %s and %s ORDER by valid ASC
        """,
            pgconn,
            params=(uniqueid, sts.date(), ets.date()),
        )
    elif ptype == "2":
        # resample the weather data
        if not wxdf.empty:
            wxdf = wxdf.resample(
                "M", loffset=datetime.timedelta(days=-27)
            ).sum()
            wxdf["ticks"] = (
                wxdf.index.values.astype("datetime64[ns]").astype(np.int64)
                // 10 ** 6
            )
        df = read_sql(
            """SELECT
        date_trunc('month', valid at time zone 'UTC') as v, plotid,
        sum(discharge_mm_qc) as discharge
        from tileflow_data WHERE uniqueid = %s
        and valid between %s and %s GROUP by v, plotid ORDER by v ASC
        """,
            pgconn,
            params=(uniqueid, sts.date(), ets.date()),
        )
        df["discharge_f"] = "-"
    elif ptype == "3":
        # Daily Aggregate
        df = read_sql(
            """SELECT
        date_trunc('day', valid at time zone 'UTC') as v, plotid,
        sum(discharge_mm_qc) as discharge
        from tileflow_data WHERE uniqueid = %s
        and valid between %s and %s GROUP by v, plotid ORDER by v ASC
        """,
            pgconn,
            params=(uniqueid, sts.date(), ets.date()),
        )
        df["discharge_f"] = "-"
    if len(df.index) < 3:
        send_error(
            start_response, viewopt, "No / Not Enough Data Found, sorry!"
        )
    linecol = "plotid"
    if group == 1:
        # Generate the plotid lookup table
        plotdf = read_sql(
            "SELECT * from plotids where siteid = %s",
            pgconn,
            params=(uniqueid,),
            index_col="plotid",
        )

        def lookup(row):
            """Lookup value."""
            try:
                return plotdf.loc[row["plotid"], "y%s" % (row["v"].year,)]
            except KeyError:
                return row["plotid"]

        df["treatment"] = df.apply(lookup, axis=1)
        del df["plotid"]
        df = df.groupby(["treatment", "v"]).mean()
        df.reset_index(inplace=True)
        linecol = "treatment"
    if ptype not in ["2", "3"]:
        df["v"] = df["v"].apply(
            lambda x: x.tz_localize("UTC").tz_convert(tzname)
        )

    if viewopt not in ["plot", "js"]:
        df = df.fillna(missing)
        df["v"] = df["v"].dt.strftime("%Y-%m-%d %H:%M")
        df.rename(
            columns=dict(v="timestamp", discharge="Discharge (mm)"),
            inplace=True,
        )
        if viewopt == "html":
            start_response("200 OK", [("Content-type", "text/html")])
            return df.to_html(index=False).encode("utf-8")
        if viewopt == "csv":
            start_response(
                "200 OK",
                [
                    ("Content-type", "application/octet-stream"),
                    (
                        "Content-Disposition",
                        "attachment; filename=%s_%s_%s.csv"
                        % (
                            uniqueid,
                            sts.strftime("%Y%m%d"),
                            ets.strftime("%Y%m%d"),
                        ),
                    ),
                ],
            )
            return df.to_csv(index=False).encode("utf-8")
        if viewopt == "excel":
            start_response(
                "200 OK",
                [
                    ("Content-type", "application/octet-stream"),
                    (
                        "Content-Disposition",
                        "attachment; filename=%s_%s_%s.xlsx"
                        % (
                            uniqueid,
                            sts.strftime("%Y%m%d"),
                            ets.strftime("%Y%m%d"),
                        ),
                    ),
                ],
            )
            with pd.ExcelWriter("/tmp/ss.xlsx") as writer:
                df.to_excel(writer, "Data", index=False)
            res = open("/tmp/ss.xlsx", "rb").read()
            os.unlink("/tmp/ss.xlsx")
            return res

    # Begin highcharts output
    start_response("200 OK", [("Content-type", "application/javascript")])
    title = "Tile Flow for Site: %s (%s to %s)" % (
        uniqueid,
        sts.strftime("%-d %b %Y"),
        ets.strftime("%-d %b %Y"),
    )
    s = []
    plot_ids = df[linecol].unique()
    plot_ids.sort()
    if group == "1":
        plot_ids = plot_ids[::-1]
    df["ticks"] = df["v"].astype(np.int64) // 10 ** 6
    seriestype = "line" if ptype in ["1", "3"] else "column"
    for i, plotid in enumerate(plot_ids):
        df2 = df[df[linecol] == plotid]
        s.append(
            (
                """{type: '"""
                + seriestype
                + """',
            """
                + getColor(plotid, i)
                + """,
            name: '"""
                + CODES.get(plotid, plotid)
                + """',
            data: """
                + str(
                    [
                        [a, b]
                        for a, b in zip(
                            df2["ticks"].values, df2["discharge"].values
                        )
                    ]
                )
                + """
        }"""
            )
            .replace("None", "null")
            .replace("nan", "null")
        )
    if not wxdf.empty:
        s.append(
            (
                """{type: 'column',
            name: 'Precip',
            color: '#0000ff',
            yAxis: 1,
            data: """
                + str(
                    [
                        [a, b]
                        for a, b in zip(
                            wxdf["ticks"].values, wxdf["precip_mm"].values
                        )
                    ]
                )
                + """
        }"""
            )
            .replace("None", "null")
            .replace("nan", "null")
        )
    series = ",".join(s)
    res = (
        """
$("#hc").highcharts({
    title: {text: '"""
        + title
        + """'},
    chart: {zoomType: 'x'},
    yAxis: [
        {title: {text: 'Discharge (mm)'}},
        {title: {text: 'Daily Precipitation (mm)'},
         reversed: true,
         maxPadding: 1,
         opposite: true},
    ],
    plotOptions: {
        line: {turboThreshold: 0},
        series: {
            cursor: 'pointer',
            allowPointSelect: true,
            point: {
                events: {
                    click: function() {
                        editPoint(this);
                    }
                }
            }
        }
    },
    xAxis: {
        type: 'datetime'
    },
    tooltip: {
        dateTimeLabelFormats: {
            hour: "%b %e %Y, %H:%M",
            minute: "%b %e %Y, %H:%M"
        },
        shared: true,
        valueDecimals: 0,
        valueSuffix: ' mm'
    },
    series: ["""
        + series
        + """]
});
    """
    )
    return res.encode("utf-8")


def application(environ, start_response):
    """Do Something"""
    form = parse_formvars(environ)
    return [make_plot(form, start_response)]
