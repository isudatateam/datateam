"""Plot!"""
# pylint: disable=abstract-class-instantiated
import sys
import datetime

import pandas as pd
from pandas.io.sql import read_sql
import numpy as np
from paste.request import parse_formvars
from pyiem.util import get_dbconn

sys.path.append("/opt/datateam/htdocs/td")
from common import CODES, getColor, send_error, COPYWRITE

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
BYCOL = {"daily": "day", "monthly": "month", "yearly": "year"}


def make_plot(form, start_response):
    """Make the plot"""
    uniqueid = form.get("site", "ISUAG")

    sts = datetime.datetime.strptime(
        form.get("date", "2014-01-01"), "%Y-%m-%d"
    )
    days = int(form.get("days", 1))
    ets = sts + datetime.timedelta(days=days)
    pgconn = get_dbconn("td")
    by = form.get("by", "daily")
    ungroup = int(form.get("ungroup", 0))
    df = read_sql(
        f"SELECT date_trunc('{BYCOL[by]}', date)::date as v, "
        "coalesce(plotid, location) as datum, "
        "avg(water_table_depth) as depth from water_table_data "
        "WHERE siteid = %s and date between %s and %s "
        "GROUP by v, datum ORDER by v ASC",
        pgconn,
        params=(uniqueid, sts.date(), ets.date()),
    )
    if len(df.index) < 3:
        return send_error(
            start_response, "by", "No / Not Enough Data Found, sorry!"
        )
    linecol = "datum"
    if ungroup == 0:
        # Generate the plotid lookup table
        plotdf = read_sql(
            "SELECT * from wellids where siteid = %s",
            pgconn,
            params=(uniqueid,),
            index_col="wellid",
        )

        def lookup(row):
            """Likely better means in pandas for this, but alas"""
            try:
                return plotdf.loc[row["datum"], "y%s" % (row["v"].year,)]
            except KeyError:
                return row["datum"]

        df["treatment"] = df.apply(lookup, axis=1)
        del df["datum"]
        df = df.groupby(["treatment", "v"]).mean()
        df.reset_index(inplace=True)
        linecol = "treatment"

    # Begin highcharts output
    start_response("200 OK", [("Content-type", "application/javascript")])
    title = "Water Table Depth for Site: %s (%s to %s)" % (
        uniqueid,
        sts.strftime("%-d %b %Y"),
        ets.strftime("%-d %b %Y"),
    )
    s = []
    plot_ids = df[linecol].unique()
    plot_ids.sort()
    if ungroup == 0:
        plot_ids = plot_ids[::-1]
    df["ticks"] = pd.to_datetime(df["v"]).astype(np.int64) // 10**6
    for i, plotid in enumerate(plot_ids):
        df2 = df[df[linecol] == plotid]
        v = df2[["ticks", "depth"]].to_json(orient="values")
        s.append(
            """{
            """
            + getColor(plotid, i)
            + """,
            name: '"""
            + CODES.get(plotid, plotid)
            + """',
            data: """
            + v
            + """
        }"""
        )
    series = ",".join(s)
    res = (
        """
$("#hc").highcharts({
    """
        + COPYWRITE
        + """
    title: {text: '"""
        + title
        + """'},
    chart: {zoomType: 'x'},
    yAxis: {title: {text: 'Depth below ground (mm)'},
        reversed: true
    },
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
