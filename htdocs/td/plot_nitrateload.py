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


def make_plot(form, start_response):
    """Make the plot"""
    pgconn = get_dbconn("td")
    siteid = form.get("site")

    sts = datetime.datetime.strptime(
        form.get("date", "2014-01-01"), "%Y-%m-%d"
    )
    days = int(form.get("days", 1))
    group = int(form.get("group", 0))
    ets = sts + datetime.timedelta(days=days)
    viewopt = form.get("view", "plot")
    ptype = form.get("ptype", "1")
    missing = form.get("missing", "M")
    custom_missing = form.get("custom_missing", "M")
    missing = missing if missing != "__custom__" else custom_missing
    if ptype == "1":
        df = read_sql(
            "SELECT date as v, coalesce(plotid, location) as plotid, "
            "nitrate_n_load, nitrate_n_removed "
            "nitrate_n_load_filled from tile_flow_and_n_loads_data WHERE "
            "siteid = %s and date between %s and %s ORDER by date ASC",
            pgconn,
            params=(siteid, sts.date(), ets.date()),
        )
    elif ptype == "2":
        df = read_sql(
            "SELECT date_trunc(month from date) as v, "
            "coalesce(plotid, location) as plotid, "
            "nitrate_n_load, nitrate_n_removed "
            "nitrate_n_load_filled from tile_flow_and_n_loads_data WHERE "
            "siteid = %s and date between %s and %s ORDER by date ASC",
            pgconn,
            params=(siteid, sts.date(), ets.date()),
        )
    if len(df.index) < 3:
        send_error(
            start_response, viewopt, "No / Not Enough Data Found, sorry!"
        )
    linecol = "plotid"
    if group == 1:
        # Generate the plotid lookup table
        plotdf = read_sql(
            "SELECT coalesce(plotid, location) as plotid "
            "from meta_plot_identifier where siteid = %s",
            pgconn,
            params=(siteid,),
            index_col="plotid",
        )

        def lookup(row):
            try:
                return plotdf.loc[row["plotid"], "y%s" % (row["v"].year,)]
            except KeyError:
                return row["plotid"]

        df["treatment"] = df.apply(lookup, axis=1)
        del df["plotid"]
        df = df.groupby(["treatment", "v"]).mean()
        df.reset_index(inplace=True)
        linecol = "treatment"

    if viewopt not in ["plot", "js"]:
        df = df.fillna(missing)
        df["v"] = df["v"].dt.strftime("%Y-%m-%d %H:%M")
        df.rename(
            columns=dict(v="timestamp", load="Load (kg ha-1)"), inplace=True
        )
        if viewopt == "html":
            start_response("200 OK", [("Content-type", "text/html")])
            return df.to_html(index=False).encode("utf-8")

    # Begin highcharts output
    start_response("200 OK", [("Content-type", "application/javascript")])
    title = ("Nitrate Load for Site: %s (%s to %s)") % (
        siteid,
        sts.strftime("%-d %b %Y"),
        ets.strftime("%-d %b %Y"),
    )
    s = []
    plot_ids = df[linecol].unique()
    plot_ids.sort()
    if group == "1":
        plot_ids = plot_ids[::-1]
    df["ticks"] = pd.to_datetime(df["v"]).astype(np.int64) // 10 ** 6
    seriestype = "line" if ptype == "1" else "column"
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
                            df2["ticks"].values, df2["nitrate_n_load"].values
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
    yAxis: {title: {text: 'Load (kg ha-1)'}
    },
    plotOptions: {line: {turboThreshold: 0}},
    xAxis: {
        type: 'datetime'
    },
    tooltip: {
        dateTimeLabelFormats: {
            hour: "%b %e %Y, %H:%M",
            minute: "%b %e %Y, %H:%M"
        },
        shared: true,
        valueDecimals: 4,
        valueSuffix: ' kg ha-1'
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
