"""Plot!"""
# pylint: disable=abstract-class-instantiated
import sys
import datetime

import pandas as pd
import numpy as np
from paste.request import parse_formvars
from pyiem.util import get_sqlalchemy_conn

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
BYCOL = {
    "daily": "day",
    "monthly": "month",
    "yearly": "year",
    "weekly": "week",
}


def make_plot(form, start_response):
    """Make the plot"""
    siteid = form.get("site")

    sts = datetime.datetime.strptime(
        form.get("date", "2014-01-01"), "%Y-%m-%d"
    )
    days = int(form.get("days", 1))
    ungroup = int(form.get("ungroup", 0))
    ets = sts + datetime.timedelta(days=days)
    by = form.get("by", "daily")
    if ungroup == 0:
        with get_sqlalchemy_conn("td") as conn:
            df = pd.read_sql(
                """WITH data as (
                SELECT date_trunc(%s, date)::date as v,
                coalesce(plotid, location) as agg,
                dwm_treatment as datum,
                sum(nitrate_n_load) as nitrate_n_load
                from tile_flow_and_n_loads_data WHERE
                siteid = %s and date between %s and %s
                and nitrate_n_load is not null and
                dwm_treatment is not null GROUP by v, agg, datum
                )
                SELECT v, datum, avg(nitrate_n_load) as nitrate_n_load
                from data GROUP by v, datum
                ORDER by v ASC""",
                conn,
                params=(BYCOL[by], siteid, sts.date(), ets.date()),
            )

    else:
        with get_sqlalchemy_conn("td") as conn:
            df = pd.read_sql(
                """SELECT date_trunc(%s, date)::date as v,
                coalesce(plotid, location) as datum,
                sum(nitrate_n_load) as nitrate_n_load
                from tile_flow_and_n_loads_data WHERE
                siteid = %s and date between %s and %s
                and nitrate_n_load is not null GROUP by v, datum
                ORDER by v ASC""",
                conn,
                params=(BYCOL[by], siteid, sts.date(), ets.date()),
            )
    if len(df.index) < 3:
        return send_error(
            start_response, 1, "No / Not Enough Data Found, sorry!"
        )
    linecol = "datum"

    # Begin highcharts output
    start_response("200 OK", [("Content-type", "application/javascript")])
    title = f"Nitrate Load for {siteid} ({sts:%-d %b %Y} to {ets:%-d %b %Y})"
    s = []
    plot_ids = df[linecol].unique()
    plot_ids.sort()
    if ungroup == 1:
        plot_ids = plot_ids[::-1]
    df["ticks"] = pd.to_datetime(df["v"]).astype(np.int64) // 10**6
    seriestype = "line" if by == "daily" else "column"
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
    """
        + COPYWRITE
        + """
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
