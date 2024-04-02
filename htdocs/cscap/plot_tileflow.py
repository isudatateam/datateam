"""Plot!"""

import datetime
import os
from io import StringIO

import numpy as np
import pandas as pd
from pandas.io.sql import read_sql
from pyiem.util import get_dbconn
from pyiem.webutil import iemapp

ERRMSG = (
    "No data found. Check the start date falls within the "
    "applicable date range for the research site. "
    "If yes, try expanding the number of days included."
)
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


def get_vardf(pgconn, tabname):
    """Get a dataframe of descriptors for this tabname"""
    return read_sql(
        """
        select element_or_value_display_name as varname,
        number_of_decimal_places_to_round_up::numeric::int as round,
        short_description, units from data_dictionary_export WHERE
        spreadsheet_tab = %s
    """,
        pgconn,
        params=(tabname,),
        index_col="varname",
    )


def add_bling(pgconn, df, tabname):
    """Do fancy things"""
    # Insert some headers rows
    metarows = [{}, {}]
    cols = df.columns
    vardf = get_vardf(pgconn, tabname)
    for i, colname in enumerate(cols):
        if i == 0:
            metarows[0][colname] = "description"
            metarows[1][colname] = "units"
            continue
        if colname in vardf.index:
            metarows[0][colname] = vardf.at[colname, "short_description"]
            metarows[1][colname] = vardf.at[colname, "units"]
    df = pd.concat([pd.DataFrame(metarows), df], ignore_index=True)
    # re-establish the correct column sorting
    df = df.reindex(cols, axis=1)
    return df


@iemapp()
def application(environ, start_response):
    """Make the plot"""
    uniqueid = environ.get("site", "ISUAG").split("::")[0]

    sts = datetime.datetime.strptime(
        environ.get("date", "2014-01-01"), "%Y-%m-%d"
    )
    days = int(environ.get("days", 1))
    ets = sts + datetime.timedelta(days=days)
    pgconn = get_dbconn("sustainablecorn")
    tzname = (
        "America/Chicago"
        if uniqueid in ["ISUAG", "SERF", "GILMORE"]
        else "America/New_York"
    )
    viewopt = environ.get("view", "plot")
    ptype = environ.get("ptype", "1")
    if ptype == "1":
        df = read_sql(
            """SELECT uniqueid, plotid, valid at time zone 'UTC' as v,
        discharge_mm_qc as discharge
        from tileflow_data WHERE uniqueid = %s
        and valid between %s and %s ORDER by valid ASC
        """,
            pgconn,
            params=(uniqueid, sts.date(), ets.date()),
        )
    elif ptype == "2":
        df = read_sql(
            """SELECT uniqueid, plotid,
        date_trunc('month', valid at time zone 'UTC') as v,
        sum(discharge_mm_qc) as discharge
        from tileflow_data WHERE uniqueid = %s
        and valid between %s and %s GROUP by uniqueid, v, plotid ORDER by v ASC
        """,
            pgconn,
            params=(uniqueid, sts.date(), ets.date()),
        )
    if ptype not in [
        "2",
    ]:
        df["v"] = df["v"].apply(
            lambda x: x.tz_localize("UTC").tz_convert(tzname)
        )

    if viewopt not in ["plot", "js"]:
        df.rename(
            columns=dict(v="timestamp", discharge="Tile Flow (mm)"),
            inplace=True,
        )
        df = add_bling(pgconn, df, "Water")
        if viewopt == "html":
            start_response("200 OK", [("Content-type", "text/html")])
            return [df.to_html(index=False).encode("ascii")]
        if viewopt == "csv":
            headers = [
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
            ]
            start_response("200 OK", headers)
            return [df.to_csv(index=False).encode("ascii")]
        if viewopt == "excel":
            headers = [
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
            ]
            start_response("200 OK", headers)
            # Prevent timezone troubles
            if ptype not in [
                "2",
            ]:
                df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M")
            with pd.ExcelWriter("/tmp/ss.xlsx") as writer:
                df.to_excel(writer, "Data", index=False)
                worksheet = writer.sheets["Data"]
                worksheet.freeze_panes(3, 0)
            payload = open("/tmp/ss.xlsx", "rb").read()
            os.unlink("/tmp/ss.xlsx")
            return [payload]

    # Begin highcharts output
    start_response("200 OK", [("Content-type", "application/javascript")])
    sio = StringIO()
    title = ("Tile Flow for Site: %s (%s to %s)") % (
        uniqueid,
        sts.strftime("%-d %b %Y"),
        ets.strftime("%-d %b %Y"),
    )
    s = []
    plot_ids = df["plotid"].unique()
    plot_ids.sort()
    df["ticks"] = df["v"].astype(np.int64) // 10**6
    seriestype = "line" if ptype == "1" else "column"
    for plotid in plot_ids:
        df2 = df[df["plotid"] == plotid]
        s.append(
            (
                """{type: '"""
                + seriestype
                + """',
            name: '"""
                + plotid
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
    series = ",".join(s)
    sio.write(
        """
$("#hc").highcharts({
    title: {text: '"""
        + title
        + """'},
    chart: {zoomType: 'x'},
    yAxis: {title: {text: 'Tile Flow (mm)'}
    },
    plotOptions: {
        line: {turboThreshold: 0}
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
        valueDecimals: 2,
        valueSuffix: ' mm'
    },
    series: ["""
        + series
        + """]
});
    """
    )
    return [sio.getvalue().encode("utf-8")]
