#!/usr/bin/env python
"""Plot!"""
# pylint: disable=abstract-class-instantiated
import sys
from io import BytesIO
import cgi
import os

import pandas as pd
from pandas.io.sql import read_sql
from pyiem.plot.use_agg import plt
from pyiem.util import get_dbconn, ssw

from common import CODES, getColor

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


def send_error(viewopt, msg):
    """" """
    if viewopt == "js":
        ssw("Content-type: application/javascript\n\n")
        ssw("alert('No data found, sorry');")
        sys.exit()
    fig, ax = plt.subplots(1, 1)
    ax.text(0.5, 0.5, msg, transform=ax.transAxes, ha="center")
    ssw("Content-type: image/png\n\n")
    ram = BytesIO()
    fig.savefig(ram, format="png")
    ram.seek(0)
    ssw(ram.read())
    sys.exit()


def get_vardesc(varname):
    """Get the heading for the variable."""
    pgconn = get_dbconn("td")
    cursor = pgconn.cursor()
    cursor.execute(
        """
        SELECT short_description, units from td_data_dictionary WHERE
        code_column_heading = %s
    """,
        (varname,),
    )
    if cursor.rowcount == 0:
        return varname, varname
    return cursor.fetchone()


def make_plot(form):
    """Make the plot"""
    pgconn = get_dbconn("td")
    uniqueid = form.getfirst("site", "ISUAG")
    varname = form.getfirst("varname", "AGR17")
    (varlabel, varunits) = get_vardesc(varname)

    group = int(form.getfirst("group", 0))
    viewopt = form.getfirst("view", "plot")
    df = read_sql(
        """SELECT value, year, plotid from agronomic_data
        WHERE uniqueid = %s and varname = %s and value is not null
        and value not in ('did not collect')
        ORDER by plotid, year ASC
        """,
        pgconn,
        params=(uniqueid, varname),
        index_col=None,
    )
    if df.empty:
        send_error(viewopt, "No / Not Enough Data Found, sorry!")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    linecol = "plotid"
    if group == 1:
        # Generate the plotid lookup table
        plotdf = read_sql(
            """
            SELECT * from plotids where siteid = %s
        """,
            pgconn,
            params=(uniqueid,),
            index_col="plotid",
        )

        def lookup(row):
            """A helper."""
            try:
                return plotdf.loc[row["plotid"], "y%s" % (row["year"],)]
            except KeyError:
                return row["plotid"]

        df["treatment"] = df.apply(lookup, axis=1)
        del df["plotid"]
        df = df.groupby(["treatment", "year"]).mean()
        df.reset_index(inplace=True)
        linecol = "treatment"

    if viewopt not in ["plot", "js"]:
        df.rename(columns=dict(value=varname), inplace=True)
        if viewopt == "html":
            ssw("Content-type: text/html\n\n")
            ssw(df.to_html(index=False))
            return
        if viewopt == "csv":
            ssw("Content-type: application/octet-stream\n")
            ssw(
                ("Content-Disposition: attachment; " "filename=%s_%s.csv\n\n")
                % (uniqueid, varname)
            )
            ssw(df.to_csv(index=False))
            return
        if viewopt == "excel":
            ssw("Content-type: application/octet-stream\n")
            ssw(
                ("Content-Disposition: attachment; " "filename=%s_%s.xlsx\n\n")
                % (uniqueid, varname)
            )
            with pd.ExcelWriter("/tmp/ss.xlsx") as writer:
                df.to_excel(writer, "Data", index=False)
            ssw(open("/tmp/ss.xlsx", "rb").read())
            os.unlink("/tmp/ss.xlsx")
            return

    # Begin highcharts output
    ssw("Content-type: application/javascript\n\n")
    title = "Agronomic Data for Site: %s" % (uniqueid,)
    arr = []
    plot_ids = df[linecol].unique()
    plot_ids.sort()
    if group == 1:
        plot_ids = plot_ids[::-1]
    for i, plotid in enumerate(plot_ids):
        df2 = df[df[linecol] == plotid]
        arr.append(
            (
                """{type: 'column',
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
                            df2["year"].values, df2["value"].values
                        )
                    ]
                )
                + """
        }"""
            )
            .replace("None", "null")
            .replace("nan", "null")
        )
    series = ",".join(arr)
    ssw(
        """
$("#hc").highcharts({
    title: {text: '"""
        + title
        + """'},
    subtitle: {text: '"""
        + varlabel
        + """ ("""
        + varunits
        + """)'},
    chart: {zoomType: 'x'},
    xAxis: {tickInterval: 1},
    yAxis: [
        {title: {text: '"""
        + varlabel
        + """ ("""
        + varunits
        + """)'}}
    ],
    plotOptions: {line: {turboThreshold: 0}},
    tooltip: {
        shared: true,
        valueDecimals: 0
    },
    series: ["""
        + series
        + """]
});
    """
    )


def main():
    """Do Something"""
    form = cgi.FieldStorage()
    make_plot(form)


if __name__ == "__main__":
    main()
