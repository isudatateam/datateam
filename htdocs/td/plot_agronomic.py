"""Plot!"""
# pylint: disable=abstract-class-instantiated
import sys

import pandas as pd
from paste.request import parse_formvars
from pandas.io.sql import read_sql
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


def make_plot(form, start_response):
    """Make the plot"""
    pgconn = get_dbconn("td")
    uniqueid = form.get("site", "ISUAG")
    varname = form.get("varname", "AGR17")
    (varlabel, varunits) = get_vardesc(varname)

    group = int(form.get("group", 0))
    df = read_sql(
        "SELECT * from agronomic_data WHERE siteid = %s "
        "ORDER by plotid, year ASC",
        pgconn,
        params=(uniqueid,),
        index_col=None,
    )
    sys.stderr.write(uniqueid)
    if df.empty:
        return send_error(
            start_response, "js", "No / Not Enough Data Found, sorry!"
        )
    df["value"] = pd.to_numeric(df[varname], errors="coerce")
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

    # Begin highcharts output
    start_response("200 OK", [("Content-type", "application/javascript")])
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
    res = (
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
    return res.encode("utf-8")


def application(environ, start_response):
    """Do Something"""
    form = parse_formvars(environ)
    return [make_plot(form, start_response)]
