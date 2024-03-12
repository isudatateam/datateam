"""Plot!"""

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
VARDICT = {
    "WAT2": {"title": "Nitrate-N Concentration", "units": "mg N / L"},
    "WAT9": {
        "title": "Soluble Reactive Phosphorus Concentration",
        "units": "ug P / L",
    },
}


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

    pgconn = get_dbconn("sustainablecorn")
    viewopt = environ.get("view", "plot")
    varname = environ.get("varname", "WAT2")
    df = read_sql(
        """
    SELECT uniqueid, plotid, valid at time zone 'UTC' as v, value
    from waterquality_data WHERE uniqueid = %s
    and varname = %s ORDER by valid ASC
    """,
        pgconn,
        params=(uniqueid, varname),
    )

    if viewopt not in ["plot", "js"]:
        newcolname = "%s, %s" % (
            VARDICT[varname]["title"],
            VARDICT[varname]["units"],
        )
        df.rename(columns=dict(v="timestamp", value=newcolname), inplace=True)
        df = add_bling(pgconn, df, "Water")
        if viewopt == "html":
            start_response("200 OK", [("Content-type", "text/html")])
            return [df.to_html(index=False).encode("ascii", "ignore")]
        if viewopt == "csv":
            headers = [
                ("Content-type", "application/octet-stream"),
                (
                    "Content-Disposition",
                    f"attachment; filename={uniqueid}.csv",
                ),
            ]
            start_response("200 OK", headers)
            return [df.to_csv(index=False).encode("ascii", "ignore")]
        if viewopt == "excel":
            headers = [
                ("Content-type", "application/octet-stream"),
                (
                    "Content-Disposition",
                    f"attachment; filename={uniqueid}.xlsx",
                ),
            ]
            start_response("200 OK", headers)
            writer = pd.ExcelWriter("/tmp/ss.xlsx")
            df.to_excel(writer, "Data", index=False)
            worksheet = writer.sheets["Data"]
            worksheet.freeze_panes(3, 0)
            writer.save()
            payload = open("/tmp/ss.xlsx", "rb").read()
            os.unlink("/tmp/ss.xlsx")
            return [payload]

    # Begin highcharts output
    start_response("200 OK", [("Content-type", "application/javascript")])
    sio = StringIO()
    title = ("Water Quality for Site: %s") % (uniqueid,)
    splots = []
    plot_ids = df["plotid"].unique()
    plot_ids.sort()
    df["ticks"] = df["v"].astype(np.int64) // 10**6
    for plotid in plot_ids:
        df2 = df[df["plotid"] == plotid]
        splots.append(
            (
                """{type: 'scatter',
            name: '"""
                + plotid
                + """',
            data: """
                + str(
                    [
                        [a, b]
                        for a, b in zip(
                            df2["ticks"].values, df2["value"].values
                        )
                    ]
                )
                + """
        }"""
            )
            .replace("None", "null")
            .replace("nan", "null")
        )
    series = ",".join(splots)
    sio.write(
        """
$("#hc").highcharts({
    title: {text: '"""
        + title
        + """'},
    chart: {zoomType: 'x'},
    yAxis: {title: {text: '"""
        + VARDICT[varname]["title"]
        + """ """
        + VARDICT[varname]["units"]
        + """'}
    },
    plotOptions: {line: {turboThreshold: 0}
    },
    xAxis: {
        type: 'datetime'
    },
    tooltip: {
        pointFormat: 'date: <b>{point.x:%b %e %Y, %H:%M}</b>' +
        '<br/>value: <b>{point.y}</b><br/>',
        shared: true,
        valueDecimals: 2,
        valueSuffix: '"""
        + VARDICT[varname]["units"]
        + """'
    },
    series: ["""
        + series
        + """]
});
    """
    )
    return [sio.getvalue().encode("ascii")]
