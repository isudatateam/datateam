"""Dynamic Calculation, yikes"""

import datetime
import os
import re

import pandas as pd
from pyiem.database import get_sqlalchemy_conn
from pyiem.util import LOG
from pyiem.webutil import iemapp
from sqlalchemy import text

VARRE = re.compile(r"(AGR[0-9]{1,2})")


def get_df(equation):
    """Attempt to compute what was asked for"""

    varnames = VARRE.findall(equation)
    with get_sqlalchemy_conn("sustainablecorn") as pgconn:
        df = pd.read_sql(
            text(
                """
        SELECT * from agronomic_data WHERE varname = ANY(:vars)
        """
            ),
            pgconn,
            params={"vars": varnames},
            index_col=None,
        )
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = pd.pivot_table(
        df,
        index=("uniqueid", "plotid", "year"),
        values="value",
        columns=("varname",),
        aggfunc=lambda x: " ".join(str(v) for v in x),
    )
    try:
        df.eval("calc = %s" % (equation,), inplace=True)
        df.sort_values(by="calc", inplace=True)
        df.reset_index(inplace=True)
        df = df[pd.notnull(df["calc"])]
    except Exception as exp:
        LOG.exception(exp, exc_info=True)
    return df


@iemapp()
def application(environ, start_response):
    """Go Main"""
    equation = environ.get("equation", "AGR33 / AGR4").upper()
    fmt = environ.get("fmt", "html")
    df = get_df(equation)

    if fmt == "excel":
        headers = [
            ("Content-type", "application/octet-stream"),
            (
                "Content-Disposition",
                "attachment; "
                f"filename=cscap_{datetime.datetime.now():%Y%m%d%H%M}.xlsx",
            ),
        ]
        start_response("200 OK", headers)
        with pd.ExcelWriter("/tmp/ss.xlsx") as writer:
            df.to_excel(writer, sheet_name="Data", index=False)
        payload = open("/tmp/ss.xlsx", "rb").read()
        os.unlink("/tmp/ss.xlsx")
        return [payload]
    if fmt == "csv":
        headers = [
            ("Content-type", "application/octet-stream"),
            (
                "Content-Disposition",
                "attachment; "
                f"filename=cscap_{datetime.datetime.now():%Y%m%d%H%M}.csv",
            ),
        ]
        payload = df.to_csv(index=False).encode("ascii")
        return [payload]

    headers = [("Content-type", "text/html")]
    start_response("200 OK", headers)
    payload = """<!DOCTYPE html>
<html lang='en'>
<head>
 <link href="/vendor/bootstrap/3.3.5/css/bootstrap.min.css" rel="stylesheet">
 <link href="/css/bootstrap-override.css" rel="stylesheet">
<style>
table {border-collapse: collapse;}
td    {padding: 6px;}
body {padding: 30px;}
</style>
</head>
<body>
<form method="GET">
<table>
<thead><tr><th>Enter Equation</th><th>Output Format</th></tr></thead>
<tbody><tr><td>
<input name="equation" type="text" size="80" value="AGR33 / (AGR4 + AGR33)">
</td>
<td><select name="fmt">
 <option value="html">HTML Table</option>
 <option value="excel">Excel</option>
 <option value="csv">Comma Delimited</option>
</select></td></tr></tbody></table>
<input type="submit">
</form>
<br /><br />
%s

</body>
</html>
    """ % (df.to_html(index=False).replace("NaN", "M"),)
    return [payload.encode("ascii")]
