#!/usr/bin/env python
"""Dynamic Calculation, yikes"""
import cgi
import re
import os
import datetime

import pandas as pd
from pandas.io.sql import read_sql
from pyiem.util import get_dbconn, ssw

VARRE = re.compile(r"(AGR[0-9]{1,2})")


def get_df(equation):
    """Attempt to compute what was asked for"""
    pgconn = get_dbconn('sustainablecorn')
    varnames = VARRE.findall(equation)
    df = read_sql("""
    SELECT * from agronomic_data WHERE varname in %s
    """, pgconn, params=(tuple(varnames), ), index_col=None)
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df = pd.pivot_table(df, index=('uniqueid', 'plotid', 'year'),
                        values='value', columns=('varname',),
                        aggfunc=lambda x: ' '.join(str(v) for v in x))
    df.eval("calc = %s" % (equation, ), inplace=True)
    df.sort_values(by='calc', inplace=True)
    df.reset_index(inplace=True)
    df = df[pd.notnull(df['calc'])]
    return df


def main():
    """Go Main"""

    form = cgi.FieldStorage()
    equation = form.getfirst('equation', 'AGR33 / AGR4').upper()
    fmt = form.getfirst('fmt', 'html')
    df = get_df(equation)

    if fmt == 'excel':
        ssw('Content-type: application/octet-stream\n')
        ssw(('Content-Disposition: attachment; filename=cscap_%s.xlsx\n\n'
             ) % (datetime.datetime.now().strftime("%Y%m%d%H%M"), ))
        writer = pd.ExcelWriter('/tmp/ss.xlsx')
        df.to_excel(writer, 'Data', index=False)
        writer.save()
        ssw(open('/tmp/ss.xlsx', 'rb').read())
        os.unlink('/tmp/ss.xlsx')
        return
    if fmt == 'csv':
        ssw('Content-type: application/octet-stream\n')
        ssw(('Content-Disposition: attachment; filename=cscap_%s.csv\n\n'
             ) % (datetime.datetime.now().strftime("%Y%m%d%H%M"), ))
        ssw(df.to_csv(index=False))
        return

    ssw('Content-type: text/html\n\n')
    ssw("""<!DOCTYPE html>
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
    """ % (df.to_html(index=False).replace("NaN", "M"), ))


if __name__ == '__main__':
    main()
