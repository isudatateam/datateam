#!/usr/bin/env python

from pandas.io.sql import read_sql
from pyiem.network import Table as NetworkTable
from pyiem.util import get_dbconn, ssw

nt = NetworkTable("CSCAP")


def main():
    ssw("Content-type: text/html\n\n")
    pgconn = get_dbconn("coop")
    cids = []
    for sid in nt.sts.keys():
        csite = nt.sts[sid]["climate_site"]
        if csite not in cids:
            cids.append(csite)
    df = read_sql(
        """
        SELECT station, year, avg((high+low)/2.) as avgt, sum(precip) as p
        from alldata where station in %s and year < 2016
        and year > 1950 GROUP by station, year
                  """,
        pgconn,
        params=(tuple(cids),),
        index_col=None,
    )
    df2 = df.copy()
    df.set_index(["station", "year"], inplace=True)

    table = ""
    ids = list(nt.sts.keys())
    ids.sort()
    for sid in ids:
        cid = nt.sts[sid]["climate_site"]
        table += "<tr><th>%s</th>" % (nt.sts[sid]["name"],)
        for yr in range(2011, 2016):
            for col in ["avgt", "p"]:
                table += "<td>%.2f</td>" % (df.at[(cid, yr), col],)
        df3 = df2[(df2["station"] == cid) & (df2["year"] > 2010)].mean()
        for col in ["avgt", "p"]:
            table += "<td>%.2f</td>" % (df3[col],)
        df4 = df2[(df2["station"] == cid)].mean()
        for col in ["avgt", "p"]:
            table += "<td>%.2f</td>" % (df4[col],)

        table += "</tr>\n"

    ssw(
        """<!DOCTYPE html>
<html lang='en'>
<head>
 <link href="/vendor/bootstrap/3.3.5/css/bootstrap.min.css" rel="stylesheet">
 <link href="/css/bootstrap-override.css" rel="stylesheet">
</head>
<body>

<table class="table table-striped table-bordered">
<thead>
 <tr>
  <th rowspan="2">Site</th>
  <th colspan="2">2011</th>
  <th colspan="2">2012</th>
  <th colspan="2">2013</th>
  <th colspan="2">2014</th>
  <th colspan="2">2015</th>
  <th colspan="2">2011-2015 Avg</th>
  <th colspan="2">Climatology</th>
  </tr>

<tr>
<th>Avg Temp</th><th>Precip</th>
<th>Avg Temp</th><th>Precip</th>
<th>Avg Temp</th><th>Precip</th>
<th>Avg Temp</th><th>Precip</th>
<th>Avg Temp</th><th>Precip</th>
<th>Avg Temp</th><th>Precip</th>
<th>Avg Temp</th><th>Precip</th>
</tr>
</thead>
%s

</table>
</body>
</html>
    """
        % (table,)
    )


if __name__ == "__main__":
    main()
