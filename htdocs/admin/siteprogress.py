"""Shrug."""

import datetime
from io import StringIO

from pandas.io.sql import read_sql
from pyiem.util import get_dbconn
from pyiem.webutil import iemapp

DBCONN = get_dbconn("sustainablecorn")
cursor = DBCONN.cursor()


def get_data(mode, data, arr):
    """Do stuff"""
    table = "agronomic_data" if mode == "agronomic" else "soil_data"
    cursor.execute(
        """SELECT uniqueid,
    -- We have some number
    sum(case when lower(value) not in ('.','','did not collect','n/a') and
        value is not null then 1 else 0 end),
    -- Periods
    sum(case when lower(value) in ('.') then 1 else 0 end),
    -- We have some value, not a number
    sum(case when lower(value) in ('did not collect', 'n/a')
        then 1 else 0 end),
    -- We have a null
    sum(case when value is null then 1 else 0 end),
    count(*) from """
        + table
        + """
    WHERE (value is Null or lower(value) != 'n/a')
    GROUP by uniqueid"""
    )
    for row in cursor:
        for site in [row[0], "_ALL"]:
            entry = data.setdefault(
                site,
                dict(
                    hits=0,
                    dots=0,
                    other=0,
                    nulls=0,
                    tot=0,
                    all=0,
                    hits2=0,
                    other2=0,
                ),
            )
            tot = 0
            entry["hits2"] += row[1]
            entry["other2"] += row[3]
            if arr[0]:
                entry["hits"] += row[1]
                tot += row[1]
            if arr[1]:
                entry["dots"] += row[2]
                tot += row[2]
            if arr[2]:
                entry["other"] += row[3]
                # tot += row[3]
            if arr[3]:
                entry["nulls"] += row[4]
                tot += row[4]
            entry["tot"] += tot
            entry["all"] += row[5] - row[3]


def make_progress(row):
    """return string for progress bar"""
    if row is None:
        return ""
    hits = row["hits"] / float(row["tot"]) * 100.0
    dots = row["dots"] / float(row["tot"]) * 100.0
    # other = row['other'] / float(row['tot']) * 100.0
    nulls = row["nulls"] / float(row["tot"]) * 100.0
    return """<div class="progress">
  <div class="progress-bar progress-bar-success" style="width: %.1f%%">
    <span>%s</span>
  </div>
  <div class="progress-bar progress-bar-info" style="width: %.1f%%">
    <span>%s</span>
  </div>
  <div class="progress-bar progress-bar-danger" style="width: %.1f%%">
    <span>%s</span>
  </div>
</div>""" % (
        hits - 0.05,
        row["hits"],
        dots - 0.05,
        row["dots"],
        # other - 0.05, row['other'],
        nulls - 0.05,
        row["nulls"],
    )


def do_site(sio, site):
    """Print out a simple listing of trouble"""
    df = read_sql(
        """
    with ag as (
        select year, varname, value, count(*) from agronomic_data
        where uniqueid = %s and (value is null or value in ('', '.'))
        GROUP by year, varname, value),
    soil as (
        select year, varname, value, count(*) from soil_data
        where uniqueid = %s and (value is null or value in ('', '.'))
        GROUP by year, varname, value)

    SELECT * from ag UNION select * from soil ORDER by year ASC, varname ASC
    """,
        DBCONN,
        params=(site, site),
        index_col=None,
    )
    sio.write("CSCAP Variable Progress Report\n")
    sio.write("Site: %s\n" % (site,))
    sio.write(
        "Generated: %s\n"
        % (datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),)
    )
    sio.write("Total Missing: %s\n" % (df["count"].sum(),))
    sio.write("%4s %-10s %-10s %-6s\n" % ("YEAR", "VARNAME", "VALUE", "COUNT"))

    def nice(val):
        if val is None:
            return "Empty"
        if val == "":
            return "Empty"
        if val == ".":
            return "Period"
        return val

    for _, row in df.iterrows():
        sio.write(
            "%s %-10s %-10s %-6s\n"
            % (row["year"], row["varname"], nice(row["value"]), row["count"])
        )


@iemapp()
def application(environ, start_response):
    """Handle mod_wsgi request."""
    sio = StringIO()
    if "site" in environ:
        do_site(sio, environ.get("site"))
        return
    # mode = form.getfirst('mode', 'agronomic')
    show_has = environ.get("has", "0") == "1"
    show_period = environ.get("period", "0") == "1"
    show_dnc = environ.get("dnc", "0") == "1"
    show_no = environ.get("no", "0") == "1"
    if environ.get("a") is None:
        show_has = True
        show_period = True
        show_dnc = True
        show_no = True
    # Forget the above, we hard code things like so
    show_has = True
    show_period = True
    show_dnc = True
    show_no = True
    data = {}
    arr = [show_has, show_period, show_dnc, show_no]
    get_data("agronomic", data, arr)
    get_data("soils", data, arr)

    sites = list(data.keys())
    sites.sort()
    sio.write("Content-type: text/html\n\n")
    sio.write(
        """<!DOCTYPE html>
<html lang='en'>
<head>
<link href="/vendor/bootstrap/3.3.5/css/bootstrap.min.css" rel="stylesheet">
    <title>CSCAP Research Site Agronomic+Soils Data Progress</title>
    </head>
    <body>
    <style>
    .progress{
     margin-bottom: 0px;
    }
    .progress-bar {
    z-index: 1;
 }
.progress span {
    color: black;
    z-index: 2;
 }
    </style>
    <!--
    <form method="GET" name="c">
    <p><strong>Which statuses to show?</strong> &nbsp;
    <input type="hidden" name="a" value="b">
    <input type="checkbox" name="has" value="1"%s>has data &nbsp;
    <input type="checkbox" name="period" value="1"%s>periods (missing) &nbsp;
    <input type="checkbox" name="dnc" value="1"%s>did not collect &nbsp;
    <input type="checkbox" name="no" value="1"%s>no entry / empty &nbsp;
    <input type="submit" value="Update Page">
    </p>
    </form> -->
    <p><span>Key:</span>
    <span class="btn btn-success">has data</span>
    <span class="btn btn-info">periods (missing)</span><!--
    <span class="btn btn-warning">did not collect</span> -->
    <span class="btn btn-danger">no entry / empty</span>

    <p>This page lists the data progress for Agronomic + Soils variables
    collected by the Google Spreadsheets. These values are valid for the
    duration of the project 2011-2015. This page DOES NOT list data progress
    for management metadata, greenhouse gas, drainage, soil moisture,
    nor pest (IPM) data.</p>

<table class='table table-striped table-bordered'>
<thead><tr>
    <th width="20%%">SiteID</th>
    <th width="60%%">Progress</th>
    <th width="10%%">Count</th>
    <th width="10%%">Percent Done</th>
</tr></thead>
    """
        % (
            "" if not show_has else ' checked="checked"',
            "" if not show_period else ' checked="checked"',
            "" if not show_dnc else ' checked="checked"',
            "" if not show_no else ' checked="checked"',
        )
    )
    for sid in sites:
        if sid == "_ALL":
            continue
        sio.write(
            """
        <tr><th>
<a href="siteprogress.py?site=%s">
<i class="glyphicon glyphicon-search"></i> %s</a></th>
        """
            % (sid, sid)
        )
        row = data[sid]
        sio.write("<td>%s</td>" % (make_progress(row)))
        sio.write("<td>%.0f</td>" % (row["tot"],))
        sio.write(
            "<td>%.0f%%</td>" % (((row["hits2"]) / float(row["all"])) * 100.0)
        )
        sio.write("</tr>\n\n")
    sid = "_ALL"
    sio.write("""<tr><th>%s</th>""" % (sid,))
    row = data[sid]
    sio.write("<td>%s</td>" % (make_progress(row)))
    sio.write("<td>%.0f</td>" % (row["tot"],))
    sio.write(
        "<td>%.0f%%</td>" % (((row["hits2"]) / float(row["all"])) * 100.0)
    )
    sio.write("</tr>\n\n")
    sio.write("</table>")

    start_response("200 OK", [("Content-type", "text/html")])
    return [sio.getvalue().encode("ascii")]
