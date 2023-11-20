""" Print out a big thing of progress bars, gasp """

from pyiem.util import get_dbconn
from pyiem.webutil import iemapp

DBCONN = get_dbconn("sustainablecorn")
cursor = DBCONN.cursor()

ALL = " ALL SITES"
varorder = []
varlookup = {}

CFG = "/opt/datateam/config/mytokens.json"


def get_data(year, mode):
    """Do stuff"""
    data = {ALL: {}}
    dvars = []
    table = "agronomic_data" if mode == "agronomic" else "soil_data"
    cursor.execute(
        """SELECT uniqueid, varname,
    -- We have some number
    sum(case when lower(value) not in ('.','','did not collect','n/a') and
        value is not null then 1 else 0 end),
    -- Periods
    sum(case when lower(value) in ('.') then 1 else 0 end),
    -- We have some value, not a number
 sum(case when lower(value) in ('did not collect', 'n/a') then 1 else 0 end),
    -- We have a null
    sum(case when value is null then 1 else 0 end),
    count(*) from """
        + table
        + """
    WHERE year = %s and (value is Null or lower(value) != 'n/a')
    GROUP by uniqueid, varname""",
        (year,),
    )
    for row in cursor:
        if row[1] not in dvars:
            dvars.append(row[1])
        if row[0] not in data:
            data[row[0]] = {}
        data[row[0]][row[1]] = {
            "hits": row[2],
            "dots": row[3],
            "other": row[4],
            "nulls": row[5],
            "tot": row[6],
        }
        if row[1] not in data[ALL]:
            data[ALL][row[1]] = {
                "hits": 0,
                "dots": 0,
                "other": 0,
                "nulls": 0,
                "tot": 0,
            }
        data[ALL][row[1]]["hits"] += row[2]
        data[ALL][row[1]]["dots"] += row[3]
        data[ALL][row[1]]["other"] += row[4]
        data[ALL][row[1]]["nulls"] += row[5]
        data[ALL][row[1]]["tot"] += row[6]

    return data, dvars


def make_progress(row):
    """return string for progress bar"""
    if row is None:
        return ""
    hits = row["hits"] / float(row["tot"]) * 100.0
    dots = row["dots"] / float(row["tot"]) * 100.0
    other = row["other"] / float(row["tot"]) * 100.0
    nulls = row["nulls"] / float(row["tot"]) * 100.0
    return """<div class="progress">
  <div class="progress-bar progress-bar-success" style="width: %.1f%%">
    <span>%s</span>
  </div>
  <div class="progress-bar progress-bar-info" style="width: %.1f%%">
    <span>%s</span>
  </div>
  <div class="progress-bar progress-bar-warning" style="width: %.1f%%">
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
        other - 0.05,
        row["other"],
        nulls - 0.05,
        row["nulls"],
    )


@iemapp()
def application(environ, start_response):
    """Go Main Go"""
    headers = [("Content-type", "text/html")]
    start_response("200 OK", headers)
    year = int(environ.get("year", 2011))
    mode = environ.get("mode", "agronomic")

    data, dvars = get_data(year, mode)

    sites = list(data.keys())
    sites.sort()
    payload = """<!DOCTYPE html>
    <html lang='en'>
    <head>
<link href="/vendor/bootstrap/3.3.5/css/bootstrap.min.css" rel="stylesheet">
    <link href="/css/bootstrap-override.css" rel="stylesheet">
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

<div class="row well">
    <div class="col-md-4 col-sm-4">Select Mode:</div>
    <div class="col-md-4 col-sm-4">
        <a href="dataprogress.py?mode=agronomic">Agronomic Data</a>
    </div>
    <div class="col-md-4 col-sm-4">
        <a href="dataprogress.py?mode=soil">Soil Data</a>
    </div>
</div>

    <form method="GET" name='theform'>
    <input type="hidden" name="mode" value="%s" />
    Select Year; <select name="year">
    """ % (mode,)
    for yr in range(2011, 2016):
        checked = ""
        if year == yr:
            checked = " selected='selected'"
        payload += f"""<option value="{yr}" {checked}>{yr}</option>\n"""

    payload += "</select><br />"

    ids = environ.get("ids", [])
    if isinstance(ids, str):
        ids = [ids]
    dvars = varorder
    if ids:
        dvars = ids
    for varid in varorder:
        checked = ""
        if varid in ids:
            checked = "checked='checked'"
        payload += """<input type='checkbox' name='ids'
        value='%s'%s><abbr title="%s">%s</abbr></input> &nbsp;
        """ % (
            varid,
            checked,
            varlookup[varid],
            varid,
        )

    payload += """
    <input type="submit" value="Generate Table">
    </form>
    <span>Key:</span>
    <span class="btn btn-success">has data</span>
    <span class="btn btn-info">periods</span>
    <span class="btn btn-warning">did not collect</span>
    <span class="btn btn-danger">no entry / empty</span>
    <table class='table table-striped table-bordered'>
    <thead><tr><th>SiteID</th>"""
    for dv in dvars:
        payload += f"""<th><abbr title="{varlookup[dv]}">{dv}</abbr></th>"""
    payload += "</tr></thead>"
    for sid in sites:
        payload += """<tr><th>%s</th>""" % (sid,)
        for datavar in dvars:
            row = data[sid].get(datavar, None)
            payload += f"<td>{make_progress(row)}</td>"
        payload += "</tr>\n\n"
    payload += "</table>"

    payload += """
    <h3>Data summary for all sites included</h3>
    <p>
    <span>Key:</span>
    <span class="btn btn-success">has data</span>
    <span class="btn btn-info">periods</span>
    <span class="btn btn-warning">DNC empty</span>
    <span class="btn btn-danger">no entry</span>
    <table class='table table-striped table-bordered'>
    <thead><tr><th width="33%%">Variable</th><th width="66%%">%s</th></tr>
    """ % (ALL,)
    for datavar in dvars:
        row = data[ALL].get(datavar, None)
        payload += ("<tr><th>%s %s</th><td>%s</td></tr>") % (
            datavar,
            varlookup[datavar],
            make_progress(row),
        )

    payload += "</table></p>"
    return [payload.encode("ascii")]
