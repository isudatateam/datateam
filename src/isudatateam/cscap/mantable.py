"""Management Table used by cover crop paper"""

import datetime

from pandas.io.sql import read_sql
from pyiem.util import get_dbconn
from pyiem.webutil import iemapp

DBCONN = get_dbconn("sustainablecorn")
cursor = DBCONN.cursor()

ALL = " ALL SITES"
varorder = []
varlookup = {}

COVER_SITES = [
    "MASON",
    "KELLOGG",
    "GILMORE",
    "ISUAG",
    "WOOSTER.COV",
    "SEPAC",
    "BRADFORD.B1",
    "BRADFORD.B2",
    "BRADFORD.C",
    "FREEMAN",
]
D7 = datetime.timedelta(days=7)


@iemapp()
def application(environ, start_response):
    """Go Main"""
    start_response("200 OK", [("Content-type", "text/html")])
    cursor.execute(
        """
        SELECT uniqueid, valid, cropyear, operation, biomassdate1,
        biomassdate2, fertilizercrop, cashcrop from operations
        WHERE operation in ('harvest_corn', 'harvest_soy', 'plant_rye',
        'plant_rye-corn-res', 'plant_rye-soy-res', 'sample_soilnitrate',
        'sample_covercrop', 'termination_rye_corn', 'termination_rye_soy',
        'plant_corn', 'plant_soy', 'fertilizer_synthetic')
        and cropyear != '2016' and valid is not null
        ORDER by operation DESC, valid ASC
    """
    )
    data = {}
    for row in cursor:
        site = row[0]
        valid = row[1]  # datetime!
        cropyear = str(row[2])
        operation = row[3]
        biomassdate1 = row[4]
        # biomassdate2 = row[5]
        fertilizercrop = row[6]
        # cashcrop = row[7]
        if site not in data:
            data[site] = {}
            for cy in ["2011", "2012", "2013", "2014", "2015"]:
                data[site][cy] = {
                    "harvest_soy": "",
                    "harvest_corn": "",
                    "plant_rye": "",
                    "plant_rye-corn-res": "",
                    "plant_rye-soy-res": "",
                    "plant_corn": None,
                    "plant_soy": None,
                    "fall_sample_soilnitrate_corn": "",
                    "fall_sample_soilnitrate_soy": "",
                    "spring_sample_soilnitrate_corn": "",
                    "spring_sample_soilnitrate_soy": "",
                    "termination_rye_corn": "",
                    "termination_rye_soy": "",
                    "fertilizer_synthetic_starter": "",
                    "fertilizer_synthetic_sidedress": "",
                    "fertilizer_synthetic_preplant": "",
                    "fertilizer_synthetic_fall": "",
                    "spring_sample_covercrop_corn": "",
                    "spring_sample_covercrop_soy": "",
                    "fall_sample_covercrop_corn": "",
                    "fal_sample_covercrop_soy": "",
                }
        _d = data[site][cropyear]
        if operation == "plant_rye":
            for op2 in ["plant_rye-soy-res", "plant_rye-corn-res"]:
                _d[op2] = valid
        elif operation.startswith("termination_rye"):
            if operation.endswith("soy") and biomassdate1 is not None:
                _d["spring_sample_covercrop_soy"] = biomassdate1
            elif biomassdate1 is not None:
                _d["spring_sample_covercrop_corn"] = biomassdate1
            _d[operation] = valid
        elif operation == "fertilizer_synthetic" and fertilizercrop in [
            None,
            "multiple",
            "corn",
            "other",
        ]:
            plantcorndate = _d["plant_corn"]
            if plantcorndate is None:
                continue
            if valid.year < plantcorndate.year:
                _d[operation + "_fall"] = valid
            elif valid == plantcorndate:
                _d[operation + "_starter"] = valid
            elif valid < (plantcorndate + D7):
                _d[operation + "_preplant"] = valid
            else:
                _d[operation + "_sidedress"] = valid

        elif operation in ["sample_soilnitrate", "sample_covercrop"]:
            # We only want 'fall' events
            season = "fall_"
            if valid.month in [6, 7, 8]:
                continue
            elif valid.month < 6:
                season = "spring_"
            if _d[season + operation + "_soy"] != "":
                _d[season + operation + "_corn"] = valid
            else:
                data[site][cropyear][season + operation + "_soy"] = valid
        else:
            data[site][cropyear][operation] = valid

    table0 = ""
    df = read_sql(
        """
    WITH sites as (
        SELECT uniqueid, latitude, longitude, officialfarmname
        from metadata_master),
    plots as (
        SELECT uniqueid, soilseriesname1, soiltaxonomicclass1,
        soilseriesname2, soiltaxonomicclass2,
        row_number() OVER (PARTITION by uniqueid ORDER by soiltaxonomicclass1)
        from plotids),
    plots2 as (select * from plots where row_number = 1)
    SELECT s.uniqueid, s.latitude, s.longitude, s.officialfarmname,
    p.soilseriesname1, p.soiltaxonomicclass1,
    p.soilseriesname2, p.soiltaxonomicclass2 from sites s JOIN plots2 p
    on (s.uniqueid = p.uniqueid) ORDER by s.uniqueid ASC
    """,
        DBCONN,
        index_col="uniqueid",
    )
    for uniqueid, row in df.iterrows():
        if uniqueid not in COVER_SITES:
            continue
        table0 += (
            "<tr><td>%s</td><td>%s</td><td>%s</td>"
            "<td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td>"
            "</tr>"
        ) % (
            uniqueid,
            row["officialfarmname"],
            row["latitude"],
            row["longitude"],
            row["soilseriesname1"],
            row["soiltaxonomicclass1"],
            row["soilseriesname2"] or "--",
            row["soiltaxonomicclass2"] or "--",
        )

    table = ""
    for site in COVER_SITES:  # data.keys():
        if site not in data:
            continue
        table += "<tr><td>%s</td>" % (site,)
        for yr in ["2011", "2012", "2013", "2014", "2015"]:
            for op in ["harvest_corn", "harvest_soy"]:
                table += "<td>%s</td>" % (data[site].get(yr, {}).get(op, ""),)
            yr2 = str(int(yr) + 1)
            if yr != "2015":
                for op in ["plant_rye-corn-res", "plant_rye-soy-res"]:
                    table += "<td>%s</td>" % (
                        data[site].get(yr2, {}).get(op, ""),
                    )
        table += "</tr>"

    # ---------------------------------------------------------------
    table2 = ""
    for site in COVER_SITES:  # data.keys():
        if site not in data:
            continue
        table2 += "<tr><td>%s</td>" % (site,)
        for yr in ["2011", "2012", "2013", "2014", "2015"]:
            for op in [
                "fall_sample_soilnitrate_corn",
                "fall_sample_soilnitrate_soy",
            ]:
                table2 += "<td>%s</td>" % (data[site].get(yr, {}).get(op, ""),)
            yr2 = str(int(yr) + 1)
            for op in [
                "fall_sample_covercrop_corn",
                "fall_sample_covercrop_soy",
            ]:
                table2 += "<td>%s</td>" % (
                    data[site].get(yr2, {}).get(op, ""),
                )
        table2 += "</tr>"

    # ---------------------------------------------------------------
    table3 = ""
    for site in COVER_SITES:  # data.keys():
        if site not in data:
            continue
        table3 += "<tr><td>%s</td>" % (site,)
        for yr in ["2012", "2013", "2014", "2015"]:
            for op in [
                "spring_sample_covercrop_corn",
                "spring_sample_covercrop_soy",
            ]:
                table3 += "<td>%s</td>" % (data[site].get(yr, {}).get(op, ""),)
            for op in [
                "spring_sample_soilnitrate_corn",
                "spring_sample_soilnitrate_soy",
            ]:
                table3 += "<td>%s</td>" % (data[site].get(yr, {}).get(op, ""),)
            for op in ["termination_rye_corn", "termination_rye_soy"]:
                table3 += "<td>%s</td>" % (data[site].get(yr, {}).get(op, ""),)
        table3 += "</tr>"

    # ---------------------------------------------------------------
    table4 = ""
    for site in COVER_SITES:  # data.keys():
        if site not in data:
            continue
        table4 += "<tr><td>%s</td>" % (site,)
        for yr in ["2011", "2012", "2013", "2014", "2015"]:
            for op in ["plant_corn", "plant_soy"]:
                table4 += "<td>%s</td>" % (data[site].get(yr, {}).get(op, ""),)
        table4 += "</tr>"

    # ---------------------------------------------------------------
    table5 = ""
    for site in COVER_SITES:  # data.keys():
        if site not in data:
            continue
        table5 += "<tr><td>%s</td>" % (site,)
        for yr in ["2011", "2012", "2013", "2014", "2015"]:
            for op in [
                "fertilizer_synthetic_fall",
                "fertilizer_synthetic_preplant",
                "fertilizer_synthetic_starter",
                "fertilizer_synthetic_sidedress",
            ]:
                table5 += "<td>%s</td>" % (data[site].get(yr, {}).get(op, ""),)
        table5 += "</tr>"

    return [
        (
            """<!DOCTYPE html>
<html lang='en'>
<head>
 <link href="/vendor/bootstrap/3.3.5/css/bootstrap.min.css" rel="stylesheet">
 <link href="/css/bootstrap-override.css" rel="stylesheet">
</head>
<body>


<h3>Sites</h3>
<table class="table table-striped table-bordered">
<thead>
 <tr>
  <th>Site</th>
  <th>Name</th>
  <th>Latitude</th>
  <th>Longitude</th>
  <th>Primary Soil Series</th>
  <th>Primary Soil Taxonomic Class</th>
  <th>Secondary Soil Series</th>
  <th>Secondary Soil Taxonomic Class</th>
 </tr>
 </thead>
 %s
 </table>

<h3>Sub Table 1</h3>

<table class="table table-striped table-bordered">
<thead>
 <tr>
  <th rowspan="3">Site</th>
  <th colspan="4">Fall 2011</th>
  <th colspan="4">Fall 2012</th>
  <th colspan="4">Fall 2013</th>
  <th colspan="4">Fall 2014</th>
  <th colspan="2">Fall 2015</th>
 </tr>
 <tr>
  <th colspan="2">cash harvest</th>
  <th colspan="2">cover seeding</th>
  <th colspan="2">cash harvest</th>
  <th colspan="2">cover seeding</th>
  <th colspan="2">cash harvest</th>
  <th colspan="2">cover seeding</th>
  <th colspan="2">cash harvest</th>
  <th colspan="2">cover seeding</th>
  <th colspan="2">cash harvest</th>
 </tr>
 <tr>
  <th>Corn</th><th>Soybean</th>
  <th>Corn</th><th>Soybean</th>
  <th>Corn</th><th>Soybean</th>
  <th>Corn</th><th>Soybean</th>
  <th>Corn</th><th>Soybean</th>
  <th>Corn</th><th>Soybean</th>
  <th>Corn</th><th>Soybean</th>
  <th>Corn</th><th>Soybean</th>
  <th>Corn</th><th>Soybean</th>
 </tr>
</thead>
%s
</table>

<h3>Sub Table 2</h3>

<table class="table table-striped table-bordered">
<thead>
 <tr>
  <th rowspan="3">Site</th>
  <th colspan="4">Fall 2011</th>
  <th colspan="4">Fall 2012</th>
  <th colspan="4">Fall 2013</th>
  <th colspan="4">Fall 2014</th>
  <th colspan="4">Fall 2015</th>
 </tr>
 <tr>
  <th colspan="2">Fall Soil Nitrate</th>
  <th colspan="2">Fall Cover Crop Sample</th>
  <th colspan="2">Fall Soil Nitrate</th>
  <th colspan="2">Fall Cover Crop Sample</th>
  <th colspan="2">Fall Soil Nitrate</th>
  <th colspan="2">Fall Cover Crop Sample</th>
  <th colspan="2">Fall Soil Nitrate</th>
  <th colspan="2">Fall Cover Crop Sample</th>
  <th colspan="2">Fall Soil Nitrate</th>
  <th colspan="2">Fall Cover Crop Sample</th>
 </tr>
 <tr>
  <th>after C</th><th>after S</th>
  <th>after C</th><th>after S</th>
  <th>after C</th><th>after S</th>
  <th>after C</th><th>after S</th>
  <th>after C</th><th>after S</th>
  <th>after C</th><th>after S</th>
  <th>after C</th><th>after S</th>
  <th>after C</th><th>after S</th>
  <th>after C</th><th>after S</th>
  <th>after C</th><th>after S</th>
 </tr>
</thead>
%s
</table>

<h3>Sub Table 3</h3>

<table class="table table-striped table-bordered">
<thead>
 <tr>
  <th rowspan="3">Site</th>
  <th colspan="6">Spring 2012</th>
  <th colspan="6">Spring 2013</th>
  <th colspan="6">Spring 2014</th>
  <th colspan="6">Spring 2015</th>
 </tr>
 <tr>
  <th colspan="2">Rye Sampling (spring)</th>
  <th colspan="2">Soil N Sampling (spring)</th>
  <th colspan="2">Termination</th>
  <th colspan="2">Rye Sampling (spring)</th>
  <th colspan="2">Soil N Sampling (spring)</th>
  <th colspan="2">Termination</th>
  <th colspan="2">Rye Sampling (spring)</th>
  <th colspan="2">Soil N Sampling (spring)</th>
  <th colspan="2">Termination</th>
  <th colspan="2">Rye Sampling (spring)</th>
  <th colspan="2">Soil N Sampling (spring)</th>
  <th colspan="2">Termination</th>
 </tr>
 <tr>
  <th>before C</th><th>before S</th>
  <th>before C</th><th>before S</th>
  <th>before C</th><th>before S</th>
  <th>before C</th><th>before S</th>
  <th>before C</th><th>before S</th>
  <th>before C</th><th>before S</th>
  <th>before C</th><th>before S</th>
  <th>before C</th><th>before S</th>
  <th>before C</th><th>before S</th>
  <th>before C</th><th>before S</th>
  <th>before C</th><th>before S</th>
  <th>before C</th><th>before S</th>
 </tr>
</thead>
%s
</table>

<h3>Cash Crop Planting</h3>

<table class="table table-striped table-bordered">
<thead>
 <tr>
  <th rowspan="3">Site</th>
  <th colspan="2">2011</th>
  <th colspan="2">2012</th>
  <th colspan="2">2013</th>
  <th colspan="2">2014</th>
  <th colspan="2">2015</th>
 </tr>
 <tr>
  <th>Corn</th><th>Soybean</th>
  <th>Corn</th><th>Soybean</th>
  <th>Corn</th><th>Soybean</th>
  <th>Corn</th><th>Soybean</th>
  <th>Corn</th><th>Soybean</th>
 </tr>
</thead>
%s
</table>

<h3>Fertilizer N Application</h3>

<table class="table table-striped table-bordered">
<thead>
 <tr>
  <th rowspan="3">Site</th>
  <th colspan="4">2011 Cash Crop</th>
  <th colspan="4">2012 Cash Crop</th>
  <th colspan="4">2013 Cash Crop</th>
  <th colspan="4">2014 Cash Crop</th>
  <th colspan="4">2015 Cash Crop</th>
 </tr>
 <tr>
  <th>Fall</th><th>Pre-Plant</th><th>Starter</th><th>Side Dress</th>
  <th>Fall</th><th>Pre-Plant</th><th>Starter</th><th>Side Dress</th>
  <th>Fall</th><th>Pre-Plant</th><th>Starter</th><th>Side Dress</th>
  <th>Fall</th><th>Pre-Plant</th><th>Starter</th><th>Side Dress</th>
  <th>Fall</th><th>Pre-Plant</th><th>Starter</th><th>Side Dress</th>
 </tr>
</thead>
%s
</table>

</body>
</html>
    """
            % (table0, table, table2, table3, table4, table5)
        ).encode("utf-8")
    ]
