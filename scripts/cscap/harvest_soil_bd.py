"""Scrape out the Soil Bulk Density and Texture data from Google Drive"""

from __future__ import print_function

import sys

import psycopg2

import isudatateam.cscap_utils as util

YEAR = sys.argv[1]

config = util.get_config()

pgconn = psycopg2.connect(
    database="sustainablecorn", host=config["database"]["host"]
)
pcursor = pgconn.cursor()

# Get me a client, stat
spr_client = util.get_spreadsheet_client(config)
drive_client = util.get_driveclient(config)

res = (
    drive_client.files()
    .list(
        q=("title contains '%s'")
        % (("Soil Bulk Density and " "Water Retention Data"),)
    )
    .execute()
)

DOMAIN = [
    "SOIL1",
    "SOIL2",
    "SOIL29",
    "SOIL30",
    "SOIL31",
    "SOIL32",
    "SOIL8",
    "SOIL33",
    "SOIL34",
    "SOIL35",
    "SOIL39",
    "SOIL41",
    "SOIL42",
]

# Load up current data, incase we need to do some deleting
current = {}
pcursor.execute(
    """
    SELECT uniqueid, plotid, varname, depth, subsample
    from soil_data WHERE year = %s and varname in %s
    """,
    (YEAR, tuple(DOMAIN)),
)
for row in pcursor:
    key = "|".join([str(s) for s in row])
    current[key] = True

for item in res["items"]:
    if item["mimeType"] != "application/vnd.google-apps.spreadsheet":
        continue
    try:
        # print("Processing %s %s" % (item['title'], item['id']))
        spreadsheet = util.Spreadsheet(spr_client, item["id"])
    except Exception as exp:
        print("harvest_soil_bd FAIL: %s\n%s" % (exp, item["title"]))
        continue
    siteid = item["title"].split()[0]
    spreadsheet.get_worksheets()
    worksheet = spreadsheet.worksheets.get(YEAR)
    if worksheet is None:
        # print 'Missing Soil BD+WR %s sheet for %s' % (YEAR, siteid)
        continue
    worksheet.get_cell_feed()
    if siteid == "DPAC":
        pass
    elif (
        worksheet.get_cell_value(1, 1) != "plotid"
        or worksheet.get_cell_value(1, 2) != "depth"
        or worksheet.get_cell_value(1, 3) != "subsample"
    ):
        print(
            ("FATAL site: %s(%s) bd & wr has bad header 1:%s 2:%s 3:%s")
            % (
                siteid,
                YEAR,
                worksheet.get_cell_value(1, 1),
                worksheet.get_cell_value(1, 2),
                worksheet.get_cell_value(1, 3),
            )
        )
        continue

    for row in range(3, worksheet.rows + 1):
        plotid = worksheet.get_cell_value(row, 1)
        if siteid == "DPAC":
            depth = worksheet.get_cell_value(row, 3)
            # Combine the location value into the subsample
            subsample = "%s%s" % (
                worksheet.get_cell_value(row, 2),
                worksheet.get_cell_value(row, 4),
            )
        else:
            depth = worksheet.get_cell_value(row, 2)
            subsample = worksheet.get_cell_value(row, 3)
        if depth.find(" to ") == -1:
            print(
                ("harvest_soil_bd found invalid depth: %s %s %s")
                % (depth, siteid, YEAR)
            )
            continue
        if plotid is None or depth is None:
            continue
        for col in range(4, worksheet.cols + 1):
            if worksheet.get_cell_value(1, col) is None:
                # print(("harvest_soil_bd %s(%s) row: %s col: %s is null"
                #       ) % (siteid, YEAR, row, col))
                continue
            varname = worksheet.get_cell_value(1, col).strip().split()[0]
            if varname[:4] != "SOIL":
                # print 'Invalid varname: %s site: %s year: %s' % (
                #                    worksheet.get_cell_value(1,col).strip(),
                #                    siteid, YEAR)
                continue
            inval = worksheet.get_cell_value(row, col)
            val = util.cleanvalue(inval)
            if inval is not None and val is None:
                print(
                    (
                        "harvest_soil_bd found None. site: %s year: %s "
                        " row: %s col: %s varname: %s"
                    )
                    % (siteid, YEAR, row, col, varname)
                )
            try:
                pcursor.execute(
                    """
                    INSERT into soil_data(uniqueid, plotid, varname, year,
                    depth, value, subsample)
                    values (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (siteid, plotid, varname, YEAR, depth, val, subsample),
                )
            except Exception as exp:
                print("HARVEST_SOIL_BD TRACEBACK")
                print(exp)
                print(
                    ("%s %s %s %s %s %s")
                    % (siteid, plotid, varname, depth, val, subsample)
                )
                sys.exit()
            key = "%s|%s|%s|%s|%s" % (
                siteid,
                plotid,
                varname,
                depth,
                subsample,
            )
            if key in current:
                del current[key]

for key in current:
    (siteid, plotid, varname, depth, subsample) = key.split("|")
    if varname in DOMAIN:
        print(
            ("harvest_soil_bd rm %s %s %s %s %s %s")
            % (YEAR, siteid, plotid, varname, repr(depth), repr(subsample))
        )
        d1 = "depth is null" if depth == "None" else "depth = '%s'" % (depth,)
        d2 = (
            "subsample is null"
            if subsample == "None"
            else "subsample = '%s'" % (subsample,)
        )
        pcursor.execute(
            """DELETE from soil_data where uniqueid = %s and
        plotid = %s and varname = %s and year = %s and """
            + d1
            + """ and
        """
            + d2,
            (siteid, plotid, varname, YEAR),
        )


pcursor.close()
pgconn.commit()
pgconn.close()
