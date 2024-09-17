"""Harvest Soil Fertility Files"""

import sys

import psycopg2

import isudatateam.cscap_utils as util

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
    .list(q=("title contains '%s'") % (("Soil Fertility Analysis Data"),))
    .execute()
)

# Load up current data, incase we need to do some deleting
current = {}
pcursor.execute(
    """
    SELECT uniqueid, plotid, varname, depth, year, sampledate, value
    from soil_data WHERE varname ~* 'SOIL19'
    """
)
for row in pcursor:
    key = "|".join([str(s) for s in row[:-1]])
    current[key] = row[-1]

newvals = 0
for item in res["items"]:
    if item["mimeType"] != "application/vnd.google-apps.spreadsheet":
        continue
    try:
        # print("Processing %s %s" % (item['title'], item['id']))
        spreadsheet = util.Spreadsheet(spr_client, item["id"])
    except Exception as exp:
        print("harvest_soil_fertility FAIL: %s\n%s" % (exp, item["title"]))
        continue
    siteid = item["title"].split()[0]
    spreadsheet.get_worksheets()
    worksheet = spreadsheet.worksheets.get("SOIL19")
    if worksheet is None:
        print(
            ("harvest_soil_fertility %s doesn't have SOIL19 tab")
            % (item["title"],)
        )
        continue
    worksheet.get_cell_feed()
    if (
        worksheet.get_cell_value(1, 4) != "plotid"
        or worksheet.get_cell_value(1, 5) != "depth"
    ):
        print(
            "FATAL site: %s fert has bad header 4:%s 5:%s"
            % (
                siteid,
                worksheet.get_cell_value(1, 4),
                worksheet.get_cell_value(1, 5),
            )
        )
        continue

    for row in range(3, worksheet.rows + 1):
        plotid = worksheet.get_cell_value(row, 4)
        if siteid.startswith("NAEW"):
            siteid = "NAEW.WS%s" % (worksheet.get_cell_value(row, 6),)
        year = worksheet.get_cell_value(row, 1)
        depth = worksheet.get_cell_value(row, 5)
        if depth is None or depth.find(" to ") == -1:
            if depth is not None:
                print(
                    ("harvest_soil_fertility found invalid depth: %s %s")
                    % (depth, siteid)
                )
            continue
        sampledate = worksheet.get_cell_value(row, 3)
        if None in [plotid, depth, year]:
            continue
        year = year.replace("?", "")
        for col in range(6, worksheet.cols + 1):
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
            key = "%s|%s|%s|%s|%s|%s" % (
                siteid,
                plotid,
                varname,
                depth,
                year,
                sampledate,
            )
            if key in current:
                oldval = current[key]
                del current[key]
                if oldval is None and val is None:
                    continue
                if val is not None:
                    # Database storage is in string, so we can't easily
                    # compare here with what was previously in the database
                    val = str(val)
                if oldval == val:
                    continue
                # Hacccccccckkkkkkkkkkk
                if oldval is not None and oldval.startswith(val[:5]):
                    continue
                print(
                    ("new site: %s year: %s oldval: %s newval: %s")
                    % (siteid, year, repr(oldval), repr(val))
                )
            try:
                pcursor.execute(
                    """
                    INSERT into soil_data(uniqueid, plotid, varname, year,
                    depth, value, sampledate)
                    values (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (siteid, plotid, varname, year, depth, val, sampledate),
                )
                newvals += 1
            except Exception as exp:
                print("HARVEST_SOIL_FERTILITY TRACEBACK")
                print(exp)
                print(
                    "%s %s %s %s %s %s"
                    % (siteid, plotid, varname, depth, val, sampledate)
                )
                sys.exit()

deletedvals = 0
for key in current:
    (siteid, plotid, varname, depth, year, sampledate) = key.split("|")
    print(
        ("harvest_soil_fert rm %s %s %s %s %s %s")
        % (year, siteid, plotid, varname, repr(depth), sampledate)
    )
    if sampledate == "None":
        sql = "sampledate is null"
    else:
        sql = "sampledate = '%s'" % (sampledate,)
    pcursor.execute(
        """DELETE from soil_data where uniqueid = %s and
    plotid = %s and varname = %s and year = %s and """
        + sql
        + """
    """,
        (siteid, plotid, varname, year),
    )
    deletedvals += 1

if newvals > 0 or deletedvals > 0:
    print(
        "harvest_soil_fert, newvals: %s, deleted: %s" % (newvals, deletedvals)
    )
pcursor.close()
pgconn.commit()
pgconn.close()
