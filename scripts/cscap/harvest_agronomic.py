"""
Harvest the Agronomic Data into the ISU Database
"""

import sys

import psycopg2

import isudatateam.cscap_utils as util

YEAR = sys.argv[1]


def delete_entries(pcursor, current, siteid):
    for key in current:
        (plotid, varname) = key.split("|")
        print("harvest_agronomic REMOVE %s %s %s" % (siteid, plotid, varname))
        pcursor.execute(
            """DELETE from agronomic_data where uniqueid = %s and
            plotid = %s and varname = %s and year = %s
        """,
            (siteid, plotid, varname, YEAR),
        )


def main():
    """Go Main."""
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
        .list(q="title contains 'Agronomic Data'")
        .execute()
    )

    for item in res["items"]:
        if item["mimeType"] != "application/vnd.google-apps.spreadsheet":
            continue
        siteid = item["title"].split()[0]
        # Load up current data, incase we need to do some deleting
        current = {}
        pcursor.execute(
            """SELECT plotid, varname
        from agronomic_data WHERE uniqueid = %s and year = %s""",
            (siteid, YEAR),
        )
        for row in pcursor:
            key = "%s|%s" % row
            current[key] = True

        spreadsheet = util.Spreadsheet(spr_client, item["id"])
        spreadsheet.get_worksheets()
        worksheet = spreadsheet.worksheets.get(YEAR)
        if worksheet is None:
            delete_entries(pcursor, current, siteid)
            continue
        worksheet.get_cell_feed()
        newvals = 0

        for col in range(1, worksheet.cols + 1):
            val = worksheet.get_cell_value(1, col)
            if val is None:
                continue
            if val.find("PlotID") == 0:
                plotidcol = col
            if val.find("AGR") != 0:
                continue
            varname = val
            for row in range(4, worksheet.rows + 1):
                plotid = worksheet.get_cell_value(row, plotidcol)
                if plotid is None:
                    continue
                inval = worksheet.get_cell_value(row, col)
                val = util.cleanvalue(inval)
                if inval is not None and val is None:
                    print(
                        (
                            "harvest_agronomic found None. site: %s year: %s "
                            " row: %s col: %s varname: %s"
                        )
                        % (siteid, YEAR, row, col, varname)
                    )
                # print row, col, plotid, varname, YEAR, val
                try:
                    pcursor.execute(
                        """
                        INSERT into agronomic_data
                        (uniqueid, plotid, varname, year, value)
                        values (%s, %s, %s, %s, %s) RETURNING value
                        """,
                        (siteid, plotid, varname, YEAR, val),
                    )
                    if pcursor.rowcount == 1:
                        newvals += 1
                except Exception as exp:
                    print("HARVEST_AGRONOMIC TRACEBACK")
                    print(exp)
                    print(
                        "%s %s %s %s %s"
                        % (YEAR, siteid, plotid, repr(varname), repr(val))
                    )
                    sys.exit()
                key = "%s|%s" % (plotid, varname)
                if key in current:
                    del current[key]
        delete_entries(current, siteid)
        if newvals > 0:
            print(
                ("harvest_agronomic year: %s site: %s had %s new values")
                % (YEAR, siteid, newvals)
            )

    pcursor.close()
    pgconn.commit()
    pgconn.close()


if __name__ == "__main__":
    main()
