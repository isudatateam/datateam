"""
Harvest the Agronomic Data into the ISU Database
"""
import sys

import psycopg2
import pyiem.cscap_utils as util

config = util.get_config()

pgconn = psycopg2.connect(database='td',
                          host=config['database']['host'])

# Get me a client, stat
spr_client = util.get_spreadsheet_client(config)
drive_client = util.get_driveclient(config, "td")


def delete_entries(current, siteid):
    for key in current:
        (plotid, varname) = key.split("|")
        print(('harvest_agronomic REMOVE %s %s %s'
               ) % (siteid, plotid, varname))
        pcursor.execute("""DELETE from agronomic_data where uniqueid = %s and
            plotid = %s and varname = %s and year = %s
        """, (siteid, plotid, varname, YEAR))


res = drive_client.files().list(q="title contains 'Crop Yield Data'").execute()

for item in res['items']:
    if item['mimeType'] != 'application/vnd.google-apps.spreadsheet':
        continue
    siteid = item['title'].split()[0]
    spreadsheet = util.Spreadsheet(spr_client, item['id'])
    spreadsheet.get_worksheets()
    for YEAR in spreadsheet.worksheets:
        pcursor = pgconn.cursor()
        print("Efforting: %s[%s]" % (siteid, YEAR))
        # Load up current data, incase we need to do some deleting
        current = {}
        pcursor.execute("""
            SELECT plotid, varname
            from agronomic_data WHERE uniqueid = %s and year = %s
        """, (siteid, YEAR))
        for row in pcursor:
            key = "%s|%s" % row
            current[key] = True

        worksheet = spreadsheet.worksheets.get(YEAR)
        if worksheet is None:
            delete_entries(current, siteid)
            continue
        worksheet.get_cell_feed()
        newvals = 0

        for col in range(1, worksheet.cols+1):
            val = worksheet.get_cell_value(1, col)
            # print("considering col:%s val:'%s'" % (col, repr(val)))
            if val is None:
                continue
            if val.lower().replace(" ", "").find("plotid") == 0:
                plotidcol = col
                # print("Plot ID column is: %s val: %s" % (plotidcol, val))
            if val.find("AGR") != 0:
                continue
            varname = val.split()[0]
            for row in range(3, worksheet.rows+1):
                plotid = worksheet.get_cell_value(row, plotidcol)
                if plotid is None:
                    continue
                inval = worksheet.get_cell_value(row, col)
                val = util.cleanvalue(inval)
                if inval is not None and val is None:
                    print(("harvest_agronomic found None. site: %s year: %s "
                           " row: %s col: %s varname: %s"
                           ) % (siteid, YEAR, row, col, varname))
                # print row, col, plotid, varname, YEAR, val
                try:
                    pcursor.execute("""
                        INSERT into agronomic_data
                        (uniqueid, plotid, varname, year, value)
                        values (%s, %s, %s, %s, %s) RETURNING value
                        """, (siteid, plotid, varname, YEAR, val))
                    if pcursor.rowcount == 1:
                        newvals += 1
                except Exception as exp:
                    print('HARVEST_AGRONOMIC TRACEBACK')
                    print(exp)
                    print(('%s %s %s %s %s'
                           ) % (YEAR, siteid, plotid,
                                repr(varname), repr(val)))
                    sys.exit()
                key = "%s|%s" % (plotid, varname)
                if key in current:
                    del current[key]
        delete_entries(current, siteid)
        if newvals > 0:
            print(('harvest_agronomic year: %s site: %s had %s new values'
                   '') % (YEAR, siteid, newvals))

        pcursor.close()
        pgconn.commit()
pgconn.close()
