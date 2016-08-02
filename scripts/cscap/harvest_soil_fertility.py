"""Harvest Soil Fertility Files"""
import pyiem.cscap_utils as util
import sys
import psycopg2

config = util.get_config()

pgconn = psycopg2.connect(database='sustainablecorn',
                          host=config['database']['host'])
pcursor = pgconn.cursor()

# Get me a client, stat
spr_client = util.get_spreadsheet_client(config)
drive_client = util.get_driveclient(config)

res = drive_client.files().list(q=("title contains '%s'"
                                   ) % (('Soil Fertility Analysis Data'),)
                                ).execute()

# Load up current data, incase we need to do some deleting
current = {}
pcursor.execute("""
    SELECT site, plotid, varname, depth, year
    from soil_data WHERE varname ~* 'SOIL19'
    """)
for row in pcursor:
    key = "|".join([str(s) for s in row])
    current[key] = True

for item in res['items']:
    if item['mimeType'] != 'application/vnd.google-apps.spreadsheet':
        continue
    try:
        # print("Processing %s %s" % (item['title'], item['id']))
        spreadsheet = util.Spreadsheet(spr_client, item['id'])
    except Exception, exp:
        print("harvest_soil_fertility FAIL: %s\n%s" % (exp, item['title']))
        continue
    siteid = item['title'].split()[0]
    spreadsheet.get_worksheets()
    worksheet = spreadsheet.worksheets.get('SOIL19')
    if worksheet is None:
        print(("harvest_soil_fertility %s doesn't have SOIL19 tab"
               ) % (item['title'],))
        continue
    worksheet.get_cell_feed()
    if (worksheet.get_cell_value(1, 4) != 'plotid' or
            worksheet.get_cell_value(1, 5) != 'depth'):
        print 'FATAL site: %s fert has bad header 4:%s 5:%s' % (
            siteid, worksheet.get_cell_value(1, 4),
            worksheet.get_cell_value(1, 5))
        continue

    for row in range(3, worksheet.rows+1):
        plotid = worksheet.get_cell_value(row, 4)
        if siteid.startswith('NAEW'):
            siteid = "NAEW.WS%s" % (worksheet.get_cell_value(row, 6),)
        year = worksheet.get_cell_value(row, 1)
        depth = worksheet.get_cell_value(row, 5)
        if plotid is None or depth is None or year is None:
            continue
        year = year.replace("?", "")
        for col in range(6, worksheet.cols+1):
            if worksheet.get_cell_value(1, col) is None:
                # print(("harvest_soil_bd %s(%s) row: %s col: %s is null"
                #       ) % (siteid, YEAR, row, col))
                continue
            varname = worksheet.get_cell_value(1, col).strip().split()[0]
            if varname[:4] != 'SOIL':
                # print 'Invalid varname: %s site: %s year: %s' % (
                #                    worksheet.get_cell_value(1,col).strip(),
                #                    siteid, YEAR)
                continue
            inval = worksheet.get_cell_value(row, col)
            val = util.cleanvalue(inval)
            try:
                pcursor.execute("""
                    INSERT into soil_data(site, plotid, varname, year,
                    depth, value)
                    values (%s, %s, %s, %s, %s, %s)
                    """, (siteid, plotid, varname, year, depth, val))
            except Exception, exp:
                print 'HARVEST_SOIL_FERTILITY TRACEBACK'
                print exp
                print '%s %s %s %s %s' % (siteid, plotid, varname, depth,
                                          val)
                sys.exit()
            key = "%s|%s|%s|%s|%s" % (siteid, plotid, varname, depth,
                                      year)
            if key in current:
                del(current[key])

for key in current:
    (siteid, plotid, varname, depth, year) = key.split("|")
    print(('harvest_soil_fert rm %s %s %s %s %s'
           ) % (year, siteid, plotid, varname, repr(depth)))
    pcursor.execute("""DELETE from soil_data where site = %s and
    plotid = %s and varname = %s and year = %s
    """, (siteid, plotid, varname, year))

pcursor.close()
pgconn.commit()
pgconn.close()
