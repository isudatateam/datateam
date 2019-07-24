import pyiem.cscap_utils as util
import pandas as pd
import psycopg2

config = util.get_config()

pgconn = psycopg2.connect(database='td',
                          host=config['database']['host'])

# Get me a client, stat
spr_client = util.get_spreadsheet_client(config)
drive_client = util.get_driveclient(config, "td")

res = drive_client.files().list(
    q="title contains 'Tile Nitrate-N Load'").execute()

for item in res['items']:
    if item['mimeType'] != 'application/vnd.google-apps.spreadsheet':
        continue
    siteid = item['title'].split()[0]
    pcursor = pgconn.cursor()
    pcursor.execute("""SELECT count(*) from nitrateload_data
    WHERE uniqueid = %s""", (siteid,))
    if pcursor.fetchone()[0] > 0:
        print("Skipping %s" % (siteid,))
        continue
    print("Processing %s" % (siteid,))
    spreadsheet = util.Spreadsheet(spr_client, item['id'])
    spreadsheet.get_worksheets()
    pcursor.execute("""
        DELETE from nitrateload_data where uniqueid = %s
    """, (siteid,))
    deleted = pcursor.rowcount
    inserts = 0
    for year in spreadsheet.worksheets.keys():

        worksheet = spreadsheet.worksheets[year]
        worksheet.get_cell_feed()
        newvals = 0

        cols = []
        plotids = []
        for col in range(1, worksheet.cols+1):
            thiscol = worksheet.get_cell_value(1, col)
            if thiscol is None:
                print("Null Column?")
                cols.append('bogus')
            elif thiscol == 'Date':
                cols.append('date')
            else:
                tokens = thiscol.strip().split()
                if len(tokens) < 3:
                    print("Invalid column header '%s'" % (thiscol,))
                    cols.append('bogus')
                    continue
                plotid = tokens[0]
                varname = tokens[1]
                if not varname.startswith('WAT'):
                    print("Invalid column header '%s'" % (thiscol,))
                    cols.append('bogus')
                    continue
                cols.append("%s %s" % (plotid, varname.replace('WAT0', 'WAT')))
                if plotid not in plotids:
                    plotids.append(plotid)
        print year, cols, plotids
        # start on row 3
        rows = []
        for row in range(3, worksheet.rows+1):
            thisrow = []
            for col in range(1, worksheet.cols+1):
                thisrow.append(worksheet.get_cell_value(row, col))
            rows.append(thisrow)

        df = pd.DataFrame(rows, columns=cols)
        for plotid in plotids:
            for col in ['WAT2', 'WAT9', 'WAT20', 'WAT26']:
                col2 = "%s %s" % (plotid, col)
                if col2 not in df.columns:
                    df[col2] = None
                else:
                    df[col2] = pd.to_numeric(df[col2], errors='coerce')
            df2 = df[['date', plotid+' WAT2', plotid+' WAT9',
                      plotid + ' WAT20', plotid + ' WAT26']]
            for vals in df2.itertuples(index=False):
                if vals[0] == ' ' or vals[0] is None:
                    continue
                pcursor.execute("""
                INSERT into nitrateload_data
                (uniqueid, plotid, valid, wat2, wat9, wat20, wat26)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (siteid, plotid, vals[0], vals[1], vals[2], vals[3],
                      vals[4]))
                inserts += 1
    print("Inserted %s, Deleted %s entries for %s" % (inserts, deleted,
                                                      siteid))
    pcursor.close()
    pgconn.commit()
pgconn.close()
