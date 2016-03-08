from pandas.io.sql import read_sql
import psycopg2
import pyiem.cscap_utils as utils

pgconn = psycopg2.connect(database='sustainablecorn')

plotdf = read_sql("""SELECT uniqueid, plotid from plotids""",
                  pgconn, index_col=None)

spr_client = utils.get_spreadsheet_client(utils.get_config())

spreadsheet = utils.Spreadsheet(
    spr_client, '1WxB9pWvTB-_8cErQgXrybX7rklu5qXL8XyWtEf7yDVs')
for worksheet in spreadsheet.worksheets:
    lf = spreadsheet.worksheets[worksheet].get_list_feed()
    for entry in lf.entry:
        d = entry.to_dict()
        df2 = plotdf[(plotdf['uniqueid'] == d['site']) &
                     (plotdf['plotid'] == d['plotid'])]
        if len(df2.index) == 0:
            print("Missing uniqueid: |%s| plotid: |%s|" % (d['site'],
                                                           d['plotid']))
