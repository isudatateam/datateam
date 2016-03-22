from pandas.io.sql import read_sql
import psycopg2
import pyiem.cscap_utils as utils
import pandas as pd

pgconn = psycopg2.connect(database='sustainablecorn')

plotdf = read_sql("""SELECT upper(uniqueid) as uniqueid, plotid from plotids""",
                  pgconn, index_col=None)

spr_client = utils.get_spreadsheet_client(utils.get_config())

spreadsheet = utils.Spreadsheet(
        spr_client, '1WxB9pWvTB-_8cErQgXrybX7rklu5qXL8XyWtEf7yDVs')
rows = []
for worksheet in spreadsheet.worksheets:
    lf = spreadsheet.worksheets[worksheet].get_list_feed()
    for entry in lf.entry:
        d = entry.to_dict()
        rows.append(dict(uniqueid=d['site'], plotid=d['plotid']))

plotipm = pd.DataFrame(rows)
for i, row in plotipm.iterrows():
    df2 = plotdf[(plotdf['uniqueid'] == row['uniqueid']) &
                 (plotdf['plotid'] == row['plotid'])]
    if len(df2.index) == 0:
        print("Missing uniqueid: |%s| plotid: |%s|" % (row['uniqueid'],
                                                       row['plotid']))
