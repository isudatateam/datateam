from pandas.io.sql import read_sql
import psycopg2
import pyiem.cscap_utils as utils
import pandas as pd

pgconn = psycopg2.connect(database='sustainablecorn')

plotdf = read_sql("""
    SELECT upper(uniqueid) as uniqueid, plotid from plotids
    """, pgconn, index_col=None)

drive = utils.get_driveclient(utils.get_config(), 'cscap')

spr_client = utils.get_spreadsheet_client(utils.get_config())

res = drive.files().list(
    q=("'0B4fyEPcRW7IscDcweEwxUFV3YkU' in parents and "
       "title contains 'USB'")).execute()

U = {}

for item in res['items']:
    if item['mimeType'] != 'application/vnd.google-apps.spreadsheet':
        continue
    uniqueid = item['title'].split("-")[1].strip()
    spreadsheet = utils.Spreadsheet(spr_client, item['id'])
    rows = []
    for worksheet in spreadsheet.worksheets:
        lf = spreadsheet.worksheets[worksheet].get_list_feed()
        for entry in lf.entry:
            d = entry.to_dict()
            rows.append(dict(uniqueid=uniqueid, plotid=d['plotid']))

    plotipm = pd.DataFrame(rows)
    for i, row in plotipm.iterrows():
        df2 = plotdf[(plotdf['uniqueid'] == row['uniqueid']) &
                     (plotdf['plotid'] == row['plotid'])]
        if len(df2.index) == 0:
            key = "%s_%s" % (row['uniqueid'], row['plotid'])
            if key in U:
                continue
            U[key] = True
            print("Missing uniqueid: |%s| plotid: |%s|" % (row['uniqueid'],
                                                           row['plotid']))
