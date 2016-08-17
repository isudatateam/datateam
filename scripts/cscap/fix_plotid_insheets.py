"""Go through our data sheets and cleanup entries that don't exactly match
things that we would like to see"""
import pyiem.cscap_utils as util
import psycopg2
from pandas.io.sql import read_sql

pgconn = psycopg2.connect(database='sustainablecorn')
df = read_sql("""SELECT * from plotids""", pgconn, index_col=None)
df['key'] = df['uniqueid'] + "::" + df['plotid']
df.set_index('key', inplace=True)

config = util.get_config()
spr_client = util.get_spreadsheet_client(config)
drive = util.get_driveclient(config)

# Fake last conditional to make it easy to reprocess one site...
res = drive.files().list(q=("title contains 'Agronomic Data'"),
                         maxResults=999).execute()

HEADERS = ['uniqueid', 'plotid', 'depth', 'tillage', 'rotation', 'soil6',
           'nitrogen', 'drainage', 'rep', 'subsample', 'landscape',
           'notes', 'herbicide', 'sampledate']

LKP = {'TIL1': '[1] No Tillage (NT)',
       'TIL2': '[2] Conventional Tillage (CT)',
       'TIL3': '[3] Natural Vegetation (NV)'}
LKP = {'DWM1': '[1] No drainage',
       'DWM2': '[2] Conventional drainage ("free")',
       'DWM3': '[3] Controlled drainage ("managed")',
       'DWM4': '[4] Shallow drainage',
       'DWM5': '[5] Drainage with subirrigation',
       'DWM6': '[6] Lysimeters '}
LKP = {'NIT1': '[1] No additional nitrogen fertilizer applied',
       'NIT2': '[2] MRTN application of N fertilizer in spring',
       'NIT3': '[3] Sensor based N application (mid-season) - MO/OH rec',
       'NIT4': '[4] Sensor based N application (mid-season) - OK rec'}
LKP = {'LND1': '[1] Near-summit',
       'LND2': '[2] Side slope',
       'LND3': '[3] Toe slope'}
col = 'landscape'

sz = len(res['items'])
for i, item in enumerate(res['items']):
    if item['mimeType'] != 'application/vnd.google-apps.spreadsheet':
        continue
    spreadsheet = util.Spreadsheet(spr_client, item['id'])
    spreadsheet.get_worksheets()
    for year in spreadsheet.worksheets:
        uniqueid = item['title'].split()[0]
        print('%3i/%3i sheet "%s" for "%s"' % (i + 1, sz, year, item['title']))
        lf = spreadsheet.worksheets[year].get_list_feed()
        for rownum, entry in enumerate(lf.entry):
            dirty = False
            data = entry.to_dict()
            if data['plotid'] is None or col not in data:
                continue
            key = "%s::%s" % (uniqueid, data['plotid'])
            old = data[col]
            new = LKP.get(df.at[key, col], df.at[key, col])
            if old is None and new is None:
                continue
            if new is None:
                new = ''
            if old is None or old[:3] != new[:3]:
                print("col: %s old: %s new: %s" % (col, old, new))
                entry.set_value(col, new)
                util.exponential_backoff(spr_client.update, entry)
