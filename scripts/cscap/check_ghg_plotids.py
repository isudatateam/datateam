from pandas.io.sql import read_sql
import psycopg2
import pyiem.cscap_utils as utils
import pandas as pd

pgconn = psycopg2.connect(database='sustainablecorn')

plotdf = read_sql("""SELECT upper(uniqueid) as uniqueid,
    upper(plotid) as plotid from plotids""",
                  pgconn, index_col=None)

spr_client = utils.get_spreadsheet_client(utils.get_config())

X = {"2011": '1DSHcfeBNJArVowk0CG_YvzI0YQnHIZXhcxOjBi2fPM4',
     '2012': '1ax_N80tIBBKEnWDnrGxsK4KUa1ssokwMxQZNVHEWWM8',
     '2013': '1UY5JYKlBHDElwljnEC-1tF7CozplAfyGpbkbd0OFqsA',
     '2014': '12NqffqVMQ0M4PMT_CP5hYfmydC-vzXQ0lFHbKwwfqzg',
     '2015': '1FxKx0GDJxv_8fIjKe2xRJ58FGILLlUSXcb6EuSLQSrI'}

years = X.keys()
years.sort()
unknown = ['UniqueID_PlotID', '-_-']
rows = []
for year in years:
    spreadsheet = utils.Spreadsheet(
        spr_client, X.get(year))
    for worksheet in spreadsheet.worksheets:
        lf = spreadsheet.worksheets[worksheet].get_list_feed()
        for entry in lf.entry:
            d = entry.to_dict()
            if d.get('ghg1') is None or d.get('ghg3') is None:
                continue
            rows.append(d)
            df2 = plotdf[(plotdf['uniqueid'] == d['ghg1'].upper()) &
                         (plotdf['plotid'] == d['ghg3'].upper())]
            if len(df2.index) == 0:
                key = "%s_%s" % (d['ghg1'], d['ghg3'])
                if key in unknown:
                    continue
                unknown.append(key)
                print("%s Unknown uniqueid: |%s| plotid: |%s|" % (year,
                                                                  d['ghg1'],
                                                                  d['ghg3']))

df = pd.DataFrame(rows)
writer = pd.ExcelWriter('output.xlsx')
df.to_excel(writer, 'Sheet1')
writer.save()
