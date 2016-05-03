from pandas.io.sql import read_sql
import psycopg2
from pyiem.network import Table as NetworkTable
nt = NetworkTable("CSCAP")
COOP = psycopg2.connect(database='coop', host='iemdb', user='nobody')
ccursor = COOP.cursor()

SUSTAIN = psycopg2.connect(database='sustainablecorn', host='iemdb',
                           user='nobody')
scursor = SUSTAIN.cursor()

SITES = ['MASON', 'KELLOGG', 'GILMORE', 'ISUAG', 'WOOSTER.COV',
         'SEPAC', 'FREEMAN', 'BRADFORD.C',
         'BRADFORD.B1', 'BRADFORD.B2']

# daily low and high temperatures and daily precipitations
# periods from the day rye was terminated to the day corn was planted
tdf = read_sql("""SELECT uniqueid, valid, cropyear, operation from operations
    where uniqueid in %s and operation = 'termination_rye_corn'
    """, SUSTAIN, params=(tuple(SITES),), index_col=None)
pdf = read_sql("""SELECT uniqueid, valid, cropyear from operations where
    uniqueid in %s and operation = 'plant_corn'
    """, SUSTAIN, params=(tuple(SITES),), index_col=None)

print("uniqueid,date,high[F],low[F],precip[in]")
for _, row in pdf.iterrows():
    uniqueid = row['uniqueid']
    year = row['cropyear']
    _df = tdf[(tdf['cropyear'] == year) & (tdf['uniqueid'] == uniqueid)]
    if len(_df.index) == 0:
        continue
    plantdate = row['valid']
    terminatedate = _df.iloc[0, 1]
    clsite = nt.sts[uniqueid]['climate_site']
    ccursor.execute("""SELECT day, high, low, precip from
    alldata_""" + clsite[:2] + """ WHERE station = %s and day >= %s
    and day <= %s ORDER by day ASC""", (clsite, terminatedate, plantdate))
    for row in ccursor:
        print("%s,%s,%s,%s,%s" % (uniqueid, row[0], row[1], row[2], row[3]))
