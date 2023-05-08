import datetime

import psycopg2
from pyiem.network import Table as NetworkTable

nt = NetworkTable("CSCAP")
COOP = psycopg2.connect(
    database="coop", host="localhost", port=5555, user="nobody"
)
ccursor = COOP.cursor()

SITES = [
    "MASON",
    "KELLOGG",
    "GILMORE",
    "ISUAG",
    "WOOSTER.COV",
    "SEPAC",
    "FREEMAN",
    "BRADFORD.C",
    "BRADFORD.B1",
    "BRADFORD.B2",
]

print("uniqueid,date,high[F],low[F],precip[in]")
for uniqueid in SITES:
    for year in range(2011, 2016):
        plantdate = datetime.date(year, 4, 15)
        terminatedate = datetime.date(year, 10, 15)
        clsite = nt.sts[uniqueid]["climate_site"]
        ccursor.execute(
            """SELECT day, high, low, precip from
        alldata_"""
            + clsite[:2]
            + """ WHERE station = %s and day >= %s
        and day <= %s ORDER by day ASC""",
            (clsite, plantdate, terminatedate),
        )
        for row in ccursor:
            print(
                "%s,%s,%s,%s,%s" % (uniqueid, row[0], row[1], row[2], row[3])
            )
