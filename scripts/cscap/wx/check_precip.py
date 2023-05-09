"""A crude check on CSCAP Precip Totals"""
import psycopg2
from pandas.io.sql import read_sql
from pyiem.network import Table as NetworkTable

nt = NetworkTable("CSCAP")

pgconn = psycopg2.connect(database="coop", host="iemdb", user="nobody")

done = []
for sid in nt.sts.keys():
    clid = nt.sts[sid]["climate_site"]
    if clid in done:
        continue
    done.append(clid)
    print("------------------------- %s %s" % (sid, clid))
    df = read_sql(
        """
    SELECT year, sum(precip) from alldata_"""
        + clid[:2]
        + """
    WHERE station = %s
    GROUP by year""",
        pgconn,
        params=(clid,),
        index_col=None,
    )
    for i, row in df[df["sum"] < 10].iterrows():
        print(sid, clid, row["year"], row["sum"])
