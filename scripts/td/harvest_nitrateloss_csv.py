import pandas as pd
import psycopg2
import sys
pgconn = psycopg2.connect(database='td', port=5555, host='localhost',
                          user='mesonet')
cursor = pgconn.cursor()

df = pd.read_csv(sys.argv[1])
uniqueid = sys.argv[2]

cursor.execute("""
    DELETE from nitrateloss_data where uniqueid = %s and
    valid between %s and %s
""", (uniqueid, df['Date'].min(), df['Date'].max()))
deleted = cursor.rowcount
if deleted > 0:
    print("Removed %s" % (deleted,))

plotids = []
for col in df.columns:
    tokens = col.split()
    if len(tokens) == 2 and tokens[1].startswith('WAT'):
        if tokens[0] not in plotids:
            plotids.append(tokens[0])
inserts = 0
for plotid in plotids:
    for col in ['WAT2', 'WAT9', 'WAT20', 'WAT26']:
        col2 = "%s %s" % (plotid, col)
        if col2 not in df.columns:
            df[col2] = None
        else:
            df[col2] = pd.to_numeric(df[col2], errors='coerse')
    df2 = df[['Date', plotid+' WAT2', plotid+' WAT9',
              plotid + ' WAT20', plotid + ' WAT26']]
    for vals in df2.itertuples(index=False):
        if vals[0] == ' ' or vals[0] is None:
            continue
        cursor.execute("""
        INSERT into nitrateloss_data
        (uniqueid, plotid, valid, wat2, wat9, wat20, wat26)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (uniqueid, plotid, vals[0], vals[1], vals[2], vals[3],
              vals[4]))
        inserts += 1
print("Inserted %s, Deleted %s entries for %s" % (inserts, deleted,
                                                  uniqueid))
cursor.close()
pgconn.commit()
pgconn.close()
