import pandas as pd
import psycopg2
import sys
pgconn = psycopg2.connect(database='td')
cursor = pgconn.cursor()

df = pd.read_csv(sys.argv[1])
uniqueid = sys.argv[2]

cursor.execute("""
    DELETE from nitrateload_data where uniqueid = %s and
    valid between %s and %s
""", (uniqueid, df['Date'].min(), df['Date'].max()))
deleted = cursor.rowcount
if deleted > 0:
    print("Removed %s" % (deleted,))

inserts = 0
for idx, row in df.iterrows():
    if row['Date'] == ' ' or row['Date'] is None:
        continue
    cursor.execute("""
    INSERT into nitrateload_data
    (uniqueid, plotid, valid, wat2, wat9, wat20, wat26)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (uniqueid, row['id'], row['Date'], row.get('WAT2'), row.get('WAT9'),
          row.get('WAT20'), row.get('WAT26')))
    inserts += 1
print("Inserted %s, Deleted %s entries for %s" % (inserts, deleted,
                                                  uniqueid))
cursor.close()
pgconn.commit()
pgconn.close()
