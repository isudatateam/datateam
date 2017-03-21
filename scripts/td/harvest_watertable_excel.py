import pandas as pd
import psycopg2
import sys
import datetime
pgconn = psycopg2.connect(database='td')
cursor = pgconn.cursor()

dfs = pd.read_excel(sys.argv[1], skiprows=[1], sheetname=None)
uniqueid = sys.argv[2]

for year in dfs.keys():
    #if len(year) == 4:
    #    print("skipping sheet: %s" % (year,))
    #    continue
    print("Processing sheet: %s" % (year,))
    df = dfs[year]
    if len(df.index) == 0:
        print("Empty Sheet, skipping")
        continue
    df.columns = ['Date', 'b', '1', 'b', '2', 'b', '3', 'b', '4', 'b', '5', 'b', '6', 'b', '7', 'b', '8', 'b',
                  '9', 'b', '10', 'b', '11', 'b', '12', 'b', '13', 'b', '14', 'b', '15', 'b', '16']
    # df['Date'] = df['Date'] - datetime.timedelta(hours=1)
    # df['Date'] = pd.to_datetime(df['Date'].dt.strftime("%Y-%m-%d") + ' ' + df['Time'].astype(str))
    cursor.execute("""
        DELETE from watertable_data where uniqueid = %s and
        valid between %s and %s
    """, (uniqueid, df['Date'].min(), df['Date'].max()))
    deleted = cursor.rowcount
    if deleted > 0:
        print("Removed %s" % (deleted,))
    inserts = 0
    for plotid in ['1', '2', '3', '4', '5', '6', '7', '8',
                   '9', '10', '11', '12', '13', '14', '15', '16']:
        for idx, row in df.iterrows():
            if (row['Date'] == ' ' or row['Date'] is None or
                    row.get(plotid) in ['dnc', 'nd']):
                continue
            try:
                val = row.get(plotid) * 10.
            except:
                continue
            cursor.execute("""
            INSERT into watertable_data
            (uniqueid, plotid, valid, depth_mm_qc, depth_mm)
            VALUES (%s, %s, %s, %s, %s)
            """, (uniqueid, plotid, row['Date'],
                  val, val
                  ))
            inserts += 1
    print("Inserted %s, Deleted %s entries for %s" % (inserts, deleted,
                                                      uniqueid))
cursor.close()
pgconn.commit()
pgconn.close()
