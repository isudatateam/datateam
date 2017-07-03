import pandas as pd
import psycopg2
import sys
pgconn = psycopg2.connect(database='td')
cursor = pgconn.cursor()

dfs = pd.read_excel(sys.argv[1], sheetname=None, skiprows=[1],
                    na_values=['Did not collect', 'Sensor malfunction'])
uniqueid = sys.argv[2]

for sheetname in dfs.keys():
    if len(sheetname) != 4:
        print("Skipping sheet %s" % (sheetname, ))
        continue
    print("Processing sheet: %s" % (sheetname, ))
    df = dfs[sheetname]
    #for i, row in df.iterrows():
    #    print row
    #    _ = pd.to_datetime(row['Date'].strftime("%Y-%m-%d") +
    #                       ' ' + row['Time'].strftime("%H:%M:%S"))
    # print df
    # print df['Time'].astype(str)
    df['Date'] = pd.to_datetime(df['Date'], errors='coerse')
    df['Time'] = pd.to_datetime(df['Time'], errors='coerse')
    df['Date'] = df['Date'] + df['Time']
    df.columns = ['Date', 'Time', 'S1', 'S2', 'S3', 'S4', 'S5', 'S6']
    cursor.execute("""
        DELETE from tileflow_data where uniqueid = %s and
        valid between %s and %s
    """, (uniqueid, df['Date'].min(), df['Date'].max()))
    deleted = cursor.rowcount
    if deleted > 0:
        print("Removed %s" % (deleted,))

    inserts = 0
    for plotid in ['S1', 'S2', 'S3', 'S4', 'S5', 'S6']:
        ldf = df[['Date', plotid]]
        for idx, row in ldf.iterrows():
            if (row['Date'] == ' ' or row['Date'] is None or
                    pd.isnull(row[plotid])):
                continue
            print row
            cursor.execute("""
            INSERT into tileflow_data
            (uniqueid, plotid, valid, discharge_mm_qc, discharge_mm)
            VALUES (%s, %s, %s, %s, %s)
            """, (uniqueid, plotid, row['Date'],
                  row.get(plotid), row.get(plotid)
                  ))
            inserts += 1
        print("Inserted %s, Deleted %s entries for %s" % (inserts, deleted,
                                                          uniqueid))
cursor.close()
pgconn.commit()
pgconn.close()
