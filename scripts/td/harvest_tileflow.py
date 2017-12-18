"""Process the Excel files that are manually downloaded from Google"""
from __future__ import print_function
import sys

import pandas as pd
from pyiem.util import get_dbconn


def do_csv(cursor, filename, uniqueid):
    """Custom"""
    df = pd.read_csv(filename, na_values=['NA', ])
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    for location in df['location'].unique():
        df2 = df[df['location'] == location]
        cursor.execute("""
            DELETE from tileflow_data where uniqueid = %s and
            valid between %s and %s and plotid = %s
        """, (uniqueid, df2['timestamp'].min(), df2['timestamp'].max(),
              location))
        deleted = cursor.rowcount
        inserts = 0
        for _idx, row in df2.iterrows():
            cursor.execute("""
            INSERT into tileflow_data
            (uniqueid, plotid, valid, discharge_mm_qc, discharge_mm)
            VALUES (%s, %s, %s, %s, %s)
            """, (uniqueid, location, row['timestamp'],
                  row['flow'], row['flow']))
            inserts += 1
        print(("%s Inserted %s, Deleted %s entries for %s"
               ) % (location, inserts, deleted, uniqueid))


def main(argv):
    """Go Main Go"""
    pgconn = get_dbconn('td')
    cursor = pgconn.cursor()
    filename = argv[1]
    uniqueid = sys.argv[2]
    if filename[-4:] == '.xlsx':
        dfs = pd.read_excel(filename, sheetname=None, skiprows=[1],
                            na_values=['Did not collect',
                                       'Sensor malfunction'],
                            converters={'Date': str, 'Time': str})
        for sheetname in dfs.keys():
            if len(sheetname) != 4:
                print("Skipping sheet %s" % (sheetname, ))
                continue
            print("Processing sheet: %s" % (sheetname, ))
            df = dfs[sheetname]
            df['Date'] = pd.to_datetime(df['Date'])
            df.columns = ['Date', 'Time', 'West', 'East']

            for plotid in ['West', 'East']:
                ldf = df[['Date', plotid]]
                cursor.execute("""
                    DELETE from tileflow_data where uniqueid = %s and
                    valid between %s and %s and plotid = %s
                """, (uniqueid, df['Date'].min(), df['Date'].max(), plotid))
                deleted = cursor.rowcount
                inserts = 0
                for _idx, row in ldf.iterrows():
                    if (row['Date'] == ' ' or row['Date'] is None or
                            pd.isnull(row[plotid])):
                        continue
                    cursor.execute("""
                    INSERT into tileflow_data
                    (uniqueid, plotid, valid, discharge_mm_qc, discharge_mm)
                    VALUES (%s, %s, %s, %s, %s)
                    """, (uniqueid, plotid, row['Date'],
                          row.get(plotid), row.get(plotid)
                          ))
                    inserts += 1
                print(("%s Inserted %s, Deleted %s entries for %s"
                       ) % (plotid, inserts, deleted, uniqueid))
    else:
        do_csv(cursor, filename, uniqueid)

    cursor.close()
    pgconn.commit()
    pgconn.close()


if __name__ == '__main__':
    main(sys.argv)
