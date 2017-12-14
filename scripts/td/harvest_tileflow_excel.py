"""Process the Excel files that are manually downloaded from Google"""
from __future__ import print_function
import sys

import pandas as pd
from pyiem.util import get_dbconn


def main():
    """Go Main Go"""
    pgconn = get_dbconn('td')
    cursor = pgconn.cursor()

    dfs = pd.read_excel(sys.argv[1], sheetname=None, skiprows=[1],
                        na_values=['Did not collect', 'Sensor malfunction'],
                        converters={'Date': str, 'Time': str})
    uniqueid = sys.argv[2]

    for sheetname in dfs.keys():
        if len(sheetname) != 4:
            print("Skipping sheet %s" % (sheetname, ))
            continue
        print("Processing sheet: %s" % (sheetname, ))
        df = dfs[sheetname]
        #df['Date'] = df['Date'].astype('str') + ' ' + df['Time'].astype('str')
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
    cursor.close()
    pgconn.commit()
    pgconn.close()


if __name__ == '__main__':
    main()
