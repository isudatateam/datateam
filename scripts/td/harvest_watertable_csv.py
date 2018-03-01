from __future__ import print_function
import pandas as pd
import sys
from pyiem.util import get_dbconn


def main():
    """Ingest things from Gio"""
    pgconn = get_dbconn('td')
    cursor = pgconn.cursor()

    df = pd.read_csv(sys.argv[1])
    uniqueid = sys.argv[2]

    cursor.execute("""
        DELETE from watertable_data where uniqueid = %s and
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
        INSERT into watertable_data
        (uniqueid, plotid, valid, depth_mm_qc, depth_mm)
        VALUES (%s, %s, %s, %s, %s)
        """, (uniqueid, row['plot'], row['Date'], row.get('WAT4'), row.get('WAT4')
            ))
        inserts += 1
    print("Inserted %s, Deleted %s entries for %s" % (inserts, deleted,
                                                    uniqueid))
    cursor.close()
    pgconn.commit()
    pgconn.close()


if __name__ == '__main__':
    main()
