"""Process a file provided by Gio with the GHG data"""
from __future__ import print_function
import sys
import StringIO

import pandas as pd
import psycopg2


def main(argv):
    """Process a given filename"""
    filename = argv[1]
    df = pd.read_csv(filename)
    pgconn = psycopg2.connect(database='sustainablecorn')
    cursor = pgconn.cursor()
    cursor.execute("""DELETE from ghg_data""")
    print("Deleted %s rows" % (cursor.rowcount, ))
    data = StringIO.StringIO()
    for row in df.itertuples(index=False):
        data.write(("\t".join([str(s) for s in row]) + "\n"
                    ).replace("nan", "\N"))
    data.seek(0)
    cursor.copy_from(data, "ghg_data", columns=df.columns.tolist())
    print("Added %s rows" % (cursor.rowcount, ))
    cursor.close()
    pgconn.commit()


if __name__ == '__main__':
    main(sys.argv)
