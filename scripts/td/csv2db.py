"""Verbatim copy of a csv file to the database."""
import sys
import os
from io import StringIO

from pyiem.util import logger, get_dbconn
import pandas as pd
import numpy as np

LOG = logger()
COLTYPES = {"object": "text", "int64": "int", "float64": "real"}


def create_table(cursor, table_name, df):
    """Create the table."""
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    sio = StringIO()
    sio.write(f"CREATE TABLE {table_name} (")
    tokens = []
    for col in df.columns:
        tokens.append("%s %s" % (col, COLTYPES[str(df[col].dtype)]))
    sio.write(", ".join(tokens))
    sio.write(")")
    cursor.execute(sio.getvalue())
    cursor.execute(f"GRANT SELECT on {table_name} to nobody,apache")


def copy_rows(cursor, table_name, df):
    """Copy data."""
    ps = ", ".join(["%s"] * len(df.columns))
    for row in df.itertuples(index=False):
        cursor.execute(f"INSERT into {table_name} values({ps})", row)


def main(argv):
    """Go Main Go."""
    csvfn = argv[1]
    table_name = os.path.basename(csvfn)[:-4]
    LOG.info("dumping %s into table: %s", csvfn, table_name)
    df = pd.read_csv(csvfn)
    df = df.replace({np.nan: None})
    pgconn = get_dbconn("td")
    cursor = pgconn.cursor()
    create_table(cursor, table_name, df)
    copy_rows(cursor, table_name, df)
    cursor.close()
    pgconn.commit()


if __name__ == "__main__":
    main(sys.argv)
