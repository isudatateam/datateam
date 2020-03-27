"""A direct copy of a Google Spreadsheet to a postgresql database"""
import pyiem.cscap_utils as util
from pyiem.util import logger, get_dbconn
from gspread_pandas import Spread

LOG = logger()
config = util.get_config()
pgconn = get_dbconn("sustainablecorn")
JOB_LISTING = [
    ["15AjRh7dvwleWqJviz53JqQm8wxNru4bsgn16Ad_MtUU", "xref_rotation"],
    ["1tQvw-TQFtBI6xcsbZpaCHtYF7oKZEXNXy-CZHBr8Lh8", "metadata_master"],
]


def cleankey(val):
    """Remove bad chars from column name"""
    return val.replace("-", "_").replace(" ", "")


def do(spreadkey, tablename):
    """Process"""
    cursor = pgconn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {tablename}")
    spread = Spread(spreadkey, config=config["cscap"]["service_account"])
    df = spread.sheet_to_df(index=None)
    sql = f"CREATE TABLE {tablename} ("
    for col in df.columns:
        sql += "%s varchar," % (cleankey(col),)
    sql = sql[:-1] + ")"
    cursor.execute(sql)
    cursor.execute(f"GRANT SELECT on {tablename} to nobody,apache")
    for _, row in df.iterrows():
        cols = []
        values = []
        for key, val in row.items():
            cols.append(cleankey(key))
            values.append((val or "").strip())
        sql = "INSERT into %s (%s) VALUES (%s)" % (
            tablename,
            ",".join(cols),
            ",".join(["%s"] * len(cols)),
        )
        cursor.execute(sql, values)
    cursor.close()
    pgconn.commit()


def main():
    """Do Something"""
    for (spreadkey, tablename) in JOB_LISTING:
        do(spreadkey, tablename)


if __name__ == "__main__":
    main()
