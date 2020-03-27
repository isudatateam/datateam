"""A direct copy of a Google Spreadsheet to a postgresql database"""
import psycopg2
import pyiem.cscap_utils as util
from pyiem.util import logger

LOG = logger()
config = util.get_config()
pgconn = psycopg2.connect(database="sustainablecorn", host=config["database"]["host"])
sheets = util.get_sheetsclient(config, "cscap")
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
    j = (
        sheets.spreadsheets()
        .get(spreadsheetId=spreadkey, includeGridData=True)
        .execute()
    )
    griddata = j["sheets"][0]["data"][0]
    header = []
    for row, rowdata in enumerate(griddata["rowData"]):
        data = []
        for col, celldata in enumerate(rowdata["values"]):
            if row == 0:
                header.append(celldata.get("formattedValue", "n/a"))
                continue
            data.append(celldata.get("formattedValue", "n/a"))
        if row == 0:
            # Create the table
            sql = f"CREATE TABLE {tablename} ("
            for col in header:
                sql += "%s varchar," % (cleankey(col),)
            sql = sql[:-1] + ")"
            cursor.execute(sql)
            cursor.execute(f"GRANT SELECT on {tablename} to nobody,apache")
            continue
        if data.count("n/a") == len(data):
            continue
        values = []
        cols = []
        for key, val in zip(header, data):
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
