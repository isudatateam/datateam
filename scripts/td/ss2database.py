"""A direct copy of a Google Sheet to a postgresql database"""
import psycopg2
import isudatateam.cscap_utils as util
from gspread_pandas import Spread

config = util.get_config()
pgconn = psycopg2.connect(database="td", host=config["database"]["host"])
ss = util.get_ssclient(config)

JOB_LISTING = [
    ["1hoV4AGXBNBB-Hq51QAEx6gGQhvznDjTcY1T4ktHRS2w", "refereed_journals"],
    ["1-IErqpAru516izW0BPHRsY4yQzz3szhwM4l60RdTGTc", "theses"],
]


def cleaner(val):
    """Clean this value"""
    val = val.lower().replace(" ", "_").replace("(", "").replace(")", "")
    val = val.replace("/", " ")
    return val


def workflow(sheetid, tablename):
    """Process"""
    cursor = pgconn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {tablename}")
    spread = Spread(sheetid, config=config["td"]["service_account"])
    df = spread.sheet_to_df(index=None)
    cols = [cleaner(c) for c in df.columns]
    cursor.execute(
        f"CREATE TABLE {tablename} (ss_order int, %s)"
        % (",".join([' "%s" varchar' % (s,) for s in cols]),)
    )
    cursor.execute(f"GRANT SELECT on {tablename} to nobody")
    for i, row in enumerate(df.itertuples()):
        vals = []
        for col in row[1:]:
            vals.append(col)
        sql = """
        INSERT into %s (ss_order, %s) VALUES (%s, %s)
        """ % (
            tablename,
            ",".join(['"%s"' % (s,) for s in cols]),
            i,
            ",".join(["%s"] * len(cols)),
        )
        cursor.execute(sql, vals)
    cursor.close()
    pgconn.commit()


def main():
    """Do Something"""
    for sheetid, tablename in JOB_LISTING:
        workflow(sheetid, tablename)


if __name__ == "__main__":
    main()
