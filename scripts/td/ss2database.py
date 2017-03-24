"""A direct copy of a Google Spreadsheet to a postgresql database"""
import psycopg2
import pyiem.cscap_utils as util

config = util.get_config()
pgconn = psycopg2.connect(database='td',
                          host=config['database']['host'])
ss = util.get_ssclient(config)

JOB_LISTING = [
    ["4416033734846340", "td_data_dictionary"],
    ]


def cleaner(val):
    val = val.lower().replace(" ", "_").replace("(", "").replace(")", "")
    val = val.replace("/", " ")
    return val


def do(sheetid, tablename):
    """Process"""
    cursor = pgconn.cursor()
    cursor.execute("""DROP TABLE IF EXISTS %s""" % (tablename, ))
    sheet = ss.Reports.get_report(sheetid, page_size=1000)
    cols = []
    for col in sheet.columns:
        cols.append(cleaner(col.title))
    cursor.execute(("""
        CREATE TABLE """ + tablename + """ (%s)
    """) % (",".join([' "%s" varchar' % (s,) for s in cols]), ))
    cursor.execute("""
        GRANT SELECT on """ + tablename + """ to nobody,apache
    """)
    for row in sheet.rows:
        vals = []
        for cell in row.cells:
            vals.append(cell.value)
        sql = """
        INSERT into %s (%s) VALUES (%s)
        """ % (tablename, ",".join(['"%s"' % (s,) for s in cols]),
               ",".join(["%s"]*len(cols)))
        cursor.execute(sql, vals)
    cursor.close()
    pgconn.commit()


def main():
    """Do Something"""
    for (sheetid, tablename) in JOB_LISTING:
        do(sheetid, tablename)


if __name__ == '__main__':
    main()
