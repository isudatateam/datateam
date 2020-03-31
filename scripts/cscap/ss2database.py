"""A direct copy of a Google Spreadsheet to a postgresql database"""
import psycopg2
import pyiem.cscap_utils as util
from unidecode import unidecode
from six import string_types

config = util.get_config()
pgconn = psycopg2.connect(
    database="sustainablecorn", host=config["database"]["host"]
)
ss = util.get_ssclient(config)

JOB_LISTING = [
    ["3879498232948612", "cscap_data_dictionary"],
    ["1292573529663364", "refereed_journals"],
    ["6868926064813956", "theses"],
    ["3644715322107780", "data_dictionary_export"],
    ["6669830439888772", "highvalue_notes"],
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
    sheet = ss.Reports.get_report(sheetid, page_size=1000)
    cols = []
    for col in sheet.columns:
        cols.append(cleaner(col.title))
    cursor.execute(
        (
            """
        CREATE TABLE """
            + tablename
            + """ (ss_order int, %s)
    """
        )
        % (",".join([' "%s" varchar' % (s,) for s in cols]),)
    )
    cursor.execute(
        """
        GRANT SELECT on """
        + tablename
        + """ to nobody,apache
    """
    )
    for i, row in enumerate(sheet.rows):
        vals = []
        for cell in row.cells:
            val = cell.value
            if isinstance(val, string_types):
                val = unidecode(val)
            vals.append(val)
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
    for (sheetid, tablename) in JOB_LISTING:
        workflow(sheetid, tablename)


if __name__ == "__main__":
    main()
