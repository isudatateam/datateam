"""A direct copy of a Google Spreadsheet to a postgresql database"""

import psycopg2
from six import string_types
from unidecode import unidecode

import isudatateam.cscap_utils as util

config = util.get_config()
pgconn = psycopg2.connect(
    database="sustainablecorn", host=config["database"]["host"]
)
ss = util.get_ssclient(config)

JOB_LISTING = [
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
    cursor.execute(f"GRANT SELECT on {tablename} to nobody")
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
    for sheetid, tablename in JOB_LISTING:
        workflow(sheetid, tablename)


if __name__ == "__main__":
    main()
