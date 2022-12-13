"""A direct copy of a Google Spreadsheet to a postgresql database"""
import psycopg2
import pyiem.cscap_utils as util

config = util.get_config()
pgconn = psycopg2.connect(database="td", host=config["database"]["host"])
spr_client = util.get_spreadsheet_client(config)

# Listing of keys, sheet label, postgres table, columns to grab
JOB_LISTING = [
    [
        "1wM8123Ag5U1Hl7Y8KMRaT6SV0J8Up6TbG_AiSKY-UbQ",
        "new Well IDs",
        "wellids",
        [
            "siteid",
            "wellid",
            "plotid",
            "y1996",
            "y1997",
            "y1998",
            "y1999",
            "y2000",
            "y2001",
            "y2002",
            "y2003",
            "y2004",
            "y2005",
            "y2006",
            "y2007",
            "y2008",
            "y2009",
            "y2010",
            "y2011",
            "y2012",
            "y2013",
            "y2014",
            "y2015",
            "y2016",
            "y2017",
            "y2018",
        ],
    ],
    [
        "1wM8123Ag5U1Hl7Y8KMRaT6SV0J8Up6TbG_AiSKY-UbQ",
        "Plot IDs",
        "plotids",
        [
            "siteid",
            "plotid",
            "y1996",
            "y1997",
            "y1998",
            "y1999",
            "y2000",
            "y2001",
            "y2002",
            "y2003",
            "y2004",
            "y2005",
            "y2006",
            "y2007",
            "y2008",
            "y2009",
            "y2010",
            "y2011",
            "y2012",
            "y2013",
            "y2014",
            "y2015",
            "y2016",
            "y2017",
            "y2018",
        ],
    ],
]


def cleankey(key):
    return key.replace("-", "_")


def do(spreadkey, sheetlabel, tablename, cols):
    """Process"""
    cursor = pgconn.cursor()
    cursor.execute("""DROP TABLE IF EXISTS %s""" % (tablename,))
    spread = util.Spreadsheet(spr_client, spreadkey)
    listfeed = spr_client.get_list_feed(
        spreadkey, spread.worksheets[sheetlabel].id
    )
    for i, entry in enumerate(listfeed.entry):
        row = entry.to_dict()
        if i == 0:
            # Create the table
            sql = "CREATE TABLE %s (" % (tablename,)
            for key in cols:
                sql += "%s varchar," % (cleankey(key),)
            sql = sql[:-1] + ")"
            cursor.execute(sql)
            cursor.execute(f"GRANT SELECT on {tablename} to nobody")
        values = []
        for key in cols:
            val = row[cleankey(key)]
            if val is None:
                val = "Unknown"
            values.append(val.strip())
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
    for (spreadkey, sheetlabel, tablename, cols) in JOB_LISTING:
        do(spreadkey, sheetlabel, tablename, cols)


if __name__ == "__main__":
    main()
