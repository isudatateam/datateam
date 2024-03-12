"""Water Quality ingest"""

import datetime
import sys
from zoneinfo import ZoneInfo

import psycopg2

CENTRAL_TIME = [
    "ISUAG",
    "GILMORE",
    "SERF",
    "CLAY_C",
    "CLAY_R",
    "MUDS2",
    "MUDS3_OLD",
    "MUDS4",
    "SERF_SD",
    "SERF_IA",
    "STORY",
    "UBWC",
]


def gio_process(filename):
    """This is a manually generated file by gio"""

    pgconn = psycopg2.connect(database="sustainablecorn")
    cursor = pgconn.cursor()
    sql = """
        INSERT into waterquality_data(uniqueid, plotid, valid,
        sample_type, varname, value) VALUES (%s, %s, %s, %s, %s, %s)
        """
    for i, line in enumerate(open(filename)):
        if i == 0:
            continue
        (
            uniqueid,
            plotid,
            date,
            localtime,
            sample_type,
            varname,
            value,
        ) = line.strip().split(",")
        if localtime == "":
            localtime = "00:00"
        ts = datetime.datetime.strptime(
            "%s %s" % (date, localtime), "%Y-%m-%d %H:%M"
        )
        offset = 6 if uniqueid in CENTRAL_TIME else 5
        ts = ts + datetime.timedelta(hours=offset)
        ts = ts.replace(tzinfo=ZoneInfo("UTC"))
        cursor.execute(
            sql,
            (
                uniqueid,
                plotid,
                ts,
                sample_type,
                varname,
                (float(value) if not value.startswith("<") else None),
            ),
        )
    cursor.close()
    pgconn.commit()
    pgconn.close()


def main(argv):
    """Go Main"""
    fn = argv[1]
    gio_process(fn)


if __name__ == "__main__":
    main(sys.argv)
