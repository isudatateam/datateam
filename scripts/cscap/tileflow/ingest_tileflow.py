"""Tileflow ingest"""
from __future__ import print_function
import sys
import datetime

import pytz
import pandas as pd
import psycopg2
import numpy as np
from pyiem.cscap_utils import get_config, get_spreadsheet_client, Spreadsheet

CENTRAL_TIME = ['ISUAG', 'GILMORE', 'SERF', 'CLAY_C', 'CLAY_R',
                'MUDS2', 'MUDS3_OLD', 'MUDS4', 'SERF_SD', 'SERF_IA',
                'STORY', 'UBWC']


def gio_process(filename):
    """This is a manually generated file by gio"""

    pgconn = psycopg2.connect(database='sustainablecorn')
    cursor = pgconn.cursor()
    sql = """
        INSERT into tileflow_data(uniqueid, plotid, valid,
        discharge_mm, discharge_mm_qc) VALUES (%s, %s, %s, %s, %s)
        """
    for i, line in enumerate(open(filename)):
        if i == 0:
            continue
        (uniqueid, plotid, date, localtime, flow) = line.strip().split(",")
        if localtime == '':
            localtime = "00:00"
        if flow == '':
            flow = None
        ts = datetime.datetime.strptime("%s %s" % (date, localtime),
                                        '%Y-%m-%d %H:%M')
        offset = 6 if uniqueid in CENTRAL_TIME else 5
        ts = ts + datetime.timedelta(hours=offset)
        ts = ts.replace(tzinfo=pytz.utc)
        cursor.execute(sql, (uniqueid, plotid, ts, flow, flow))
    cursor.close()
    pgconn.commit()
    pgconn.close()


def main(argv):
    """Go Main"""
    fn = argv[1]
    gio_process(fn)


if __name__ == '__main__':
    main(sys.argv)
