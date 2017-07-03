#!/usr/bin/env python
"""Generalized accepting of a data edit!

UPSTREAM is /cscap/edit.py, so edit there and copy to td :/
"""
import cgi
import os
import sys
import psycopg2
import datetime
import pytz
import json


def decagon_logic(uniqueid_in, plotid_in):
    """Hackery"""
    # Split the uniqueid by :: to get the plotid
    (uniqueid, plotid) = uniqueid_in.split("::")
    # Convert the plotid_in into database columns
    DEPTHS = ['-', '10 cm', '20 cm', '40 cm', '60 cm', '100 cm']
    if uniqueid in ['KELLOGG', 'MASON']:
        DEPTHS[1] = '-'
        DEPTHS[5] = '80 cm'
    elif uniqueid == 'NAEW':
        DEPTHS[1] = '5 cm'
        DEPTHS[2] = '10 cm'
        DEPTHS[3] = '20 cm'
        DEPTHS[4] = '30 cm'
        DEPTHS[5] = '50 cm'
    column = "d"
    for i, d in enumerate(DEPTHS):
        if plotid_in.startswith(d):
            column += str(i)
    if plotid_in.find("VSM") > -1:
        column += "moisture"
    else:
        column += "temp"
    return uniqueid, plotid, column


def main():
    """Do Something"""
    form = cgi.FieldStorage()
    remote_user = os.environ.get('REMOTE_USER', 'anonymous')
    sys.stdout.write("Content-type: application/json\n\n")

    # Figure out what we are editing
    table = form.getfirst('table')
    valid = datetime.datetime.strptime(form.getfirst('valid')[:19],
                                       '%Y-%m-%dT%H:%M:%S')
    valid = valid.replace(tzinfo=pytz.timezone("UTC"))
    column = form.getfirst('column')
    uniqueid = form.getfirst('uniqueid')
    plotid = form.getfirst('plotid')
    value = form.getfirst('value')
    comment = form.getfirst('comment')
    if value == 'null':
        value = None
    if table == "decagon_data":
        # We have to do some hackery straighten this out
        uniqueid, plotid, column = decagon_logic(uniqueid, plotid)

    dbname = ('sustainablecorn'
              if os.environ.get('DATATEAM_APP') == 'cscap'
              else 'td')
    pgconn = psycopg2.connect(database=dbname, host='iemdb', user='nobody')
    cursor = pgconn.cursor()

    cursor.execute("""UPDATE """+table+""" SET """+column+"""_qc = %s,
    """+column+"""_qcflag = 'w'
    WHERE uniqueid = %s and plotid = %s and valid = %s
    """, (value, uniqueid, plotid, valid))
    res = {}
    if cursor.rowcount == 1:
        res['status'] = 'OK'
    else:
        sys.stderr.write(repr(uniqueid) + repr(plotid) + repr(valid))
        res['status'] = 'ERROR: Failed to find database entry'

    cursor.execute("""INSERT into website_edits(
        username, edit_table, uniqueid, plotid, valid, edit_column, newvalue,
        comment)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (remote_user, table, uniqueid, plotid, valid, column, value,
              comment))
    cursor.close()
    pgconn.commit()
    sys.stdout.write(json.dumps(res))

if __name__ == '__main__':
    main()
    # print decagon_logic("ISUAG::302", "10 cm VSM")
