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
