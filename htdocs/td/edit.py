"""Generalized accepting of a data edit!

UPSTREAM is /td/edit.py, so edit there and copy to admin :/
"""
import os
import sys
import datetime
import json

import pytz
from paste.request import parse_formvars
from pyiem.util import get_dbconn


def decagon_logic(uniqueid_in, plotid_in):
    """Hackery"""
    # Split the uniqueid by :: to get the plotid
    (uniqueid, plotid) = uniqueid_in.split("::")
    # Convert the plotid_in into database columns
    DEPTHS = ["-", "10 cm", "20 cm", "40 cm", "60 cm", "100 cm"]
    if uniqueid in ["KELLOGG", "MASON"]:
        DEPTHS[1] = "-"
        DEPTHS[5] = "80 cm"
    elif uniqueid == "NAEW":
        DEPTHS[1] = "5 cm"
        DEPTHS[2] = "10 cm"
        DEPTHS[3] = "20 cm"
        DEPTHS[4] = "30 cm"
        DEPTHS[5] = "50 cm"
    column = "d"
    for i, d in enumerate(DEPTHS):
        if plotid_in.startswith(d):
            column += str(i)
    if plotid_in.find("VSM") > -1:
        column += "moisture"
    else:
        column += "temp"
    return uniqueid, plotid, column


def application(environ, start_response):
    """Do Something"""
    form = parse_formvars(environ)
    remote_user = environ.get("REMOTE_USER", "anonymous")
    start_response("200 OK", [("Content-type", "application/json")])

    # Figure out what we are editing
    table = form.get("table")
    valid = datetime.datetime.strptime(
        form.getfirst("valid")[:19], "%Y-%m-%dT%H:%M:%S"
    )
    valid = valid.replace(tzinfo=pytz.utc)
    column = form.get("column")
    uniqueid = form.get("uniqueid")
    plotid = form.get("plotid")
    value = form.get("value")
    comment = form.get("comment")
    if value == "null":
        value = None
    if table == "decagon_data":
        # We have to do some hackery straighten this out
        uniqueid, plotid, column = decagon_logic(uniqueid, plotid)

    dbname = (
        "sustainablecorn"
        if os.environ.get("DATATEAM_APP") == "cscap"
        else "td"
    )
    pgconn = get_dbconn(dbname)
    cursor = pgconn.cursor()

    cursor.execute(
        f"UPDATE {table} SET {column}_qc = %s, {column}_qcflag = 'w' "
        "WHERE uniqueid = %s and plotid = %s and valid = %s",
        (value, uniqueid, plotid, valid),
    )
    res = {}
    if cursor.rowcount == 1:
        res["status"] = "OK"
    else:
        sys.stderr.write(repr(uniqueid) + repr(plotid) + repr(valid))
        res["status"] = "ERROR: Failed to find database entry"

    cursor.execute(
        "INSERT into website_edits(username, edit_table, uniqueid, plotid, "
        "valid, edit_column, newvalue, comment) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (remote_user, table, uniqueid, plotid, valid, column, value, comment),
    )
    cursor.close()
    pgconn.commit()
    return [json.dumps(res).encode("utf-8")]
