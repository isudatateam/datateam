#!/usr/bin/env python
"""This is our fancy pants download function"""
import sys
import cgi
import psycopg2
import pandas as pd
from pandas.io.sql import read_sql
pgconn = psycopg2.connect(database='sustainablecorn', host='iemdb',
                          user='nobody')


def do(form):
    sites = form.getlist('sites[]')
    treatments = form.getlist('treatments[]')
    agronomic = form.getlist('agronomic[]')
    soil = form.getlist('soil[]')
    year = form.getlist('year[]')

    # Sheet one are operations
    opdf = read_sql("""
    SELECT * from operations where uniqueid in %s and cropyear in %s
    ORDER by valid ASC
    """, pgconn, params=(tuple(sites), tuple(year)))

    sys.stdout.write("Content-type: application/vnd.ms-excel\n")
    sys.stdout.write((
        "Content-Disposition: attachment;Filename=cscap.xlsx\n\n"))
    writer = pd.ExcelWriter("/tmp/cscap.xlsx", engine='xlsxwriter')
    opdf.to_excel(writer, 'Operations', index=False)
    writer.close()
    sys.stdout.write(open('/tmp/cscap.xlsx', 'rb').read())


def main():
    """Do Stuff"""
    form = cgi.FieldStorage()
    do(form)

if __name__ == '__main__':
    main()
