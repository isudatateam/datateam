#!/usr/bin/env python
"""This is our fancy pants download function"""
import sys
import cgi
import psycopg2
import pandas as pd
from pandas.io.sql import read_sql
pgconn = psycopg2.connect(database='sustainablecorn', host='iemdb',
                          user='nobody')


def conv(value, detectlimit):
    if value is None or value == '':
        return "M"
    if value.startswith("<"):
        if detectlimit == "1":
            return value
        floatval = float(value[1:])
        if detectlimit == "2":
            return floatval / 2.
        if detectlimit == "3":
            return floatval / 2 ** 0.5
        if detectlimit == "4":
            return "M"
    if value in ['n/a', 'did not collect']:
        return value
    try:
        return float(value)
    except:
        return value


def do_agronomic(writer, sites, agronomic, years, detectlimit):
    df = read_sql("""
    SELECT site, plotid, varname, year, value from agronomic_data
    WHERE site in %s and year in %s and varname in %s ORDER by site, year
    """, pgconn, params=(tuple(sites), tuple(years),
                         tuple(agronomic)), index_col=None)
    df['value'] = df['value'].apply(lambda x: conv(x, detectlimit))
    df = pd.pivot_table(df, index=('site', 'plotid', 'year'),
                        values='value', columns=('varname',),
                        aggfunc=lambda x: ' '.join(str(v) for v in x))
    df.reset_index(inplace=True)
    df.to_excel(writer, 'Agronomic', index=False)


def do_soil(writer, sites, soil, years, detectlimit):
    df = read_sql("""
    SELECT site, plotid, depth, subsample, varname, year, value
    from soil_data
    WHERE site in %s and year in %s and varname in %s ORDER by site, year
    """, pgconn, params=(tuple(sites), tuple(years),
                         tuple(soil)), index_col=None)
    df['value'] = df['value'].apply(lambda x: conv(x, detectlimit))
    df = pd.pivot_table(df, index=('site', 'plotid', 'depth', 'subsample',
                                   'year'),
                        values='value', columns=('varname',),
                        aggfunc=lambda x: ' '.join(str(v) for v in x))
    df.reset_index(inplace=True)
    df.to_excel(writer, 'Soil', index=False)


def do(form):
    sites = form.getlist('sites[]')
    treatments = form.getlist('treatments[]')
    agronomic = form.getlist('agronomic[]')
    soil = form.getlist('soil[]')
    years = form.getlist('year[]')
    detectlimit = form.getfirst('detectlimit', "1")

    writer = pd.ExcelWriter("/tmp/cscap.xlsx", engine='xlsxwriter')

    # Sheet one are operations
    opdf = read_sql("""
    SELECT * from operations where uniqueid in %s and cropyear in %s
    ORDER by valid ASC
    """, pgconn, params=(tuple(sites), tuple(years)))
    opdf.to_excel(writer, 'Operations', index=False)

    if len(agronomic) > 0:
        do_agronomic(writer, sites, agronomic, years, detectlimit)
    if len(soil) > 0:
        do_soil(writer, sites, soil, years, detectlimit)

    # Send to client
    writer.close()
    sys.stdout.write("Content-type: application/vnd.ms-excel\n")
    sys.stdout.write((
        "Content-Disposition: attachment;Filename=cscap.xlsx\n\n"))
    sys.stdout.write(open('/tmp/cscap.xlsx', 'rb').read())


def main():
    """Do Stuff"""
    form = cgi.FieldStorage()
    do(form)

if __name__ == '__main__':
    main()
