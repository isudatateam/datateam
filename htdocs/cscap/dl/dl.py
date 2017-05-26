#!/usr/bin/env python
"""This is our fancy pants download function"""
import sys
import cgi

import psycopg2
import pandas as pd
from pandas.io.sql import read_sql

PGCONN = psycopg2.connect(database='sustainablecorn', host='iemdb',
                          user='nobody')
FERTELEM = ['nitrogen', 'phosphorus', 'phosphate', 'potassium',
            'potash', 'sulfur', 'calcium', 'magnesium', 'zinc', 'iron']
KGH_LBA = 1.12085


def conv(value, detectlimit):
    """Convert a value into something that gets returned"""
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
    except Exception as _:
        return value


def do_dictionary(writer):
    """Add Data Dictionary to the spreadsheet"""
    df = read_sql("""
    SELECT * from cscap_data_dictionary ORDER by code_column_heading
    """, PGCONN, index_col=None)
    for col in df.columns:
        df[col] = df[col].str.decode('ascii', 'ignore')
    df.to_excel(writer, 'Data Dictionary', index=False)


def do_agronomic(writer, sites, agronomic, years, detectlimit):
    df = read_sql("""
    SELECT site, plotid, varname, year, value from agronomic_data
    WHERE site in %s and year in %s and varname in %s ORDER by site, year
    """, PGCONN, params=(tuple(sites), tuple(years),
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
    """, PGCONN, params=(tuple(sites), tuple(years),
                         tuple(soil)), index_col=None)
    df['value'] = df['value'].apply(lambda x: conv(x, detectlimit))
    df = pd.pivot_table(df, index=('site', 'plotid', 'depth', 'subsample',
                                   'year'),
                        values='value', columns=('varname',),
                        aggfunc=lambda x: ' '.join(str(v) for v in x))
    df.reset_index(inplace=True)
    df.to_excel(writer, 'Soil', index=False)


def get_operations(sites, years):
    """Return a DataFrame for the operations"""
    opdf = read_sql("""
    SELECT * from operations where uniqueid in %s and cropyear in %s
    ORDER by valid ASC
    """, PGCONN, params=(tuple(sites), tuple(years)))
    opdf['productrate'] = pd.to_numeric(opdf['productrate'],
                                        errors='coerse')
    for fert in FERTELEM:
        opdf[fert] = pd.to_numeric(opdf[fert], errors='coerse')

    # __________________________________________________________
    # case 1, values are > 0, so columns are in %
    df = opdf[opdf['productrate'] > 0]
    for elem in FERTELEM:
        opdf.loc[df.index, elem] = df['productrate'] * KGH_LBA * df[elem] / 100.
    opdf.loc[df.index, 'productrate'] = df['productrate'] * KGH_LBA

    # ________________________________________________________
    # case 2, value is -1 and so columns are in lbs / acre
    df = opdf[opdf['productrate'] < 0]
    opdf.loc[df.index, 'productrate'] = None
    for elem in FERTELEM:
        opdf.loc[df.index, elem] = df[elem] * KGH_LBA
    return opdf


def do(form):
    sites = form.getlist('sites[]')
    # treatments = form.getlist('treatments[]')
    agronomic = form.getlist('agronomic[]')
    soil = form.getlist('soil[]')
    years = form.getlist('year[]')
    detectlimit = form.getfirst('detectlimit', "1")

    writer = pd.ExcelWriter("/tmp/cscap.xlsx", engine='xlsxwriter')

    # Sheet one are operations
    opdf = get_operations(sites, years)
    opdf.to_excel(writer, 'Operations', index=False)

    if len(agronomic) > 0:
        do_agronomic(writer, sites, agronomic, years, detectlimit)
    if len(soil) > 0:
        do_soil(writer, sites, soil, years, detectlimit)
    do_dictionary(writer)

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
