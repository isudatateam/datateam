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
# -----------------------------------------------------------------------------
# NOTE: filter.py is upstream for this table, copy to dl.py
AGG = {"_T1": ['ROT4', 'ROT5', 'ROT54'],
       "_T2": ["ROT8", 'ROT7', 'ROT6'],
       "_T3": ["ROT16", "ROT15", "ROT17"],
       "_T4": ["ROT37", "ROT36", "ROT55", "ROT59", "ROT60"],
       "_T5": ["ROT61", "ROT56"],
       "_T6": ["ROT57", "ROT58"],
       "_T7": ["ROT40", "ROT50"],
       "_S1": ["SOIL41", "SOIL34", "SOIL29", "SOIL30", "SOIL31", "SOIL2",
               "SOIL35", "SOIL32", "SOIL42", "SOIL33", "SOIL39"]}


def valid2date(df):
    """If dataframe has valid in columns, rename it to date"""
    if 'valid' in df.columns:
        df.rename(columns={'valid': 'date'}, inplace=True)


def redup(arr):
    """Replace any codes that are collapsed by the above"""
    additional = []
    for key, vals in AGG.iteritems():
        for val in vals:
            if val in arr and key not in additional:
                additional.append(key)
    sys.stderr.write("dedup added %s to %s\n" % (str(additional), str(arr)))
    return arr + additional


def conv(value, detectlimit, missing):
    """Convert a value into something that gets returned"""
    if value is None or value == '':
        return missing
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
        return missing
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


def do_ghg(writer, sites, ghg, years):
    """get GHG data"""
    cols = ", ".join(ghg)
    df = read_sql("""
    SELECT uniqueid, plotid, date, year, """ + cols + """ from ghg_data
    WHERE uniqueid in %s and year in %s ORDER by uniqueid, year
    """, PGCONN, params=(tuple(sites), tuple(years)), index_col=None)
    df.to_excel(writer, 'GHG', index=False)


def do_ipm(writer, sites, ipm, years):
    """get IPM data"""
    cols = ", ".join(ipm)
    df = read_sql("""
    SELECT uniqueid, plotid, date, year, """ + cols + """ from ipm_data
    WHERE uniqueid in %s and year in %s ORDER by uniqueid, year
    """, PGCONN, params=(tuple(sites), tuple(years)), index_col=None)
    df.to_excel(writer, 'IPM', index=False)


def do_agronomic(writer, sites, agronomic, years, detectlimit, missing):
    """get agronomic data"""
    df = read_sql("""
    SELECT site, plotid, varname, year, value from agronomic_data
    WHERE site in %s and year in %s and varname in %s ORDER by site, year
    """, PGCONN, params=(tuple(sites), tuple(years),
                         tuple(agronomic)), index_col=None)
    df['value'] = df['value'].apply(lambda x: conv(x, detectlimit, missing))
    df = pd.pivot_table(df, index=('site', 'plotid', 'year'),
                        values='value', columns=('varname',),
                        aggfunc=lambda x: ' '.join(str(v) for v in x))
    df.reset_index(inplace=True)
    valid2date(df)
    df.to_excel(writer, 'Agronomic', index=False)


def do_soil(writer, sites, soil, years, detectlimit, missing):
    """get soil data"""
    df = read_sql("""
    SELECT site, plotid, depth, subsample, varname, year, value
    from soil_data
    WHERE site in %s and year in %s and varname in %s ORDER by site, year
    """, PGCONN, params=(tuple(sites), tuple(years),
                         tuple(soil)), index_col=None)
    df['value'] = df['value'].apply(lambda x: conv(x, detectlimit, missing))
    df = pd.pivot_table(df, index=('site', 'plotid', 'depth', 'subsample',
                                   'year'),
                        values='value', columns=('varname',),
                        aggfunc=lambda x: ' '.join(str(v) for v in x))
    df.reset_index(inplace=True)
    valid2date(df)
    df.to_excel(writer, 'Soil', index=False)


def do_operations(writer, sites, years):
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
        opdf.loc[df.index, elem] = (df['productrate'] * KGH_LBA *
                                    df[elem] / 100.)
    opdf.loc[df.index, 'productrate'] = df['productrate'] * KGH_LBA

    # ________________________________________________________
    # case 2, value is -1 and so columns are in lbs / acre
    df = opdf[opdf['productrate'] < 0]
    opdf.loc[df.index, 'productrate'] = None
    for elem in FERTELEM:
        opdf.loc[df.index, elem] = df[elem] * KGH_LBA
    valid2date(opdf)
    opdf.to_excel(writer, 'Operations', index=False)


def do_management(writer, sites, years):
    """Return a DataFrame for the management"""
    opdf = read_sql("""
    SELECT * from management where uniqueid in %s and cropyear in %s
    ORDER by cropyear ASC
    """, PGCONN, params=(tuple(sites), tuple(years)))
    opdf.to_excel(writer, 'Management', index=False)


def do_pesticides(writer, sites, years):
    """Return a DataFrame for the pesticides"""
    opdf = read_sql("""
    SELECT * from pesticides where uniqueid in %s and cropyear in %s
    ORDER by cropyear ASC
    """, PGCONN, params=(tuple(sites), tuple(years)))
    valid2date(opdf)
    opdf.to_excel(writer, 'Pesticides', index=False)


def do_plotids(writer, sites):
    """Write plotids to the spreadsheet"""
    opdf = read_sql("""
        SELECT uniqueid, plotid, rep, tillage, rotation, drainage, nitrogen,
        landscape, herbicide, soilseriesname2, soiltextureseries2,
        soilseriesname1,
        soiltextureseries1, soilseriesdescription1, soiltaxonomicclass1,
        soilseriesdescription2, soiltaxonomicclass2, soiltextureseries3,
        soiltaxonomicclass3, soilseriesdescription3, soilseriesname3,
        soiltaxonomicclass4, soilseriesdescription4, soilseriesname4,
        soiltextureseries4, notes, agro, soil, ghg, ipmcscap, ipmusb
        from plotids where uniqueid in %s
        ORDER by uniqueid, plotid ASC
    """, PGCONN, params=(tuple(sites), ))
    opdf[opdf.columns[:30]].to_excel(writer, 'Plot IDs', index=False)


def do_work(form):
    """do great things"""
    sites = form.getlist('sites[]')
    # treatments = form.getlist('treatments[]')
    agronomic = redup(form.getlist('agronomic[]'))
    soil = redup(form.getlist('soil[]'))
    ghg = redup(form.getlist('ghg[]'))
    water = redup(form.getlist('water[]'))
    ipm = redup(form.getlist('ipm[]'))
    years = redup(form.getlist('year[]'))
    shm = redup(form.getlist('shm[]'))
    missing = form.getfirst('missing', "M")
    if missing == '__custom__':
        missing = form.getfirst('custom_missing', 'M')
    sys.stderr.write("Missing is %s\n" % (missing, ))
    if len(years) == 0:
        years = [str(s) for s in range(2011, 2016)]
    detectlimit = form.getfirst('detectlimit', "1")

    writer = pd.ExcelWriter("/tmp/cscap.xlsx", engine='xlsxwriter')

    # Sheet one are operations
    if "SHM1" in shm:
        do_operations(writer, sites, years)

    if len(agronomic) > 0:
        do_agronomic(writer, sites, agronomic, years, detectlimit, missing)
    if len(soil) > 0:
        do_soil(writer, sites, soil, years, detectlimit, missing)
    if len(ghg) > 0:
        do_ghg(writer, sites, ghg, years)
    if len(ipm) > 0:
        do_ipm(writer, sites, ipm, years)

    # Management
    if 'SHM3' in shm:
        do_management(writer, sites, years)

    # Pesticides
    if 'SHM2' in shm:
        do_pesticides(writer, sites, years)

    # DWM SHM4 is a TODO

    # Plot IDs
    if 'SHM4' in shm:
        do_plotids(writer, sites)

    if 'SHM5' in shm:
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
    do_work(form)


if __name__ == '__main__':
    main()
