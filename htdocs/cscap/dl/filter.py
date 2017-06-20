#!/usr/bin/env python
"""This is our fancy pants filter function.

We end up return a JSON document that lists out what is possible

{'treatments': ['ROT1', 'ROT2'...],
 'years': [2011, 2012, 2013...],
 'agronomic': ['AGR1', 'AGR2']
}
"""
import json
import sys
import cgi

import psycopg2
from pandas.io.sql import read_sql

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


def redup(arr):
    """Replace any codes that are collapsed by the above"""
    additional = []
    for key, vals in AGG.iteritems():
        for val in vals:
            if val in arr and key not in additional:
                additional.append(key)
    sys.stderr.write("dedup added %s to %s\n" % (str(additional), str(arr)))
    return arr + additional


def agg(arr):
    """Make listish and apply dedup logic"""
    if len(arr) == 0:
        arr.append('ZZZ')
    additional = []
    for val in arr:
        if val in AGG:
            additional += AGG[val]
    if len(additional) > 0:
        arr += additional
    sys.stderr.write("agg added %s to %s\n" % (str(additional), str(arr)))
    return arr


def do_filter(form):
    pgconn = psycopg2.connect(database='sustainablecorn', host='iemdb',
                              user='nobody')
    cursor = pgconn.cursor()
    res = {'treatments': [], 'agronomic': [], 'soil': [],
           'ghg': [], 'water': [], 'ipm': [], 'year': []}
    sites = agg(form.getlist('sites[]'))
    treatments = agg(form.getlist('treatments[]'))
    agronomic = agg(form.getlist('agronomic[]'))
    soil = agg(form.getlist('soil[]'))
    ghg = agg(form.getlist('ghg[]'))
    water = agg(form.getlist('water[]'))
    ipm = agg(form.getlist('ipm[]'))
    year = agg(form.getlist('year[]'))

    # build a list of treatments based on the sites selected
    df = read_sql("""
        SELECT distinct tillage, rotation, drainage, nitrogen,
        landscape from plotids where uniqueid in %s
        """, pgconn, params=(tuple(sites),), index_col=None)
    arr = []
    for col in ['tillage', 'rotation', 'drainage', 'nitrogen',
                'landscape']:
        for v in df[col].unique():
            if v is None:
                continue
            arr.append(v)
    res['treatments'] = redup(arr)

    # build a list of agronomic data based on the plotids and sites
    a = {}
    sql = []
    args = []
    for l, col in zip(['TIL', 'ROT', 'DWM', 'NIT', 'LND'],
                      ['tillage', 'rotation', 'drainage', 'nitrogen',
                       'landscape']):
        a[l] = [b for b in treatments if b.startswith(l)]
        if len(a[l]) > 0:
            sql.append(" %s in %%s" % (col,))
            args.append(tuple(a[l]))
    if len(sql) == 0:
        sql = ""
    else:
        sql = "where " + " and ".join(sql)

    df = read_sql("""
    with myplotids as (
        SELECT uniqueid, plotid from plotids """ + sql + """
    )
    SELECT distinct varname from agronomic_data a, myplotids p
    WHERE a.site = p.uniqueid and a.plotid = p.plotid
    """, pgconn, params=args, index_col=None)
    if len(df.index) > 0:
        res['agronomic'] = redup(df['varname'].values.tolist())

    # build a list of soil data based on the plotids and sites
    df = read_sql("""
    with myplotids as (
        SELECT uniqueid, plotid from plotids """ + sql + """
    )
    SELECT distinct varname from soil_data a, myplotids p
    WHERE a.site = p.uniqueid and a.plotid = p.plotid
    """, pgconn, params=args, index_col=None)
    if len(df.index) > 0:
        res['soil'] = redup(df['varname'].values.tolist())

    # Figure out which GHG variables we have
    df = read_sql("""
    with myplotids as (
        SELECT uniqueid, plotid from plotids
        WHERE uniqueid in %s
    )
    SELECT * from ghg_data a, myplotids p
    WHERE a.uniqueid = p.uniqueid and a.plotid = p.plotid
    """, pgconn, params=(tuple(sites), ), index_col=None)
    if len(df.index) > 0:
        for i in range(1, 17):
            col = "ghg%02i" % (i, )
            if len(df[df[col].notnull()].index) > 0:
                res['ghg'].append(col.upper())

    # Compute which years we have data for these locations
    df = read_sql("""
    WITH soil_years as (
        SELECT distinct year from soil_data where varname in %s
        and site in %s),
    agronomic_years as (
        SELECT distinct year from agronomic_data where varname in %s
        and site in %s),
    ghg_years as (
        SELECT distinct year from ghg_data where uniqueid in %s),
    agg as (SELECT year from soil_years UNION select year from agronomic_years
        UNION select year from ghg_years)

    SELECT distinct year from agg ORDER by year
    """, pgconn, params=(tuple(soil), tuple(sites), tuple(agronomic),
                         tuple(sites), tuple(sites)), index_col=None)
    for _, row in df.iterrows():
        res['year'].append(row['year'])

    return res


def main():
    """Do Stuff"""
    sys.stdout.write("Content-type: application/json\n\n")
    form = cgi.FieldStorage()
    res = do_filter(form)
    sys.stdout.write(json.dumps(res))


if __name__ == '__main__':
    main()
