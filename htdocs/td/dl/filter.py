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

from pandas.io.sql import read_sql
from pyiem.util import get_dbconn, ssw

# NOTE: filter.py is upstream for this table, copy to dl.py
AGG = {"_T1": ['ROT4', 'ROT5', 'ROT54'],
       "_T2": ["ROT8", 'ROT7', 'ROT6'],
       "_T3": ["ROT16", "ROT15", "ROT17"],
       "_T4": ["ROT37", "ROT36", "ROT55", "ROT59"],
       "_T5": ["ROT61", "ROT56", "ROT1"],
       "_T6": ["ROT57", "ROT58", "ROT38"],
       "_T7": ["ROT40", "ROT50"],
       "_S1": ["SOIL41", "SOIL34", "SOIL29", "SOIL30", "SOIL31", "SOIL2",
               "SOIL35", "SOIL32", "SOIL42", "SOIL33", "SOIL39"],
       "_S19": ["SOIL19.8", "SOIL19.11", "SOIL19.12", "SOIL19.1",
                "SOIL19.10", "SOIL19.2", "SOIL19.5", "SOIL19.7", "SOIL19.6",
                "SOIL19.13"]}


def redup(arr):
    """Replace any codes that are collapsed by the above"""
    additional = []
    for key in AGG:
        vals = AGG[key]
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
    pgconn = get_dbconn('td')
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
    arsql = []
    args = [tuple(sites), ]
    for l, col in zip(['TIL', 'ROT', 'DWM', 'NIT', 'LND'],
                      ['tillage', 'rotation', 'drainage', 'nitrogen',
                       'landscape']):
        a[l] = [b for b in treatments if b.startswith(l)]
        if l == 'LND':
            a[l].append('n/a')
        if len(a[l]) > 0:
            arsql.append(" %s in %%s" % (col,))
            args.append(tuple(a[l]))
    if len(arsql) == 0:
        sql = ""
    else:
        sql = " and "
        sql = sql + " and ".join(arsql)

    df = read_sql("""
    with myplotids as (
        SELECT uniqueid, plotid, nitrogen from plotids
        WHERE uniqueid in %s """ + sql + """
    )
    SELECT distinct varname from agronomic_data a, myplotids p
    WHERE a.uniqueid = p.uniqueid and a.plotid = p.plotid and
    a.value not in ('n/a', 'did not collect')
    """, pgconn, params=args, index_col=None)
    if not df.empty:
        res['agronomic'] = redup(df['varname'].values.tolist())

    # build a list of soil data based on the plotids and sites
    df = read_sql("""
    with myplotids as (
        SELECT uniqueid, plotid from plotids
        WHERE uniqueid in %s """ + sql + """
    )
    SELECT distinct varname from soil_data a, myplotids p
    WHERE a.uniqueid = p.uniqueid and a.plotid = p.plotid and
    a.value not in ('n/a', 'did not collect')
    """, pgconn, params=args, index_col=None)
    if not df.empty:
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
    if not df.empty:
        for i in range(1, 17):
            col = "ghg%02i" % (i, )
            if len(df[df[col].notnull()].index) > 0:
                res['ghg'].append(col.upper())

    # Figure out which IPM variables we have
    df = read_sql("""
    with myplotids as (
        SELECT uniqueid, plotid from plotids
        WHERE uniqueid in %s
    )
    SELECT * from ipm_data a, myplotids p
    WHERE a.uniqueid = p.uniqueid and a.plotid = p.plotid
    """, pgconn, params=(tuple(sites), ), index_col=None)
    if not df.empty:
        for i in range(1, 15):
            col = "ipm%02i" % (i, )
            if len(df[df[col].notnull()].index) > 0:
                res['ipm'].append(col.upper())

    # Compute which years we have data for these locations
    _g = [" %s is not null " % (g, ) for g in ghg if g != 'ZZZ']
    ghglimiter = "1 = 2"
    if _g:
        ghglimiter = "( %s )" % ("or".join(_g), )
    df = read_sql("""
    WITH soil_years as (
        SELECT distinct year from soil_data where varname in %s
        and uniqueid in %s and value not in ('n/a', 'did not collect')),
    agronomic_years as (
        SELECT distinct year from agronomic_data where varname in %s
        and uniqueid in %s and value not in ('n/a', 'did not collect')),
    ghg_years as (
        SELECT distinct year from ghg_data where uniqueid in %s
        and """ + ghglimiter + """),
    agg as (SELECT year from soil_years UNION select year from agronomic_years
        UNION select year from ghg_years)

    SELECT distinct year from agg ORDER by year
    """, pgconn, params=(tuple(soil), tuple(sites), tuple(agronomic),
                         tuple(sites), tuple(sites)), index_col=None)
    for _, row in df.iterrows():
        res['year'].append(float(row['year']))

    return res


def main():
    """Do Stuff"""
    ssw("Content-type: application/json\n\n")
    form = cgi.FieldStorage()
    res = do_filter(form)
    ssw(json.dumps(res))


if __name__ == '__main__':
    main()
