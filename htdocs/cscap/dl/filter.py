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
pgconn = psycopg2.connect(database='sustainablecorn', host='iemdb',
                          user='nobody')
cursor = pgconn.cursor()


def agg(arr):
    if len(arr) > 0:
        return arr
    arr.append('ZZZ')
    return arr


def do_filter(form):
    res = {'treatments': [], 'agronomic': [], 'soil': [],
           'year': []}
    sites = agg(form.getlist('sites[]'))
    treatments = agg(form.getlist('treatments[]'))
    agronomic = agg(form.getlist('agronomic[]'))
    soil = agg(form.getlist('soil[]'))
    year = agg(form.getlist('year[]'))

    # build a list of treatments based on the sites selected
    df = read_sql("""
        SELECT distinct tillage, rotation, drainage, nitrogen,
        landscape from plotids where uniqueid in %s
        """, pgconn, params=(tuple(sites),), index_col=None)
    for col in ['tillage', 'rotation', 'drainage', 'nitrogen',
                'landscape']:
        for v in df[col].unique():
            if v is None:
                continue
            res['treatments'].append(v)

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
    sql = " and ".join(sql)

    df = read_sql("""
    with myplotids as (
        SELECT uniqueid, plotid from plotids where
        """ + sql + """
    )
    SELECT distinct varname from agronomic_data a, myplotids p
    WHERE a.site = p.uniqueid and a.plotid = p.plotid
    """, pgconn, params=args, index_col=None)
    if len(df.index) > 0:
        res['agronomic'] = df['varname'].values.tolist()

    # build a list of soil data based on the plotids and sites
    df = read_sql("""
    with myplotids as (
        SELECT uniqueid, plotid from plotids where
        """ + sql + """
    )
    SELECT distinct varname from soil_data a, myplotids p
    WHERE a.site = p.uniqueid and a.plotid = p.plotid
    """, pgconn, params=args, index_col=None)
    if len(df.index) > 0:
        res['soil'] = df['varname'].values.tolist()

    # Compute which years we have data for these locations
    df = read_sql("""
    WITH soil_years as (
        SELECT distinct year from soil_data where varname in %s
        and site in %s),
    agronomic_years as (
        SELECT distinct year from agronomic_data where varname in %s
        and site in %s),
    agg as (SELECT year from soil_years UNION select year from agronomic_years)

    SELECT distinct year from agg ORDER by year
    """, pgconn, params=(tuple(soil), tuple(sites), tuple(agronomic),
                         tuple(sites)), index_col=None)
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
