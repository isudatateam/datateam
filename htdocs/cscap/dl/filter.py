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
    treatments.append(None)
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
    for l in ['TIL', 'ROT', 'DWM', 'NIT', 'LND']:
        a[l] = [b for b in treatments if (b is None or b.startswith(l))]
    df = read_sql("""
    with myplotids as (
        SELECT uniqueid, plotid from plotids where
        tillage in %s and rotation in %s and drainage in %s and nitrogen in %s
        and landscape in %s
    )
    SELECT distinct varname from agronomic_data a, myplotids p
    WHERE a.site = p.uniqueid and a.plotid = p.plotid
    """, pgconn, params=(tuple(a['TIL']), tuple(a['ROT']),
                         tuple(a['DWM']), tuple(a['NIT']),
                         tuple(a['LND'])), index_col=None)
    if len(df.index) > 0:
        res['agronomic'] = df['varname'].values.tolist()
    return res


def main():
    """Do Stuff"""
    sys.stdout.write("Content-type: application/json\n\n")
    form = cgi.FieldStorage()
    res = do_filter(form)
    sys.stdout.write(json.dumps(res))

if __name__ == '__main__':
    main()
