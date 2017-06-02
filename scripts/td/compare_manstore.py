"""Perhaps the TD manstore has newer entries than CSCAP"""
from __future__ import print_function
import psycopg2
import pandas as pd
from pandas.io.sql import read_sql

CSCAPSITES = ['DPAC', 'HICKS.B', 'SERF', 'STJOHNS']
IGNORECOLS = ['editedby', 'cropyear']


def diff(csrow, tdrow):
    """Difference two rows"""
    for col, val in csrow.iteritems():
        if col not in tdrow.index or col in IGNORECOLS:
            continue
        val2 = tdrow.get(col)
        if val is not None and val != val2:
            print(("Site: %s Date:%s Difference [%s] cscap: %s td: %s"
                   ) % (tdrow['uniqueid'], tdrow['valid'], col, val, val2))


def cmp_fertilizer(cscapdf, tddf):
    """Compare Fertilizer entries"""
    df2 = tddf[pd.notnull(tddf['valid'])]
    for _, tdrow in df2.iterrows():
        if tdrow['uniqueid'] not in CSCAPSITES:
            continue
        df = cscapdf[(cscapdf['valid'] == tdrow['valid']) &
                     (cscapdf['uniqueid'] == tdrow['uniqueid'])]
        if len(df.index) == 0:
            # print(("New entry for %s valid: %s updated: %s"
            #       ) % (tdrow['uniqueid'], tdrow['valid'], tdrow['updated']))
            continue
        df = cscapdf[(cscapdf['valid'] == tdrow['valid']) &
                     (cscapdf['uniqueid'] == tdrow['uniqueid']) &
                     (cscapdf['updated'] == tdrow['updated'])]
        if len(df.index) == 1:
            diff(df.iloc[0], tdrow)
        # print(("Updated entry for %s valid: %s updated: %s"
        #       ) % (tdrow['uniqueid'], tdrow['valid'], tdrow['updated']))


def get_dfs():
    """Return the dataframes"""
    pgconn_cscap = psycopg2.connect(database='sustainablecorn')
    pgconn_td = psycopg2.connect(database='td')
    cscap = {'management': None, 'pesticides': None, 'operations': None}
    td = {'dwm': None, 'irrigation': None, 'notes': None, 'pesticides': None,
          'residue_mngt': None, 'soil_fert': None, 'plant_harvest': None}
    for key in cscap:
        cscap[key] = read_sql("SELECT * from %s" % (key, ), pgconn_cscap,
                              index_col=None)
    for key in td:
        td[key] = read_sql("SELECT * from %s" % (key, ), pgconn_td,
                           index_col=None)
        # Translate entries for SERF_IA into SERF
        td[key].loc[td[key]['uniqueid'] == 'SERF_IA', 'uniqueid'] = 'SERF'
        # Translate entries for HICKS_B into HICKS.B
        td[key].loc[td[key]['uniqueid'] == 'HICKS_B', 'uniqueid'] = 'HICKS.B'
    return cscap, td


def main():
    """Go Main"""
    cscap, td = get_dfs()
    cmp_fertilizer(cscap['operations'], td['soil_fert'])


if __name__ == '__main__':
    main()
