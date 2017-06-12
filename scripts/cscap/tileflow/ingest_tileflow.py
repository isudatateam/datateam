"""Tileflow ingest"""
from __future__ import print_function
import sys
import datetime

import pandas as pd
import psycopg2
import numpy as np
from pyiem.cscap_utils import get_config, get_spreadsheet_client, Spreadsheet

CENTRAL_TIME = ['ISUAG', 'GILMORE', 'SERF', 'CLAY_C', 'CLAY_R',
                'MUDS2', 'MUDS3_OLD', 'MUDS4', 'SERF_SD', 'SERF_IA',
                'STORY', 'UBWC']


def process5(spreadsheetid):
    """ SERF """
    config = get_config()
    sprclient = get_spreadsheet_client(config)
    ss = Spreadsheet(sprclient, spreadsheetid)
    df = dict()
    rows = []
    for year in ss.worksheets:
        sheet = ss.worksheets[year]
        lf = sheet.get_list_feed()
        for entry in lf.entry:
            rows.append(entry.to_dict())
    bigdf = pd.DataFrame(rows)
    bigdf['valid'] = pd.to_datetime(bigdf['date'] + " " + bigdf['time'],
                                    format='%m/%d/%Y %H:%M',
                                    errors='coerce')
    print(bigdf.columns)
    for col in bigdf.columns:
        if col.find('wat1tileflow') == -1:
            print("Skipping column: %s" % (repr(col,)))
            continue
        plotid = col.replace("wat1tileflow", "").upper()
        col = '%swat1tileflow' % (plotid.lower(), )
        df[plotid] = bigdf[['valid', col]].copy()
        df[plotid].rename(columns={col: 'discharge_mm'}, inplace=True)
    return df


def process3(filename):
    """ SERF"""
    retdf = {}
    for p in range(1, 7):
        retdf['S%s' % (p, )] = pd.DataFrame()
    df = pd.read_excel(filename, na_values=['NA'])
    for p in range(1, 7):
        plot = "S%s" % (p,)
        retdf[plot]['valid'] = df['Date&Time']
        retdf[plot]['discharge_mm'] = df['WAT1 Plot%s' % (p, )]
    return retdf


def process2(spreadsheetid):
    """ Ingest a google sheet of data """
    # raise Exception(
    #    "BUG as plotids get 'T' added when starts with numeric value")
    config = get_config()
    sprclient = get_spreadsheet_client(config)
    ss = Spreadsheet(sprclient, spreadsheetid)
    df = dict()
    rows = []
    for year in ss.worksheets:
        sheet = ss.worksheets[year]
        lf = sheet.get_list_feed()
        for entry in lf.entry:
            rows.append(entry.to_dict())
    bigdf = pd.DataFrame(rows)
    bigdf['valid'] = pd.to_datetime(bigdf['enddate'], format='%m/%d/%Y',
                                    errors='coerce')
    bigdf['valid'] = bigdf['valid'].apply(lambda s: s.replace(hour=12))
    bigdf['wat1'] = pd.to_numeric(bigdf['wat1'], errors='coerce')
    bigdf['discharge_mm'] = bigdf['wat1']
    for plotid in bigdf['plotid'].unique():
        df[plotid] = bigdf[bigdf['plotid'] == plotid].copy()
    return df


def process1(filename):
    """Format 1, DPAC"""
    df2 = {'NW': pd.DataFrame(), 'NE': pd.DataFrame(),
           'SW': pd.DataFrame(), 'SE': pd.DataFrame()}
    df = pd.read_excel(filename, sheetname=None, na_values=['NaN'],
                       skiprows=[1, ])
    bigdf = pd.concat(df)

    bigdf['valid'] = pd.to_datetime(
        (bigdf['Date'].dt.strftime("%m/%d/%Y") + " " +
         bigdf['Time'].astype('str')),
        format='%m/%d/%Y %H:%M:%S', errors='coerce')

    for sector in ['NW', 'NE', 'SE', 'SW']:
        df2[sector]['valid'] = bigdf['valid']
        df2[sector]['discharge_mm'] = (
            bigdf['%s WAT1 Tile Flow' % (sector,)])
    return df2


def database_save(df, uniqueid, plotid, project):
    pgconn = psycopg2.connect(database=project, host='iemdb')
    cursor = pgconn.cursor()
    for i, row in df.iterrows():
        if not isinstance(row['valid'], datetime.datetime):
            print('Row df.index=%s, valid=%s, culling' % (i, row['valid']))
            df.drop(i, inplace=True)
        elif row['valid'] is pd.NaT:
            print('Row df.index=%s, valid=%s, culling' % (i, row['valid']))
            df.drop(i, inplace=True)
    if len(df.index) == 0:
        print("Skipping plot: %s as there is no data?" % (plotid,))
        return
    minvalid = df['valid'].min()
    maxvalid = df['valid'].max()
    print("Time Domain: %s - %s" % (minvalid, maxvalid))

    # Data is always standard time, this is why this works
    tzoff = "05" if uniqueid not in CENTRAL_TIME else "06"
    cursor.execute("""
        DELETE from tileflow_data WHERE
        uniqueid = %s and plotid = %s and valid >= %s and valid <= %s
    """, (uniqueid, plotid, minvalid.strftime("%Y-%m-%d %H:%M-"+tzoff),
          maxvalid.strftime("%Y-%m-%d %H:%M-"+tzoff)))

    if cursor.rowcount > 0:
        print("DELETED %s rows previously saved!" % (cursor.rowcount, ))

    def v(row, name):
        val = row.get(name)
        if val is None or val is pd.NaT:
            return 'null'
        if isinstance(val, (unicode, str)):
            if val.strip().lower() in ['nan', 'did not collect', '#ref!',
                                       'n/a']:
                return 'null'
            return val
        # elif isinstance(val, pd.core.series.Series):
        #    print val
        #    print row
        #    sys.exit()
        try:
            if np.isnan(val):
                return 'null'
        except Exception, exp:
            print(exp)
            print(('Plot: %s Val: %s[%s] Name: %s Valid: %s'
                   ) % (plotid, val, type(val), name, row['valid']))
            return 'null'
        return val
    for _, row in df.iterrows():
        cursor.execute("""
        INSERT into tileflow_data(uniqueid, plotid, valid,
        discharge_mm, discharge_mm_qc, discharge_m3, discharge_m3_qc)
        VALUES ('%s', '%s', '%s', %s, %s, %s, %s)
        """ % (uniqueid, plotid,
               row['valid'].strftime("%Y-%m-%d %H:%M-"+tzoff),
               v(row, 'discharge_mm'), v(row, 'discharge_mm'),
               v(row, 'discharge_m3'), v(row, 'discharge_m3')))

    print("Processed %s entries" % (len(df.index), ))
    cursor.close()
    pgconn.commit()
    pgconn.close()


def main(argv):
    """Go Main"""
    fn = argv[1]
    fmt = argv[2]
    uniqueid = argv[3]
    plotid = argv[4]
    project = argv[5]
    if fmt == '1':
        df = process1(fn)
    elif fmt == '2':
        df = process2(fn)
    elif fmt == '3':
        df = process3(fn)
    elif fmt == '5':
        df = process5(fn)
    for plotid in df:
        print("  --> database_save plotid: %s" % (plotid,))
        database_save(df[plotid], uniqueid, plotid, project)


if __name__ == '__main__':
    main(sys.argv)
