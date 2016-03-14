import pandas as pd
import psycopg2
import sys
import datetime
import numpy as np
from pyiem.cscap_utils import get_config, get_spreadsheet_client, Spreadsheet

CENTRAL_TIME = ['ISUAG', 'GILMORE', 'SERF']


def process4(spreadsheetid):
    """ Ingest a google sheet of data """
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
    bigdf['valid'] = pd.to_datetime(bigdf['datetime'],
                                    format='%m/%d/%Y %H:%M:%S',
                                    errors='coerce')
    print bigdf['valid']
    df['WN'] = bigdf[['valid', 'wndischargemm']].copy()
    df['WS'] = bigdf[['valid', 'wsdischargemm']].copy()
    df['WN'].rename(columns={'wndischargemm': 'discharge_mm'},
                    inplace=True)
    df['WS'].rename(columns={'wsdischargemm': 'discharge_mm'},
                    inplace=True)
    df['WN']['discharge_mm'] = pd.to_numeric(df['WN']['discharge_mm'],
                                             errors='coerce')
    df['WS']['discharge_mm'] = pd.to_numeric(df['WS']['discharge_mm'],
                                             errors='coerce')
    return df


def process3(fn):
    """ SERF"""
    df = pd.read_excel(fn, sheetname=None)
    for plotid in df:
        df[plotid]['valid'] = pd.to_datetime(df[plotid]['valid'],
                                             format='%m/%d/%Y %H:%M',
                                             errors='coerce')
        df[plotid]['discharge_mm'] = df[plotid]['WAT1']
    return df


def process2(spreadsheetid):
    """ Ingest a google sheet of data """
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


def process1(fn):
    """Format 1, DPAC"""
    df2 = {'NW': pd.DataFrame(), 'NE': pd.DataFrame(),
           'SW': pd.DataFrame(), 'SE': pd.DataFrame()}
    df = pd.read_excel(fn, sheetname=None, na_values=['NaN'])
    for sector in ['NW', 'NE', 'SE', 'SW']:
        df2[sector]['valid'] = df['Drainage Flow']['Date&Time']
        df2[sector]['discharge_mm'] = (
            df['Drainage Flow']['%s_Discharge(mm)' % (sector,)])
        df2[sector]['discharge_m3'] = (
            df['Tile Net Discharge']['%s_Discharge(m3)' % (sector,)])
    return df2


def database_save(df, uniqueid, plotid):
    pgconn = psycopg2.connect(database='sustainablecorn', host='iemdb')
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
        if val is None:
            return 'null'
        if isinstance(val, unicode):
            if val.strip().lower() in ['nan', 'did not collect']:
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
            print exp
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
    fn = argv[1]
    fmt = argv[2]
    uniqueid = argv[3]
    plotid = argv[4]
    if fmt == '1':
        df = process1(fn)
    elif fmt == '2':
        df = process2(fn)
    elif fmt == '3':
        df = process3(fn)
    elif fmt == '4':
        df = process4(fn)
    for plotid in df:
        print("  --> database_save plotid: %s" % (plotid,))
        database_save(df[plotid], uniqueid, plotid)


if __name__ == '__main__':
    main(sys.argv)
