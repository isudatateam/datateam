"""Process the decagon data"""
from __future__ import print_function
import sys
import datetime

import pandas as pd
from pyiem.util import get_dbconn

CENTRAL_TIME = ['SERF_IA', 'BEAR', 'CLAY_U', 'FAIRM', 'MAASS', 'SERF_SD']


def translate(df):
    """Translate data columns"""
    x = {}
    # print df.columns
    for colname in df.columns:
        tokens = colname.split()
        name = None
        if colname.find('Measurement Time') > 0:
            name = "valid"
        elif colname.startswith('Port '):
            name = 'd%s' % (tokens[1].split(".")[0], )
            if colname.find('Bulk') > 0:
                name += "ec"
            elif colname.find('VWC') > 0:
                name += 'moisture'
            else:
                name += "temp"

        if name is not None:
            x[colname] = name

    df.rename(columns=x, inplace=True)


def process0(fn):
    """DPAC"""
    df = pd.read_csv(fn, index_col=False, sep='\t')
    df.columns = ['valid', 
                  'd1moisture', 'd1temp',  'd1ec',
                  'd2moisture', 'd2temp',
                  'd3moisture', 'd3temp',
                  'd4moisture', 'd4temp',
                  'd5moisture', 'd5temp',
                  ]
    df['valid'] = pd.to_datetime(df['valid'])
    return df


def process1(fn):
    df = pd.read_csv(fn, skiprows=[0, 1, 2, 3, 5, 6], index_col=False)
    df.columns = ['valid', 'bogus',
                  'd1temp', 'd1moisture', 'd1ec',
                  'd2temp', 'd2moisture', 'd2ec',
                  'd3temp', 'd3moisture', 'd3ec',
                  'd4temp', 'd4moisture', 'd4ec',
                  'd5temp', 'd5moisture', 'd5ec',
                  'd6temp', 'd6moisture', 'd6ec',
                  'd7temp', 'd7moisture', 'd7ec',
                  'd1temp_2', 'd1moisture_2', 'd1ec_2',
                  'd2temp_2', 'd2moisture_2', 'd2ec_2',
                  'd3temp_2', 'd3moisture_2', 'd3ec_2',
                  'd4temp_2', 'd4moisture_2', 'd4ec_2',
                  'd5temp_2', 'd5moisture_2', 'd5ec_2',
                  'd6temp_2', 'd6moisture_2', 'd6ec_2',
                  'd7temp_2', 'd7moisture_2', 'd7ec_2',
                  'bogus', 'bogus', 'bogus', 'bogus', 'bogus', 'bogus'
                  ]
    df['valid'] = pd.to_datetime(df['valid'])
    p1 = df[['valid',
             'd1temp', 'd1moisture', 'd1ec',
             'd2temp', 'd2moisture', 'd2ec',
             'd3temp', 'd3moisture', 'd3ec',
             'd4temp', 'd4moisture', 'd4ec',
             'd5temp', 'd5moisture', 'd5ec',
             'd6temp', 'd6moisture', 'd6ec',
             'd7temp', 'd7moisture', 'd7ec']]
    p2 = df[['valid',
             'd1temp_2', 'd1moisture_2', 'd1ec_2',
             'd2temp_2', 'd2moisture_2', 'd2ec_2',
             'd3temp_2', 'd3moisture_2', 'd3ec_2',
             'd4temp_2', 'd4moisture_2', 'd4ec_2',
             'd5temp_2', 'd5moisture_2', 'd5ec_2',
             'd6temp_2', 'd6moisture_2', 'd6ec_2',
             'd7temp_2', 'd7moisture_2', 'd7ec_2']]
    p2.columns = p1.columns
    return {'CD1': p1, 'CD2': p2}


def process2(fn):
    mydict = pd.read_excel(fn, sheet_name=None, index_col=False)
    df = pd.concat(mydict.values())
    gdf = df[['Date', 'grass 3"', 'grass 6"', 'grass 12"',
              'grass 24"', 'grass 36"']]
    gdf.columns = ['valid', 'd1moisture', 'd2moisture', 'd3moisture',
                   'd4moisture', 'd5moisture']
    tdf = df[['Date', 'trees 3"', 'trees 6"', 'trees 12"',
              'trees 24"', 'trees 36"']]
    tdf.columns = ['valid', 'd1moisture', 'd2moisture', 'd3moisture',
                   'd4moisture', 'd5moisture']
    return dict(trees=tdf, grass=gdf)


def process3(fn):
    """SERF_IA"""
    mydict = pd.read_excel(fn, sheet_name=None, index_col=False)
    # Need to load up rows 0 and 1 into the column names
    for sheetname in mydict:
        if sheetname in ['metadata', ]:
            continue
        df = mydict[sheetname]
        row0 = df.iloc[0, :]
        row1 = df.iloc[1, :]
        reg = {df.columns[0]: 'valid'}
        for c, r0, r1 in zip(df.columns, row0, row1):
            reg[c] = "%s %s %s" % (c, r0, r1)
        df.rename(columns=reg, inplace=True)
        df.drop(df.head(2).index, inplace=True)
        translate(df)
    return mydict


def process4(fn):
    df = pd.read_excel(fn, skiprows=range(4), sheet_name='Data',
                       index_col=False)
    # df = pd.read_csv(fn, skiprows=range(6), index_col=False)
    df.columns = ['valid', 'bogus',
                  'd1temp', 'd1moisture', 'd1ec', 'd1ec2',
                  'd2temp', 'd2moisture', 'd2ec', 'd2ec2',
                  'd3temp', 'd3moisture', 'd3ec', 'd3ec2',
                  'd4temp', 'd4moisture', 'd4ec', 'd4ec2',
                  'd5temp', 'd5moisture', 'd5ec', 'd5ec2',
                  'd6temp', 'd6moisture', 'd6ec', 'd6ec2',
                  'd7temp', 'd7moisture', 'd7ec', 'd7ec2',
                  'd1temp_2', 'd1moisture_2', 'd1ec_2', 'd1ec2_2',
                  'd2temp_2', 'd2moisture_2', 'd2ec_2', 'd2ec2_2',
                  'd3temp_2', 'd3moisture_2', 'd3ec_2', 'd3ec2_2',
                  'd4temp_2', 'd4moisture_2', 'd4ec_2', 'd4ec2_2',
                  'd5temp_2', 'd5moisture_2', 'd5ec_2', 'd5ec2_2',
                  'd6temp_2', 'd6moisture_2', 'd6ec_2', 'd6ec2_2',
                  'd7temp_2', 'd7moisture_2', 'd7ec_2', 'd7ec2_2',
                  ]
    df['valid'] = pd.to_datetime(df['valid'])
    p1 = df[['valid',
             'd1temp', 'd1moisture', 'd1ec',
             'd2temp', 'd2moisture', 'd2ec',
             'd3temp', 'd3moisture', 'd3ec',
             'd4temp', 'd4moisture', 'd4ec',
             'd5temp', 'd5moisture', 'd5ec',
             'd6temp', 'd6moisture', 'd6ec',
             'd7temp', 'd7moisture', 'd7ec']]
    p2 = df[['valid',
             'd1temp_2', 'd1moisture_2', 'd1ec_2',
             'd2temp_2', 'd2moisture_2', 'd2ec_2',
             'd3temp_2', 'd3moisture_2', 'd3ec_2',
             'd4temp_2', 'd4moisture_2', 'd4ec_2',
             'd5temp_2', 'd5moisture_2', 'd5ec_2',
             'd6temp_2', 'd6moisture_2', 'd6ec_2',
             'd7temp_2', 'd7moisture_2', 'd7ec_2']]
    p2.columns = p1.columns
    return {'1': p1, '2': p2}


def process5(fn):
    df = pd.read_excel(fn, skiprows=range(6), sheet_name='Data',
                       index_col=False)
    df.columns = ['valid', 'bogus',
                  'd1temp', 'd1moisture', 'd1ec', 'd1ec2', 'b', 'b', 'b', 'b',
                  'd2temp', 'd2moisture', 'd2ec', 'd2ec2', 'b', 'b', 'b', 'b',
                  'd3temp', 'd3moisture', 'd3ec', 'd3ec2', 'b', 'b', 'b', 'b',
                  'd4temp', 'd4moisture', 'd4ec', 'd4ec2', 'b', 'b', 'b', 'b',
                  'd5temp', 'd5moisture', 'd5ec', 'd5ec2', 'b', 'b', 'b', 'b',
                  'd6temp', 'd6moisture', 'd6ec', 'd6ec2', 'b', 'b', 'b', 'b',
                  'd7temp', 'd7moisture', 'd7ec', 'd7ec2', 'b', 'b', 'b', 'b',
                  'd1temp_2', 'd1moisture_2', 'd1ec_2', 'd1ec2_2',
                  'b', 'b', 'b', 'b',
                  'd2temp_2', 'd2moisture_2', 'd2ec_2', 'd2ec2_2',
                  'b', 'b', 'b', 'b',
                  'd3temp_2', 'd3moisture_2', 'd3ec_2', 'd3ec2_2',
                  'b', 'b', 'b', 'b',
                  'd4temp_2', 'd4moisture_2', 'd4ec_2', 'd4ec2_2',
                  'b', 'b', 'b', 'b',
                  'd5temp_2', 'd5moisture_2', 'd5ec_2', 'd5ec2_2',
                  'b', 'b', 'b', 'b',
                  'd6temp_2', 'd6moisture_2', 'd6ec_2', 'd6ec2_2',
                  'b', 'b', 'b', 'b',
                  'd7temp_2', 'd7moisture_2', 'd7ec_2', 'd7ec2_2',
                  'b', 'b', 'b', 'b',
                  'b', 'b', 'b', 'b', 'b', 'b', 'b', 'b',
                  'b', 'b', 'b', 'b', 'b', 'b', 'b', 'b',
                  ]
    df['valid'] = pd.to_datetime(df['valid'], errors='coerse')
    p1 = df[['valid',
             'd1temp', 'd1moisture', 'd1ec',
             'd2temp', 'd2moisture', 'd2ec',
             'd3temp', 'd3moisture', 'd3ec',
             'd4temp', 'd4moisture', 'd4ec',
             'd5temp', 'd5moisture', 'd5ec',
             'd6temp', 'd6moisture', 'd6ec',
             'd7temp', 'd7moisture', 'd7ec']]
    p2 = df[['valid',
             'd1temp_2', 'd1moisture_2', 'd1ec_2',
             'd2temp_2', 'd2moisture_2', 'd2ec_2',
             'd3temp_2', 'd3moisture_2', 'd3ec_2',
             'd4temp_2', 'd4moisture_2', 'd4ec_2',
             'd5temp_2', 'd5moisture_2', 'd5ec_2',
             'd6temp_2', 'd6moisture_2', 'd6ec_2',
             'd7temp_2', 'd7moisture_2', 'd7ec_2']]
    p2.columns = p1.columns
    return {'1': p1, '2': p2}


def process6(fn):
    sm = pd.read_excel('Maass soil moisture.xlsx', sheet_name=None)
    sm = pd.concat(sm.values())
    sm.columns = ['valid', 'd1moisture', 'd2moisture', 'd3moisture',
                  'd4moisture', 'd5moisture']
    sm = sm.set_index('valid')

    st = pd.read_excel('Maass soil temperature.xlsx', skiprows=[1,],
                       sheet_name=None)
    st = pd.concat(st.values())
    st.columns = ['valid', 'd1temp', 'd2temp', 'd3temp',
                  'd4temp', 'd5temp']
    st = st.set_index('valid')
    df = st.join(sm)
    df = df.reset_index()
    return {'1': df}


def process7(fn):
    df = pd.read_csv(fn, index_col=False)
    p7 = df[df['plotID'] == 7]
    p8 = df[df['plotID'] == 8]
    p7 = pd.pivot_table(p7, values='SM', index='date', columns='depth')
    p7.reset_index(inplace=True)
    p7['date'] = pd.to_datetime(p7['date'])
    p7.columns = ['valid', 'd1moisture', 'd2moisture', 'd3moisture',
                  'd4moisture', 'd5moisture']

    p8 = pd.pivot_table(p8, values='SM', index='date', columns='depth')
    p8.reset_index(inplace=True)
    p8['date'] = pd.to_datetime(p8['date'])
    p8.columns = ['valid', 'd1moisture', 'd2moisture', 'd3moisture',
                  'd4moisture', 'd5moisture']

    return {'7': p7, '8': p8}


def database_save(uniqueid, plot, df):
    pgconn = get_dbconn('td')
    cursor = pgconn.cursor()
    for i, row in df.iterrows():
        if not isinstance(row['valid'], datetime.datetime) or pd.isnull(row['valid']):
            print('Row df.index=%s, valid=%s, culling' % (i, row['valid']))
            df.drop(i, inplace=True)
    minvalid = df['valid'].min()
    maxvalid = df['valid'].max()
    print("Time Domain: %s - %s" % (minvalid, maxvalid))

    tzoff = "05" if uniqueid not in CENTRAL_TIME else "06"
    cursor.execute("""
        DELETE from decagon_data WHERE
        uniqueid = %s and plotid = %s and valid >= %s and valid <= %s
    """, (uniqueid, plot, minvalid.strftime("%Y-%m-%d %H:%M-"+tzoff),
          maxvalid.strftime("%Y-%m-%d %H:%M-"+tzoff)))
    if cursor.rowcount > 0:
        print("DELETED %s rows previously saved!" % (cursor.rowcount, ))
        if minvalid.year < 2011 or maxvalid.year > 2018:
            print("Aborting, due to valid bounds outside of domain")
            sys.exit()

    def v(row, name):
        val = row.get(name)
        if val is None:
            return 'null'
        if isinstance(val, (str, unicode)):
            if val.strip().lower() in ['nan', '-999']:
                return 'null'
            return val
        elif isinstance(val, pd.core.series.Series):
            print(val)
            print(row)
            sys.exit()
        try:
            if pd.isnull(val):
                return 'null'
        except Exception, exp:
            print(exp)
            print(('Plot: %s Val: %s[%s] Name: %s Valid: %s'
                   ) % (plot, val, type(val), name, row['valid']))
            # sys.exit()
            return 'null'
        return val
    for _, row in df.iterrows():
        cursor.execute("""
        INSERT into decagon_data(uniqueid, plotid, valid, d1moisture, d1temp,
        d1ec, d2moisture, d2temp, d3moisture, d3temp, d4moisture, d4temp,
        d5moisture, d5temp, d1moisture_qc, d1temp_qc,
        d1ec_qc, d2moisture_qc, d2temp_qc, d3moisture_qc, d3temp_qc,
        d4moisture_qc, d4temp_qc,
        d5moisture_qc, d5temp_qc,
        d6moisture_qc, d6temp_qc,
        d7moisture_qc, d7temp_qc) VALUES ('%s', '%s', '%s', %s, %s,
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s)
        """ % (uniqueid, plot, row['valid'].strftime("%Y-%m-%d %H:%M-"+tzoff),
               v(row, 'd1moisture'), v(row, 'd1temp'), v(row, 'd1ec'),
               v(row, 'd2moisture'), v(row, 'd2temp'),
               v(row, 'd3moisture'), v(row, 'd3temp'),
               v(row, 'd4moisture'), v(row, 'd4temp'),
               v(row, 'd5moisture'), v(row, 'd5temp'),
               v(row, 'd1moisture'), v(row, 'd1temp'), v(row, 'd1ec'),
               v(row, 'd2moisture'), v(row, 'd2temp'),
               v(row, 'd3moisture'), v(row, 'd3temp'),
               v(row, 'd4moisture'), v(row, 'd4temp'),
               v(row, 'd5moisture'), v(row, 'd5temp'),
               v(row, 'd6moisture'), v(row, 'd6temp'),
               v(row, 'd7moisture'), v(row, 'd7temp')))

    cursor.close()
    pgconn.commit()
    pgconn.close()


def main(argv):
    """Process a file into the database, please!"""
    fmt = argv[1]
    fn = argv[2]
    uniqueid = argv[3]
    plot = argv[4]
    if fmt == '0':
        df = process0(fn)
    elif fmt == '1':
        df = process1(fn)
    elif fmt == '2':
        df = process2(fn)
    elif fmt == '3':
        df = process3(fn)
    elif fmt == '4':
        df = process4(fn)
    elif fmt == '5':
        df = process5(fn)
    elif fmt == '6':
        df = process6(fn)
    elif fmt == '7':
        df = process7(fn)
    if isinstance(df, dict):
        for plot in df:
            print(("File: %s[%s] found: %s lines for columns %s"
                   ) % (fn, plot, len(df[plot].index),
                        df[plot].columns))
            database_save(uniqueid, plot, df[plot])
    else:
        print(("File: %s found: %s lines for columns %s"
               ) % (fn, len(df.index), df.columns))
        database_save(uniqueid, plot, df)

if __name__ == '__main__':
    main(sys.argv)
