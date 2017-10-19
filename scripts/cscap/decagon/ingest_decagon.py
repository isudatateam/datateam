"""Process the decagon data"""
from __future__ import print_function
import sys
import os
import glob
import datetime

import pandas as pd
import psycopg2

CENTRAL_TIME = ['ISUAG', 'ISUAG.USB', 'GILMORE', 'SERF', 'HICKS.B', 'HICKS.G']


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
            if colname.find('Bulk EC') > 0:
                name += "ec"
            elif colname.find('VWC') > 0:
                name += 'moisture'
            else:
                name += "temp"

        if name is not None:
            x[colname] = name

    df.rename(columns=x, inplace=True)


def process1(fn, timefmt='%m/%d/%y %H:%M'):
    df = pd.read_table(fn, sep='\t', index_col=False, low_memory=False)
    df['valid'] = df['Measurement Time'].apply(
        lambda s: datetime.datetime.strptime(s.strip(), timefmt))
    df.drop('Measurement Time', axis=1, inplace=True)
    translate(df)
    return df


def process4(fn):
    """See MASON for an example"""
    df = pd.read_excel(fn, index_col=False, skiprows=[0, 1, 3, 4, 5])
    x = {'20': 'd2', '40': 'd3', '60': 'd4', '80': 'd5'}
    extract = {}
    for col in df.columns[1:]:
        plotid = col[:-3]
        depth = col[-2:]
        e = extract.setdefault(plotid, [df.columns[0]])
        e.append(col)
    print(extract)
    mydict = {}
    for plotid in extract.keys():
        mydict[plotid] = df[extract[plotid]].copy()
        rename = {df.columns[0]: 'valid'}
        for col in mydict[plotid].columns[1:]:
            depth = col[-2:]
            rename[col] = "%smoisture" % (x[depth], )
        mydict[plotid].rename(columns=rename, inplace=True)
        print(mydict[plotid].columns)
    return mydict


def process3(fn):
    """SERF"""
    mydict = pd.read_excel(fn, sheetname=None, index_col=False)
    # Need to load up rows 0 and 1 into the column names
    for sheetname in mydict:
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


def process5(uniqueid, dirname):
    """Look through a directory of XL files"""
    os.chdir(dirname)
    files = glob.glob("*.xlsx")
    files.sort()
    for fn in files:
        plotid = fn.split()[0]
        df = pd.read_excel(fn, index_col=None)
        row0 = df.iloc[0, :]
        row1 = df.iloc[1, :]
        reg = {df.columns[0]: 'valid'}
        for c, r0, r1 in zip(df.columns, row0, row1):
            reg[c] = "%s %s %s" % (c, r0, r1)
        df.rename(columns=reg, inplace=True)
        df.drop(df.head(2).index, inplace=True)
        translate(df)
        print(("File: %s[%s] found: %s lines for columns %s"
               ) % (fn, plotid, len(df.index), df.columns))
        if len(df.index) > 0:
            database_save(uniqueid, plotid, df)
    return {}


def process6(uniqueid, dirname):
    """HICKS"""
    os.chdir(dirname)
    files = glob.glob("*2015.xlsx")
    files.sort()
    res = {}
    rename = {'Port 1 VWC': 'd1moisture', 'Port 1 Temp': 'd1temp',
              'Port 2 VWC': 'd2moisture', 'Port 2 Temp': 'd2temp',
              'Port 3 VWC': 'd3moisture', 'Port 3 Temp': 'd3temp',
              'Port 4 VWC': 'd4moisture', 'Port 4 Temp': 'd4temp',
              'Port 5 VWC': 'd5moisture', 'Port 5 Temp': 'd5temp'}
    for fn in files:
        plotid = fn.split("_")[1].replace("Field", "")
        thissite = "HICKS.%s" % (plotid[0], )
        plotid = plotid[1]
        print("%s %s %s" % (fn, thissite, plotid))
        df = pd.read_excel(fn, sheetname=None)
        sheets = df.keys()
        df[sheets[0]].set_index('valid', inplace=True)
        df[sheets[1]].set_index('valid', inplace=True)
        print(df[sheets[0]].columns)
        print(df[sheets[1]].columns)
        if 'Port 1 VWC' in df[sheets[0]].columns:
            print('Port 1 in sheet 0')
            ldf = df[sheets[0]].copy()
            two = 1
        else:
            ldf = df[sheets[1]].copy()
            two = 0
        for col in ['Port 3 VWC', 'Port 3 Temp',
                    'Port 4 VWC', 'Port 4 Temp',
                    'Port 5 VWC', 'Port 5 Temp']:
            ldf[col] = df[sheets[two]][col]
        ldf.reset_index(inplace=True)
        ldf.rename(columns=rename, inplace=True)
        ldf.fillna(method='bfill', inplace=True, limit=12)
        ldf['valid'] = pd.to_datetime(ldf['valid'])
        res[plotid] = ldf.copy()
    return res


def process7(uniqueid, fn):
    """Kellogg"""
    df = pd.read_excel(fn)
    x = {}
    plotids = []
    for colname in df.columns:
        if colname == 'valid':
            continue
        depth = colname.split("-")[-1]
        if depth == "20":
            port = 2
        elif depth == '40':
            port = 3
        elif depth == '60':
            port = 4
        elif depth == '80':
            port = 5
        else:
            print(colname)
            sys.exit()
        plotid = "-".join(colname.split("-")[:-1])
        if plotid not in plotids:
            plotids.append(plotid)
        x[colname] = "%s_d%smoisture" % (plotid, port)

    df.rename(columns=x, inplace=True)
    df2 = {}
    for plotid in plotids:
        df2[plotid] = pd.DataFrame(
            {'valid': df['valid'],
             'd2moisture': df['%s_d2moisture' % (plotid, )],
             'd3moisture': df['%s_d3moisture' % (plotid, )],
             'd4moisture': (df['%s_d4moisture' % (plotid, )] if
                            '%s_d4moisture' % (plotid, ) in df.columns else
                            None),
             'd5moisture': (df['%s_d5moisture' % (plotid, )] if
                            '%s_d5moisture' % (plotid, ) in df.columns else
                            None)})

    return df2


def database_save(uniqueid, plot, df):
    pgconn = psycopg2.connect(database='sustainablecorn', host='iemdb')
    cursor = pgconn.cursor()
    for i, row in df.iterrows():
        if not isinstance(row['valid'], datetime.datetime):
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
        if minvalid.year < 2011 or maxvalid.year > 2016:
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
        # elif isinstance(val, pd.core.series.Series):
        #    print val
        #    print row
        #    sys.exit()
        try:
            if pd.isnull(val):
                return 'null'
        except Exception, exp:
            print(exp)
            print(('Plot: %s Val: %s[%s] Name: %s Valid: %s'
                   ) % (plot, val, type(val), name, row['valid']))
            sys.exit()
            return 'null'
        return val
    for _, row in df.iterrows():
        cursor.execute("""
        INSERT into decagon_data(uniqueid, plotid, valid, d1moisture, d1temp,
        d1ec, d2moisture, d2temp, d3moisture, d3temp, d4moisture, d4temp,
        d5moisture, d5temp, d1moisture_qc, d1temp_qc,
        d1ec_qc, d2moisture_qc, d2temp_qc, d3moisture_qc, d3temp_qc,
        d4moisture_qc, d4temp_qc,
        d5moisture_qc, d5temp_qc) VALUES ('%s', '%s', '%s', %s, %s,
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s)
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
               v(row, 'd5moisture'), v(row, 'd5temp')))

    cursor.close()
    pgconn.commit()
    pgconn.close()


def main(argv):
    """Process a file into the database, please!"""
    fmt = argv[1]
    fn = argv[2]
    uniqueid = argv[3]
    plot = argv[4]
    if fmt == '1':
        df = process1(fn)
    elif fmt == '2':
        df = process1(fn, '%m/%d/%y %I:%M %p')
    elif fmt == '3':
        df = process3(fn)
    elif fmt == '4':
        df = process4(fn)
    elif fmt == '5':
        df = process5(uniqueid, fn)
    elif fmt == '6':
        df = process6(uniqueid, fn)
    elif fmt == '7':
        df = process7(uniqueid, fn)
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
