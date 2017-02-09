"""Ingest of TD Project Weather Data

See how I am called:
    python ignest.py filename_or_googleid siteid format

Assumptions:
    1. Timestamps are in local standard time
"""
import sys
import pandas as pd
import psycopg2
import datetime
from pyiem.datatypes import speed
import pyiem.cscap_utils as util

XREF = {'precipmm': 'precip_mm',
        'CLIM12': 'precip_mm',
        'Rain_mm_Tot': 'precip_mm',
        'radwm2': 'srad_wm2',
        'rh': 'relhum_percent',
        'CLIM18': 'relhum_percent',
        'CLIM19': 'srad_wms',
        'CLIM16': 'winddir_deg',
        'CLIM17': 'windgust_mps',
        'CLIM15': 'windspeed_mps',
        'tmpc': 'airtemp_c',
        'AIRTEMPC': 'airtemp_c',
        'AirTC_Avg': 'airtemp_c',
        'wmps': 'windspeed_mps',
        'DATE&TIME': 'valid',
        'TIMESTAMP': 'valid'}
TZREF = {'ACRE': 5,
         'DPAC': 5,
         'DEFI_R': 5,
         'FULTON': 5,
         'VANWERT': 5,
         'MAASS': 6,
         'BEAR': 6}


def fmt(val):
    if val is None:
        return val
    if val in ['M', 'nd']:
        return None
    return val


def googlesheet(siteid, sheetkey):
    """Harvest a google sheet, please"""
    rows = []
    config = util.get_config()
    sheets = util.get_sheetsclient(config, "td")
    f = sheets.spreadsheets().get(spreadsheetId=sheetkey, includeGridData=True)
    j = util.exponential_backoff(f.execute)
    for sheet in j['sheets']:
        # sheet_title = sheet['properties']['title']
        for griddata in sheet['data']:
            for row, rowdata in enumerate(griddata['rowData']):
                if 'values' not in rowdata:  # empty sheet
                    continue
                if row == 1:  # skip units
                    continue
                if row == 0:
                    header = []
                    for col, celldata in enumerate(rowdata['values']):
                        header.append(celldata['formattedValue'])
                    continue
                data = {}
                for col, celldata in enumerate(rowdata['values']):
                    data[header[col]] = fmt(celldata.get('formattedValue'))
                rows.append(data)
    df = pd.DataFrame(rows)
    print("googlesheet has columns: %s" % (repr(df.columns.values),))
    newcols = {}
    for k in df.columns:
        newcols[k] = XREF.get(k, k)
    df.rename(columns=newcols, inplace=True)
    df['valid'] = pd.to_datetime(df['valid'], errors='raise',
                                 format='%m/%d/%y %H:%M')
    df['valid'] = df['valid'] + datetime.timedelta(hours=TZREF[siteid])

    # do some conversions
    print("ALERT: doing windspeed unit conv")
    df['windspeed_mps'] = speed(df['windspeed_mps'].values, 'KMH').value('MPS')
    print("ALERT: doing windgustunit conv")
    df['windgust_mps'] = speed(df['windgust_mps'].values, 'KMH').value('MPS')
    return df


def read_excel(siteid, fn):
    df = pd.read_excel(fn, skiprows=[1, ])
    newcols = {}
    for k in df.columns:
        newcols[k] = XREF.get(k, k)
    df.rename(columns=newcols, inplace=True)
    df['valid'] = df['valid'] + datetime.timedelta(hours=TZREF[siteid])
    # do some conversions
    print("ALERT: doing windspeed unit conv")
    df['windspeed_mps'] = speed(df['windspeed_mps'].values, 'KMH').value('MPS')
    print("ALERT: doing windgustunit conv")
    df['windgust_mps'] = speed(df['windgust_mps'].values, 'KMH').value('MPS')
    return df


def save(siteid, df):
    pgconn = psycopg2.connect(database='td')
    cursor = pgconn.cursor()
    print(("Saving %s entries %s->%s"
           ) % (len(df.index), df['valid'].min().strftime("%Y%m%d%H%M"),
                df['valid'].max().strftime("%Y%m%d%H%M")))
    cursor.execute("SET TIME ZONE 'UTC'")
    cursor.execute("""
        DELETE from weather_observations where siteid = %s
        and valid >= %s and valid <= %s
        """, (siteid, df['valid'].min(), df['valid'].max()))
    if cursor.rowcount > 0:
        print("  -> found previous data, deleted %s rows" % (cursor.rowcount,))
    for _, row in df.iterrows():
        if row['valid'] is pd.NaT:
            continue
        cursor.execute("""
        INSERT into weather_observations(siteid, valid, precip_mm, srad_wm2,
        relhum_percent, airtemp_c, windspeed_mps, srad_wms,
        winddir_deg, windgust_mps) VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (siteid, row['valid'], row.get('precip_mm'),
              row.get('srad_wm2'), row.get('relhum_percent'),
              row.get('airtemp_c'), row.get('windspeed_mps'),
              row.get('srad_wms'), row.get('winddir_deg'),
              row.get('windgust_mps')))

    cursor.close()
    pgconn.commit()


def read_csv(siteid, fn):
    df = pd.read_csv(fn)
    df['valid'] = pd.to_datetime({'year': df['year'], 'month': df['mo'],
                                  'day': df['dy'], 'hour': df['hr']})
    df['valid'] = df['valid'] + datetime.timedelta(hours=TZREF[siteid])
    newcols = {}
    for k in df.columns:
        newcols[k] = XREF.get(k, k)
    df.rename(columns=newcols, inplace=True)
    return df


def main(argv):
    """Do Something!"""
    (fn, siteid, fmt) = argv[1:]
    if fmt == "csv":
        df = read_csv(siteid, fn)
    elif fmt == "googlesheet":
        df = googlesheet(siteid, fn)
    elif fmt == "excel":
        df = read_excel(siteid, fn)
    save(siteid, df)

if __name__ == '__main__':
    main(sys.argv)
