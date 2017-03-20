#!/usr/bin/env python
"""Plot!"""
import unittest
import psycopg2
import matplotlib
import sys
import cStringIO
import pandas as pd
from pandas.io.sql import read_sql
import cgi
import datetime
import os
import numpy as np
matplotlib.use('agg')
import matplotlib.pyplot as plt  # NOPEP8

LINESTYLE = ['-', '-', '-', '-', '-', '-',
             '-', '-', '-.', '-.', '-.', '-.', '-.',
             '-', '-.', '-.', '-.', '-.', '-.', '-.', '-.', '-.', '-.', '-.']

CODES = {'UD': 'Undarined (No Drainage)',
         'FD': 'Free Drainage (Conventional Drainage)',
         'CD': 'Controlled Drainage (Managed Drainage)',
         'SD': 'Surface Drainage',
         'SH': 'Shallow Drainage',
         'SI': 'Controlled Drainage with Subirrigation',
         'CA': 'Automated Controlled Drainage',
         'SB': 'Saturated Buffer',
         'TBD': 'To Be Determined',
         'n/a': 'Not Available or Not Applicable'}


def send_error(viewopt, msg):
    """" """
    if viewopt == 'js':
        sys.stdout.write("Content-type: application/javascript\n\n")
        sys.stdout.write("alert('No data found, sorry');")
        sys.exit()
    fig, ax = plt.subplots(1, 1)
    ax.text(0.5, 0.5, msg, transform=ax.transAxes, ha='center')
    sys.stdout.write("Content-type: image/png\n\n")
    ram = cStringIO.StringIO()
    fig.savefig(ram, format='png')
    ram.seek(0)
    sys.stdout.write(ram.read())
    sys.exit()


def get_weather(pgconn, uniqueid, sts, ets):
    """Retreive the daily precipitation"""
    # Convert ids
    dbid = uniqueid
    if uniqueid not in ['SERF_IA', 'SERF_SD', 'DEFI_R']:
        dbid = uniqueid.split("_")[0]
    df = read_sql("""SELECT valid, precip_mm from weather_daily
    WHERE siteid = %s and valid >= %s and valid <= %s ORDER by valid ASC
    """, pgconn, index_col='valid', params=(dbid, sts.date(),
                                            ets.date()))
    df.index = pd.DatetimeIndex(df.index.values)
    df['ticks'] = df.index.values.astype('datetime64[ns]').astype(
        np.int64) // 10 ** 6
    return df


def make_plot(form):
    """Make the plot"""
    pgconn = psycopg2.connect(database='td', host='iemdb',
                              user='nobody')
    (uniqueid, plotid) = form.getfirst('site', 'ISUAG::302E').split("::")

    sts = datetime.datetime.strptime(form.getfirst('date', '2014-01-01'),
                                     '%Y-%m-%d')
    days = int(form.getfirst('days', 1))
    group = int(form.getfirst('group', 0))
    ets = sts + datetime.timedelta(days=days)
    wxdf = get_weather(pgconn, uniqueid, sts, ets)
    tzname = 'America/Chicago' if uniqueid in [
        'ISUAG', 'SERF', 'GILMORE'] else 'America/New_York'
    viewopt = form.getfirst('view', 'plot')
    ptype = form.getfirst('ptype', '1')
    if ptype == '1':
        df = read_sql("""SELECT valid at time zone 'UTC' as v, plotid,
        discharge_mm_qc as discharge,
        coalesce(discharge_mm_qcflag, '') as discharge_f
        from tileflow_data WHERE uniqueid = %s
        and valid between %s and %s ORDER by valid ASC
        """, pgconn, params=(uniqueid, sts.date(), ets.date()))
    elif ptype == '2':
        # resample the weather data
        wxdf = wxdf.resample('M', loffset=datetime.timedelta(days=-27)).sum()
        wxdf['ticks'] = wxdf.index.values.astype('datetime64[ns]').astype(
            np.int64) // 10 ** 6
        df = read_sql("""SELECT
        date_trunc('month', valid at time zone 'UTC') as v, plotid,
        sum(discharge_mm_qc) as discharge
        from tileflow_data WHERE uniqueid = %s
        and valid between %s and %s GROUP by v, plotid ORDER by v ASC
        """, pgconn, params=(uniqueid, sts.date(), ets.date()))
        df["discharge_f"] = '-'
    elif ptype == '3':
        # Daily Aggregate
        df = read_sql("""SELECT
        date_trunc('day', valid at time zone 'UTC') as v, plotid,
        sum(discharge_mm_qc) as discharge
        from tileflow_data WHERE uniqueid = %s
        and valid between %s and %s GROUP by v, plotid ORDER by v ASC
        """, pgconn, params=(uniqueid, sts.date(), ets.date()))
        df["discharge_f"] = '-'
    if len(df.index) < 3:
        send_error(viewopt, "No / Not Enough Data Found, sorry!")
    linecol = 'plotid'
    if group == 1:
        # Generate the plotid lookup table
        plotdf = read_sql("""
            SELECT * from plotids where siteid = %s
        """, pgconn, params=(uniqueid, ), index_col='plotid')

        def lookup(row):
            return plotdf.loc[row['plotid'], "y%s" % (row['v'].year, )]
        df['treatment'] = df.apply(lambda row: lookup(row), axis=1)
        del df['plotid']
        df = df.groupby(['treatment', 'v']).mean()
        df.reset_index(inplace=True)
        linecol = 'treatment'
    if ptype not in ['2', '3']:
        df['v'] = df['v'].apply(
            lambda x: x.tz_localize('UTC').tz_convert(tzname))

    if viewopt not in ['plot', 'js']:
        df.rename(columns=dict(v='timestamp',
                               discharge='Discharge (mm)'
                               ),
                  inplace=True)
        if viewopt == 'html':
            sys.stdout.write("Content-type: text/html\n\n")
            sys.stdout.write(df.to_html(index=False))
            return
        if viewopt == 'csv':
            sys.stdout.write('Content-type: application/octet-stream\n')
            sys.stdout.write(('Content-Disposition: attachment; '
                              'filename=%s_%s_%s_%s.csv\n\n'
                              ) % (uniqueid, plotid, sts.strftime("%Y%m%d"),
                                   ets.strftime("%Y%m%d")))
            sys.stdout.write(df.to_csv(index=False))
            return
        if viewopt == 'excel':
            sys.stdout.write('Content-type: application/octet-stream\n')
            sys.stdout.write(('Content-Disposition: attachment; '
                              'filename=%s_%s_%s_%s.xlsx\n\n'
                              ) % (uniqueid, plotid, sts.strftime("%Y%m%d"),
                                   ets.strftime("%Y%m%d")))
            writer = pd.ExcelWriter('/tmp/ss.xlsx')
            df.to_excel(writer, 'Data', index=False)
            writer.save()
            sys.stdout.write(open('/tmp/ss.xlsx', 'rb').read())
            os.unlink('/tmp/ss.xlsx')
            return

    # Begin highcharts output
    sys.stdout.write("Content-type: application/javascript\n\n")
    title = ("Tile Flow for Site: %s (%s to %s)"
             ) % (uniqueid, sts.strftime("%-d %b %Y"),
                  ets.strftime("%-d %b %Y"))
    s = []
    plot_ids = df[linecol].unique()
    plot_ids.sort()
    df['ticks'] = df['v'].astype(np.int64) // 10 ** 6
    seriestype = 'line' if ptype in ['1', '3'] else 'column'
    for plotid in plot_ids:
        df2 = df[df[linecol] == plotid]
        s.append(("""{type: '""" + seriestype + """',
            name: '""" + CODES.get(plotid, plotid) + """',
            data: """ + str([[a, b] for a, b in zip(df2['ticks'].values,
                                                    df2['discharge'].values)]) + """
        }""").replace("None", "null").replace("nan", "null"))
    if len(wxdf.index) > 0:
        s.append(("""{type: 'column',
            name: 'Precip',
            color: '#0000ff',
            yAxis: 1,
            data: """ + str([[a, b] for a, b in zip(wxdf['ticks'].values,
                                                    wxdf['precip_mm'].values)]) + """
        }""").replace("None", "null").replace("nan", "null"))
    series = ",".join(s)
    sys.stdout.write("""
$("#hc").highcharts({
    title: {text: '"""+title+"""'},
    chart: {zoomType: 'x'},
    yAxis: [
        {title: {text: 'Discharge (mm)'}},
        {title: {text: 'Daily Precipitation (mm)'},
         reversed: true,
         maxPadding: 1,
         opposite: true},
    ],
    plotOptions: {line: {turboThreshold: 0}},
    xAxis: {
        type: 'datetime'
    },
    tooltip: {
        dateTimeLabelFormats: {
            hour: "%b %e %Y, %H:%M",
            minute: "%b %e %Y, %H:%M"
        },
        shared: true,
        valueDecimals: 0,
        valueSuffix: ' mm'
    },
    series: ["""+series+"""]
});
    """)


def main():
    """Do Something"""
    form = cgi.FieldStorage()
    make_plot(form)

if __name__ == '__main__':
    main()


class TestCase(unittest.TestCase):

    def test_wx(self):
        pgconn = psycopg2.connect(database='td', host='iemdb',
                                  user='nobody')
        wxdf = get_weather(pgconn, 'SERF_IA',
                           datetime.datetime(2015, 1, 1),
                           datetime.datetime(2016, 1, 1))
        print wxdf.resample('M', loffset=datetime.timedelta(days=15)).sum()
        print wxdf.index
        self.assertEquals(len(wxdf.index), 0)
