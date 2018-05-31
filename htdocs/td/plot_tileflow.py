#!/usr/bin/env python
"""Plot!"""
import unittest
import sys
from io import BytesIO
import cgi
import datetime
import os

import pandas as pd
from pandas.io.sql import read_sql
import numpy as np
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
from pyiem.util import get_dbconn, ssw
from common import CODES, getColor, ERRMSG

LINESTYLE = ['-', '-', '-', '-', '-', '-',
             '-', '-', '-.', '-.', '-.', '-.', '-.',
             '-', '-.', '-.', '-.', '-.', '-.', '-.', '-.', '-.', '-.', '-.']


def send_error(viewopt, msg):
    """" """
    if viewopt == 'js':
        ssw("Content-type: application/javascript\n\n")
        ssw("alert('"+ERRMSG+"');")
        sys.exit()
    fig, ax = plt.subplots(1, 1)
    ax.text(0.5, 0.5, msg, transform=ax.transAxes, ha='center')
    ssw("Content-type: image/png\n\n")
    ram = BytesIO()
    fig.savefig(ram, format='png')
    ram.seek(0)
    ssw(ram.read())
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
    pgconn = get_dbconn('td')
    uniqueid = form.getfirst('site', 'ISUAG')

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
        if len(wxdf.index) > 0:
            wxdf = wxdf.resample('M',
                                 loffset=datetime.timedelta(days=-27)).sum()
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
            try:
                return plotdf.loc[row['plotid'], "y%s" % (row['v'].year, )]
            except KeyError:
                return row['plotid']
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
            ssw("Content-type: text/html\n\n")
            ssw(df.to_html(index=False))
            return
        if viewopt == 'csv':
            ssw('Content-type: application/octet-stream\n')
            ssw(('Content-Disposition: attachment; '
                              'filename=%s_%s_%s.csv\n\n'
                              ) % (uniqueid, sts.strftime("%Y%m%d"),
                                   ets.strftime("%Y%m%d")))
            ssw(df.to_csv(index=False))
            return
        if viewopt == 'excel':
            ssw('Content-type: application/octet-stream\n')
            ssw(('Content-Disposition: attachment; '
                              'filename=%s_%s_%s.xlsx\n\n'
                              ) % (uniqueid, sts.strftime("%Y%m%d"),
                                   ets.strftime("%Y%m%d")))
            writer = pd.ExcelWriter('/tmp/ss.xlsx',
                                    options={'remove_timezone': True})
            df.to_excel(writer, 'Data', index=False)
            writer.save()
            ssw(open('/tmp/ss.xlsx', 'rb').read())
            os.unlink('/tmp/ss.xlsx')
            return

    # Begin highcharts output
    ssw("Content-type: application/javascript\n\n")
    title = ("Tile Flow for Site: %s (%s to %s)"
             ) % (uniqueid, sts.strftime("%-d %b %Y"),
                  ets.strftime("%-d %b %Y"))
    s = []
    plot_ids = df[linecol].unique()
    plot_ids.sort()
    if group == '1':
        plot_ids = plot_ids[::-1]
    df['ticks'] = df['v'].astype(np.int64) // 10 ** 6
    seriestype = 'line' if ptype in ['1', '3'] else 'column'
    for i, plotid in enumerate(plot_ids):
        df2 = df[df[linecol] == plotid]
        s.append(("""{type: '""" + seriestype + """',
            """ + getColor(plotid, i) + """,
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
    ssw("""
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
    plotOptions: {
        line: {turboThreshold: 0},
        series: {
            cursor: 'pointer',
            allowPointSelect: true,
            point: {
                events: {
                    click: function() {
                        editPoint(this);
                    }
                }
            }
        }
    },
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
        pgconn = get_dbconn('td')
        wxdf = get_weather(pgconn, 'CLAY_C',
                           datetime.datetime(2015, 4, 20),
                           datetime.datetime(2016, 1, 1))
        self.assertEquals(len(wxdf.index), 0)
