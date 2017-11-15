#!/usr/bin/env python
"""Plot!"""
import psycopg2
import matplotlib
import sys
import cStringIO
import pandas as pd
from pandas.io.sql import read_sql
import cgi
import datetime
import pytz
import os
import numpy as np
matplotlib.use('agg')
import matplotlib.pyplot as plt  # NOPEP8

LINESTYLE = ['-', '-', '-', '-', '-', '-',
             '-', '-', '-.', '-.', '-.', '-.', '-.',
             '-', '-.', '-.', '-.', '-.', '-.', '-.', '-.', '-.', '-.', '-.']


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


def make_plot(form):
    """Make the plot"""
    uniqueid = form.getfirst('site', 'ISUAG')

    sts = datetime.datetime.strptime(form.getfirst('date', '2014-01-01'),
                                     '%Y-%m-%d')
    days = int(form.getfirst('days', 1))
    ets = sts + datetime.timedelta(days=days)
    pgconn = psycopg2.connect(database='sustainablecorn', host='iemdb',
                              user='nobody')
    tzname = 'America/Chicago' if uniqueid in [
        'ISUAG', 'SERF', 'GILMORE'] else 'America/New_York'
    viewopt = form.getfirst('view', 'plot')
    ptype = form.getfirst('ptype', '1')
    if ptype == '1':
        df = read_sql("""SELECT uniqueid, plotid, valid at time zone 'UTC' as v, 
        depth_mm_qc as depth
        from watertable_data WHERE uniqueid = %s
        and valid between %s and %s ORDER by valid ASC
        """, pgconn, params=(uniqueid, sts.date(), ets.date()))
    elif ptype in ['3', '4']:
        res = 'hour' if ptype == '3' else 'week'
        df = read_sql("""SELECT uniqueid, plotid,
        date_trunc('"""+res+"""', valid at time zone 'UTC') as v,
        avg(depth_mm_qc) as depth
        from watertable_data WHERE uniqueid = %s
        and valid between %s and %s GROUP by v, plotid ORDER by v ASC
        """, pgconn, params=(uniqueid, sts.date(), ets.date()))
    else:
        df = read_sql("""SELECT uniqueid, plotid, date(valid at time zone %s) as v,
        avg(depth_mm_qc) as depth
        from watertable_data WHERE uniqueid = %s
        and valid between %s and %s GROUP by v, plotid ORDER by v ASC
        """, pgconn, params=(tzname, uniqueid, sts.date(), ets.date()))
    if len(df.index) < 3:
        send_error(viewopt, "No / Not Enough Data Found, sorry!")
    if ptype not in ['2', ]:
        df['v'] = df['v'].apply(
            lambda x: x.tz_localize('UTC').tz_convert(tzname))

    if viewopt not in ['plot', 'js']:
        df.rename(columns=dict(v='timestamp',
                               depth='Depth (mm)'
                               ),
                  inplace=True)
        if viewopt == 'html':
            sys.stdout.write("Content-type: text/html\n\n")
            sys.stdout.write(df.to_html(index=False))
            return
        if viewopt == 'csv':
            sys.stdout.write('Content-type: application/octet-stream\n')
            sys.stdout.write(('Content-Disposition: attachment; '
                              'filename=%s_%s_%s.csv\n\n'
                              ) % (uniqueid, sts.strftime("%Y%m%d"),
                                   ets.strftime("%Y%m%d")))
            sys.stdout.write(df.to_csv(index=False))
            return
        if viewopt == 'excel':
            sys.stdout.write('Content-type: application/octet-stream\n')
            sys.stdout.write(('Content-Disposition: attachment; '
                              'filename=%s_%s_%s.xlsx\n\n'
                              ) % (uniqueid, sts.strftime("%Y%m%d"),
                                   ets.strftime("%Y%m%d")))
            writer = pd.ExcelWriter('/tmp/ss.xlsx',
                                    options={'remove_timezone': True})
            df.to_excel(writer, 'Data', index=False)
            writer.save()
            sys.stdout.write(open('/tmp/ss.xlsx', 'rb').read())
            os.unlink('/tmp/ss.xlsx')
            return

    # Begin highcharts output
    sys.stdout.write("Content-type: application/javascript\n\n")
    title = ("Water Table Depth for Site: %s (%s to %s)"
             ) % (uniqueid, sts.strftime("%-d %b %Y"),
                  ets.strftime("%-d %b %Y"))
    s = []
    plot_ids = df['plotid'].unique()
    plot_ids.sort()
    df['ticks'] = pd.to_datetime(df['v']).astype(np.int64) // 10 ** 6
    for plotid in plot_ids:
        df2 = df[df['plotid'] == plotid]
        v = df2[['ticks', 'depth']].to_json(orient='values')
        s.append("""{
            name: '"""+plotid+"""',
            data: """ + v + """
        }""")
    series = ",".join(s)
    sys.stdout.write("""
$("#hc").highcharts({
    title: {text: '"""+title+"""'},
    chart: {zoomType: 'x'},
    yAxis: {title: {text: 'Depth below ground (mm)'},
        reversed: true
    },
    plotOptions: {line: {turboThreshold: 0},
        series: {
            allowPointSelect: true,
            cursor: 'pointer',
            point: {
                events: {
                    click: function () {
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
