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
import os
from common import CODES, getColor
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
    pgconn = psycopg2.connect(database='td', host='iemdb',
                              user='nobody')
    (uniqueid, plotid) = form.getfirst('site', 'ISUAG::302E').split("::")

    sts = datetime.datetime.strptime(form.getfirst('date', '2014-01-01'),
                                     '%Y-%m-%d')
    days = int(form.getfirst('days', 1))
    group = int(form.getfirst('group', 0))
    ets = sts + datetime.timedelta(days=days)
    tzname = 'America/Chicago' if uniqueid in [
        'ISUAG', 'SERF', 'GILMORE'] else 'America/New_York'
    viewopt = form.getfirst('view', 'plot')
    ptype = form.getfirst('ptype', '1')
    if ptype == '1':
        df = read_sql("""SELECT valid at time zone 'UTC' as v, plotid,
        wat20 as load
        from nitrateload_data WHERE uniqueid = %s
        and valid between %s and %s ORDER by valid ASC
        """, pgconn, params=(uniqueid, sts.date(), ets.date()))
    elif ptype == '2':
        df = read_sql("""SELECT
        date_trunc('month', valid at time zone 'UTC') as v, plotid,
        sum(wat20) as load
        from nitrateload_data WHERE uniqueid = %s
        and valid between %s and %s GROUP by v, plotid ORDER by v ASC
        """, pgconn, params=(uniqueid, sts.date(), ets.date()))
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
    if ptype not in ['2', ]:
        df['v'] = df['v'].apply(
            lambda x: x.tz_localize('UTC').tz_convert(tzname))

    if viewopt not in ['plot', 'js']:
        df.rename(columns=dict(v='timestamp',
                               load='Load (kg ha-1)'
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
    title = ("Nitrate Load for Site: %s (%s to %s)"
             ) % (uniqueid, sts.strftime("%-d %b %Y"),
                  ets.strftime("%-d %b %Y"))
    s = []
    plot_ids = df[linecol].unique()
    plot_ids.sort()
    df['ticks'] = df['v'].astype(np.int64) // 10 ** 6
    seriestype = 'line' if ptype == '1' else 'column'
    for i, plotid in enumerate(plot_ids):
        df2 = df[df[linecol] == plotid]
        s.append(("""{type: '""" + seriestype + """',
            """ + getColor(plotid, i) + """,
            name: '""" + CODES.get(plotid, plotid) + """',
            data: """ + str([[a, b] for a, b in zip(df2['ticks'].values,
                                                    df2['load'].values)]) + """
        }""").replace("None", "null").replace("nan", "null"))
    series = ",".join(s)
    sys.stdout.write("""
$("#hc").highcharts({
    title: {text: '"""+title+"""'},
    chart: {zoomType: 'x'},
    yAxis: {title: {text: 'Load (kg ha-1)'}
    },
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
        valueDecimals: 4,
        valueSuffix: ' kg ha-1'
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
