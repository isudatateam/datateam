#!/usr/bin/env python
"""Plot!"""
import sys
import cStringIO
import cgi
import os
import pandas as pd
from pandas.io.sql import read_sql
import psycopg2
import numpy as np
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt  # NOPEP8

VARDICT = {'WAT2': {'title': 'Nitrate-N Concentration', 'units': 'mg N / L'},
           'WAT9': {'title': 'Soluble Reactive Phosphorus Concentration',
                    'units': 'ug P / L'}}


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
    uniqueid = form.getfirst('site', 'ISUAG').split("::")[0]

    pgconn = psycopg2.connect(database='sustainablecorn', host='iemdb',
                              user='nobody')
    viewopt = form.getfirst('view', 'plot')
    varname = form.getfirst('varname', 'WAT2')
    df = read_sql("""SELECT valid at time zone 'UTC' as v, plotid,
    value
    from waterquality_data WHERE uniqueid = %s
    and varname = %s ORDER by valid ASC
    """, pgconn, params=(uniqueid, varname))

    if viewopt not in ['plot', 'js']:
        df.rename(columns=dict(v='timestamp',
                               value=varname
                               ),
                  inplace=True)
        if viewopt == 'html':
            sys.stdout.write("Content-type: text/html\n\n")
            sys.stdout.write(df.to_html(index=False))
            return
        if viewopt == 'csv':
            sys.stdout.write('Content-type: application/octet-stream\n')
            sys.stdout.write(('Content-Disposition: attachment; '
                              'filename=%s.csv\n\n'
                              ) % (uniqueid, ))
            sys.stdout.write(df.to_csv(index=False))
            return
        if viewopt == 'excel':
            sys.stdout.write('Content-type: application/octet-stream\n')
            sys.stdout.write(('Content-Disposition: attachment; '
                              'filename=%s.xlsx\n\n'
                              ) % (uniqueid, ))
            writer = pd.ExcelWriter('/tmp/ss.xlsx',
                                    options={'remove_timezone': True})
            df.to_excel(writer, 'Data', index=False)
            writer.save()
            sys.stdout.write(open('/tmp/ss.xlsx', 'rb').read())
            os.unlink('/tmp/ss.xlsx')
            return

    # Begin highcharts output
    sys.stdout.write("Content-type: application/javascript\n\n")
    title = ("Water Quality for Site: %s"
             ) % (uniqueid, )
    splots = []
    plot_ids = df['plotid'].unique()
    plot_ids.sort()
    df['ticks'] = df['v'].astype(np.int64) // 10 ** 6
    for plotid in plot_ids:
        df2 = df[df['plotid'] == plotid]
        splots.append(("""{type: 'scatter',
            name: '"""+plotid+"""',
            data: """ + str([[a, b] for a, b in zip(df2['ticks'].values,
                                                    df2['value'].values)]) + """
        }""").replace("None", "null").replace("nan", "null"))
    series = ",".join(splots)
    sys.stdout.write("""
$("#hc").highcharts({
    title: {text: '"""+title+"""'},
    chart: {zoomType: 'x'},
    yAxis: {title: {text: '""" + VARDICT[varname]["title"], VARDICT[varname]["units"] + """'}
    },
    plotOptions: {line: {turboThreshold: 0}
    },
    xAxis: {
        type: 'datetime'
    },
    tooltip: {
        pointFormat: 'date: <b>{point.x:%b %e %Y, %H:%M}</b><br/>value: <b>{point.y}</b><br/>',
        shared: true,
        valueDecimals: 2,
        valueSuffix: '""" + VARDICT[varname]["units"] + """'
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
