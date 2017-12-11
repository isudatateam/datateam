#!/usr/bin/env python
"""Plot!"""
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
from pyiem.util import get_dbconn

LINESTYLE = ['-', '-', '-', '-', '-', '-',
             '-', '-', '-.', '-.', '-.', '-.', '-.',
             '-', '-.', '-.', '-.', '-.', '-.', '-.', '-.', '-.', '-.', '-.']


def get_vardf(pgconn, tabname):
    """Get a dataframe of descriptors for this tabname"""
    return read_sql("""
        select element_or_value_display_name as varname,
        number_of_decimal_places_to_round_up::numeric::int as round,
        short_description, units from data_dictionary_export WHERE
        spreadsheet_tab = %s
    """, pgconn, params=(tabname, ), index_col='varname')


def add_bling(pgconn, df, tabname):
    """Do fancy things"""
    # Insert some headers rows
    metarows = [{}, {}]
    cols = df.columns
    vardf = get_vardf(pgconn, tabname)
    for i, colname in enumerate(cols):
        if i == 0:
            metarows[0][colname] = 'description'
            metarows[1][colname] = 'units'
            continue
        if colname in vardf.index:
            metarows[0][colname] = vardf.at[colname, 'short_description']
            metarows[1][colname] = vardf.at[colname, 'units']
    df = pd.concat([pd.DataFrame(metarows), df], ignore_index=True)
    # re-establish the correct column sorting
    df = df.reindex_axis(cols, axis=1)
    return df


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

    sts = datetime.datetime.strptime(form.getfirst('date', '2014-01-01'),
                                     '%Y-%m-%d')
    days = int(form.getfirst('days', 1))
    ets = sts + datetime.timedelta(days=days)
    pgconn = get_dbconn('sustainablecorn')
    tzname = 'America/Chicago' if uniqueid in [
        'ISUAG', 'SERF', 'GILMORE'] else 'America/New_York'
    viewopt = form.getfirst('view', 'plot')
    ptype = form.getfirst('ptype', '1')
    if ptype == '1':
        df = read_sql("""SELECT uniqueid, plotid, valid at time zone 'UTC' as v,
        discharge_mm_qc as discharge
        from tileflow_data WHERE uniqueid = %s
        and valid between %s and %s ORDER by valid ASC
        """, pgconn, params=(uniqueid, sts.date(), ets.date()))
    elif ptype == '2':
        df = read_sql("""SELECT uniqueid, plotid,
        date_trunc('month', valid at time zone 'UTC') as v,
        sum(discharge_mm_qc) as discharge
        from tileflow_data WHERE uniqueid = %s
        and valid between %s and %s GROUP by uniqueid, v, plotid ORDER by v ASC
        """, pgconn, params=(uniqueid, sts.date(), ets.date()))
    if len(df.index) < 3:
        send_error(viewopt, "No / Not Enough Data Found, sorry!")
    if ptype not in ['2', ]:
        df['v'] = df['v'].apply(
            lambda x: x.tz_localize('UTC').tz_convert(tzname))

    if viewopt not in ['plot', 'js']:
        df.rename(columns=dict(v='timestamp',
                               discharge='Tile Flow (mm)'
                               ),
                  inplace=True)
        df = add_bling(pgconn, df, 'Water')
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
            worksheet = writer.sheets['Data']
            worksheet.freeze_panes(3, 0)
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
    plot_ids = df['plotid'].unique()
    plot_ids.sort()
    df['ticks'] = df['v'].astype(np.int64) // 10 ** 6
    seriestype = 'line' if ptype == '1' else 'column'
    for plotid in plot_ids:
        df2 = df[df['plotid'] == plotid]
        s.append(("""{type: '""" + seriestype + """',
            name: '"""+plotid+"""',
            data: """ + str([[a, b] for a, b in zip(df2['ticks'].values,
                                                    df2['discharge'].values)]) + """
        }""").replace("None", "null").replace("nan", "null"))
    series = ",".join(s)
    sys.stdout.write("""
$("#hc").highcharts({
    title: {text: '"""+title+"""'},
    chart: {zoomType: 'x'},
    yAxis: {title: {text: 'Tile Flow (mm)'}
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
        valueDecimals: 2,
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
