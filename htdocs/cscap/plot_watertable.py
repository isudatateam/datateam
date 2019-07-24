#!/usr/bin/env python
"""Plot!"""
import sys
from io import BytesIO
import cgi
import datetime
import os

import pandas as pd
from pandas.io.sql import read_sql
import numpy as np
from pyiem.plot.use_agg import plt
from pyiem.util import get_dbconn, ssw

ERRMSG = ("No data found. Check the start date falls within the "
          "applicable date range for the research site. "
          "If yes, try expanding the number of days included.")
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
    df = df.reindex(cols, axis=1)
    return df


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


def make_plot(form):
    """Make the plot"""
    uniqueid = form.getfirst('site', 'ISUAG')

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
        df = read_sql("""
        SELECT uniqueid, plotid, valid at time zone 'UTC' as v,
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
        and valid between %s and %s GROUP by uniqueid, v, plotid ORDER by v ASC
        """, pgconn, params=(uniqueid, sts.date(), ets.date()))
    else:
        df = read_sql("""
        SELECT uniqueid, plotid, date(valid at time zone %s) as v,
        avg(depth_mm_qc) as depth
        from watertable_data WHERE uniqueid = %s
        and valid between %s and %s GROUP by uniqueid, v, plotid ORDER by v ASC
        """, pgconn, params=(tzname, uniqueid, sts.date(), ets.date()))
    if len(df.index) < 3:
        send_error(viewopt, "No / Not Enough Data Found, sorry!")
    if ptype not in ['2', ]:
        df['v'] = df['v'].apply(
            lambda x: x.tz_localize('UTC').tz_convert(tzname))

    if viewopt not in ['plot', 'js']:
        if viewopt == 'excel':
            df['v'] = df['v'].dt.strftime("%Y-%m-%d %H:%M")
        df = df.rename(columns=dict(v='timestamp',
                               depth='Depth (mm)'
                               ))
        df = add_bling(pgconn, df, 'Water')
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
            writer = pd.ExcelWriter('/tmp/ss.xlsx')
            df.to_excel(writer, 'Data', index=False)
            worksheet = writer.sheets['Data']
            worksheet.freeze_panes(3, 0)
            writer.save()
            ssw(open('/tmp/ss.xlsx', 'rb').read())
            os.unlink('/tmp/ss.xlsx')
            return

    # Begin highcharts output
    ssw("Content-type: application/javascript\n\n")
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
    ssw("""
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
