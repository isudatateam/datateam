#!/usr/bin/env python
"""Plot!"""
import sys
from io import BytesIO
import cgi
import os

import pandas as pd
from pandas.io.sql import read_sql
import numpy as np
from pyiem.plot.use_agg import plt
from pyiem.util import get_dbconn, ssw

ERRMSG = ("No data found. Check the start date falls within the "
          "applicable date range for the research site. "
          "If yes, try expanding the number of days included.")
VARDICT = {'WAT2': {'title': 'Nitrate-N Concentration', 'units': 'mg N / L'},
           'WAT9': {'title': 'Soluble Reactive Phosphorus Concentration',
                    'units': 'ug P / L'}}


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
    uniqueid = form.getfirst('site', 'ISUAG').split("::")[0]

    pgconn = get_dbconn('sustainablecorn')
    viewopt = form.getfirst('view', 'plot')
    varname = form.getfirst('varname', 'WAT2')
    df = read_sql("""
    SELECT uniqueid, plotid, valid at time zone 'UTC' as v, value
    from waterquality_data WHERE uniqueid = %s
    and varname = %s ORDER by valid ASC
    """, pgconn, params=(uniqueid, varname))

    if viewopt not in ['plot', 'js']:
        newcolname = "%s, %s" % (VARDICT[varname]['title'],
                                 VARDICT[varname]['units'])
        df.rename(columns=dict(v='timestamp',
                               value=newcolname
                               ),
                  inplace=True)
        df = add_bling(pgconn, df, 'Water')
        if viewopt == 'html':
            ssw("Content-type: text/html\n\n")
            ssw(df.to_html(index=False))
            return
        if viewopt == 'csv':
            ssw('Content-type: application/octet-stream\n')
            ssw(('Content-Disposition: attachment; '
                 'filename=%s.csv\n\n') % (uniqueid, ))
            ssw(df.to_csv(index=False))
            return
        if viewopt == 'excel':
            ssw('Content-type: application/octet-stream\n')
            ssw(('Content-Disposition: attachment; '
                 'filename=%s.xlsx\n\n') % (uniqueid, ))
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
    ssw("""
$("#hc").highcharts({
    title: {text: '"""+title+"""'},
    chart: {zoomType: 'x'},
    yAxis: {title: {text: '""" + VARDICT[varname]["title"] + """ """ +
    VARDICT[varname]["units"] + """'}
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
