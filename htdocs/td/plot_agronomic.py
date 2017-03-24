#!/usr/bin/env python
"""Plot!"""
import psycopg2
import matplotlib
import sys
import cStringIO
import pandas as pd
from pandas.io.sql import read_sql
import cgi
import os
matplotlib.use('agg')
import matplotlib.pyplot as plt  # NOPEP8

LINESTYLE = ['-', '-', '-', '-', '-', '-',
             '-', '-', '-.', '-.', '-.', '-.', '-.',
             '-', '-.', '-.', '-.', '-.', '-.', '-.', '-.', '-.', '-.', '-.']

CODES = {'UD': 'Undrained (No Drainage)',
         'FD': 'Free Drainage (Conventional Drainage)',
         'CD': 'Controlled Drainage (Managed Drainage)',
         'SD': 'Surface Drainage',
         'ND': 'No Drainage',
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


def get_vardesc(varname):
    pgconn = psycopg2.connect(database='td', host='iemdb',
                              user='nobody')
    cursor = pgconn.cursor()
    cursor.execute("""
    SELECT short_description, units from td_data_dictionary WHERE
    code_column_heading = %s
    """, (varname, ))
    if cursor.rowcount == 0:
        return varname, varname
    return cursor.fetchone()


def make_plot(form):
    """Make the plot"""
    pgconn = psycopg2.connect(database='td', host='iemdb', user='nobody')
    uniqueid = form.getfirst('site', 'ISUAG')
    varname = form.getfirst('varname', 'AGR17')
    (varlabel, varunits) = get_vardesc(varname)

    group = int(form.getfirst('group', 0))
    viewopt = form.getfirst('view', 'plot')
    df = read_sql("""SELECT value, year, plotid from agronomic_data
        WHERE site = %s and varname = %s and value is not null
        and value not in ('did not collect')
        ORDER by plotid, year ASC
        """, pgconn, params=(uniqueid, varname), index_col=None)
    if len(df.index) < 1:
        send_error(viewopt, "No / Not Enough Data Found, sorry!")
    df['value'] = pd.to_numeric(df['value'], errors='coerse')
    linecol = 'plotid'
    if group == 1:
        # Generate the plotid lookup table
        plotdf = read_sql("""
            SELECT * from plotids where siteid = %s
        """, pgconn, params=(uniqueid, ), index_col='plotid')

        def lookup(row):
            return plotdf.loc[row['plotid'], "y%s" % (row['year'], )]
        df['treatment'] = df.apply(lambda row: lookup(row), axis=1)
        del df['plotid']
        df = df.groupby(['treatment', 'year']).mean()
        df.reset_index(inplace=True)
        linecol = 'treatment'

    if viewopt not in ['plot', 'js']:
        df.rename(columns=dict(value=varname
                               ),
                  inplace=True)
        if viewopt == 'html':
            sys.stdout.write("Content-type: text/html\n\n")
            sys.stdout.write(df.to_html(index=False))
            return
        if viewopt == 'csv':
            sys.stdout.write('Content-type: application/octet-stream\n')
            sys.stdout.write(('Content-Disposition: attachment; '
                              'filename=%s_%s.csv\n\n'
                              ) % (uniqueid, varname))
            sys.stdout.write(df.to_csv(index=False))
            return
        if viewopt == 'excel':
            sys.stdout.write('Content-type: application/octet-stream\n')
            sys.stdout.write(('Content-Disposition: attachment; '
                              'filename=%s_%s.xlsx\n\n'
                              ) % (uniqueid, varname))
            writer = pd.ExcelWriter('/tmp/ss.xlsx')
            df.to_excel(writer, 'Data', index=False)
            writer.save()
            sys.stdout.write(open('/tmp/ss.xlsx', 'rb').read())
            os.unlink('/tmp/ss.xlsx')
            return

    # Begin highcharts output
    sys.stdout.write("Content-type: application/javascript\n\n")
    title = ("Agronomic Data for Site: %s"
             ) % (uniqueid, )
    s = []
    plot_ids = df[linecol].unique()
    plot_ids.sort()
    for plotid in plot_ids:
        df2 = df[df[linecol] == plotid]
        s.append(("""{type: 'column',
            name: '""" + CODES.get(plotid, plotid) + """',
            data: """ + str([[a, b] for a, b in zip(df2['year'].values,
                                                    df2['value'].values)]) + """
        }""").replace("None", "null").replace("nan", "null"))
    series = ",".join(s)
    sys.stdout.write("""
$("#hc").highcharts({
    title: {text: '"""+title+"""'},
    subtitle: {text: '""" + varlabel + """ (""" + varunits + """)'},
    chart: {zoomType: 'x'},
    xAxis: {tickInterval: 1},
    yAxis: [
        {title: {text: '""" + varlabel + """ (""" + varunits + """)'}}
    ],
    plotOptions: {line: {turboThreshold: 0}},
    tooltip: {
        shared: true,
        valueDecimals: 0
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
