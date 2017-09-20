#!/usr/bin/env python
"""Decagon SM Plot!"""
import psycopg2
import sys
import pytz
import numpy as np
import pandas as pd
from pandas.io.sql import read_sql
import cgi
import datetime
import os

DEPTHS = [None, '10 cm', '20 cm', '40 cm', '60 cm', '100 cm']

LINESTYLE = ['-', '-', '-', '-', '-', '-',
             '-', '-', '-.', '-.', '-.', '-.', '-.',
             '-', '-.', '-.', '-.', '-.', '-.', '-.', '-.', '-.', '-.', '-.']


def send_error(msg):
    """" """
    sys.stdout.write("Content-type: application/javascript\n\n")
    sys.stdout.write("alert('No data found, sorry');")
    sys.exit()


def make_plot(form):
    """Make the plot"""
    (uniqueid, plotid) = form.getfirst('site', 'ISUAG::302E').split("::")
    if uniqueid in ['KELLOGG', 'MASON']:
        DEPTHS[1] = '-'
        DEPTHS[5] = '80 cm'
    elif uniqueid == 'NAEW':
        DEPTHS[1] = '5 cm'
        DEPTHS[2] = '10 cm'
        DEPTHS[3] = '20 cm'
        DEPTHS[4] = '30 cm'
        DEPTHS[5] = '50 cm'

    sts = datetime.datetime.strptime(form.getfirst('date', '2014-06-10'),
                                     '%Y-%m-%d')
    days = int(form.getfirst('days', 1))
    ets = sts + datetime.timedelta(days=days)
    pgconn = psycopg2.connect(database='sustainablecorn', host='iemdb')
    tzname = 'America/Chicago' if uniqueid in [
        'ISUAG', 'SERF', 'GILMORE'] else 'America/New_York'
    viewopt = form.getfirst('view', 'js')
    ptype = form.getfirst('ptype', '1')
    plotid_limit = "and plotid = '%s'" % (plotid, )
    depth = form.getfirst('depth', 'all')
    if depth != 'all':
        plotid_limit = ""
    if ptype == '1':
        df = read_sql("""SELECT valid as v, plotid,
        d1temp_qc as d1t,
        d2temp_qc as d2t,
        d3temp_qc as d3t,
        d4temp_qc as d4t,
        d5temp_qc as d5t,
        d1moisture_qc as d1m,
        d2moisture_qc as d2m,
        d3moisture_qc as d3m,
        d4moisture_qc as d4m,
        d5moisture_qc as d5m
        from decagon_data WHERE uniqueid = %s """+plotid_limit+"""
        and valid between %s and %s ORDER by valid ASC
        """, pgconn, params=(uniqueid, sts.date(), ets.date()))
        df['v'] = df['v'].apply(
            lambda x: x.astimezone(pytz.timezone(tzname)))

    elif ptype in ['3', '4']:
        res = 'hour' if ptype == '3' else 'week'
        df = read_sql("""SELECT
        timezone('UTC', date_trunc('"""+res+"""', valid at time zone 'UTC')) as v, plotid,
        avg(d1temp_qc) as d1t, avg(d2temp_qc) as d2t,
        avg(d3temp_qc) as d3t, avg(d4temp_qc) as d4t, avg(d5temp_qc) as d5t,
        avg(d1moisture_qc) as d1m, avg(d2moisture_qc) as d2m,
        avg(d3moisture_qc) as d3m, avg(d4moisture_qc) as d4m,
        avg(d5moisture_qc) as d5m
        from decagon_data WHERE uniqueid = %s """+plotid_limit+"""
        and valid between %s and %s GROUP by v, plotid ORDER by v ASC
        """, pgconn, params=(uniqueid, sts.date(), ets.date()))

    else:
        df = read_sql("""SELECT
        timezone('UTC', date_trunc('day', valid at time zone %s)) as v, plotid,
        avg(d1temp_qc) as d1t, avg(d2temp_qc) as d2t,
        avg(d3temp_qc) as d3t, avg(d4temp_qc) as d4t, avg(d5temp_qc) as d5t,
        avg(d1moisture_qc) as d1m, avg(d2moisture_qc) as d2m,
        avg(d3moisture_qc) as d3m, avg(d4moisture_qc) as d4m,
        avg(d5moisture_qc) as d5m
        from decagon_data WHERE uniqueid = %s  """+plotid_limit+"""
        and valid between %s and %s GROUP by v, plotid ORDER by v ASC
        """, pgconn, params=(tzname, uniqueid, sts.date(), ets.date()))

    if len(df.index) < 3:
        send_error("No / Not Enough Data Found, sorry!")
    if ptype not in ['2']:
        df['v'] = df['v'].apply(
            lambda x: x.tz_convert(tzname))

    if viewopt != 'js':
        df.rename(columns=dict(v='timestamp',
                               d1t='%s Temp (C)' % (DEPTHS[1], ),
                               d2t='%s Temp (C)' % (DEPTHS[2], ),
                               d3t='%s Temp (C)' % (DEPTHS[3], ),
                               d4t='%s Temp (C)' % (DEPTHS[4], ),
                               d5t='%s Temp (C)' % (DEPTHS[5], ),
                               d1m='%s Moisture (cm3/cm3)' % (DEPTHS[1], ),
                               d2m='%s Moisture (cm3/cm3)' % (DEPTHS[2], ),
                               d3m='%s Moisture (cm3/cm3)' % (DEPTHS[3], ),
                               d4m='%s Moisture (cm3/cm3)' % (DEPTHS[4], ),
                               d5m='%s Moisture (cm3/cm3)' % (DEPTHS[5], ),
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
            writer = pd.ExcelWriter('/tmp/ss.xlsx',
                                    options={'remove_timezone': True})
            df.to_excel(writer, 'Data', index=False)
            writer.save()
            sys.stdout.write(open('/tmp/ss.xlsx', 'rb').read())
            os.unlink('/tmp/ss.xlsx')
            return

    # Begin highcharts output
    lbl = "Plot:%s" % (plotid,)
    if depth != 'all':
        lbl = "Depth:%s" % (DEPTHS[int(depth)],)
    title = ("Decagon Temperature + Moisture for "
             "Site:%s %s Period:%s to %s"
             ) % (uniqueid, lbl, sts.date(), ets.date())
    sys.stdout.write("Content-type: application/javascript\n\n")
    sys.stdout.write("""
/**
 * In order to synchronize tooltips and crosshairs, override the
 * built-in events with handlers defined on the parent element.
 */
var charts = [],
    options;

/**
 * Synchronize zooming through the setExtremes event handler.
 */
function syncExtremes(e) {
    var thisChart = this.chart;

    if (e.trigger !== 'syncExtremes') { // Prevent feedback loop
        Highcharts.each(Highcharts.charts, function (chart) {
            if (chart !== thisChart) {
                if (chart.xAxis[0].setExtremes) { // It is null while updating
                    chart.xAxis[0].setExtremes(e.min, e.max, undefined, false,
                    { trigger: 'syncExtremes' });
                }
            }
        });
    }
}

function syncTooltip(container, p) {
    var i = 0;
    for (; i < charts.length; i++) {
        if (container.id != charts[i].container.id) {
            var d = [];
            for (j=0; j < charts[i].series.length; j++){
                d[j] = charts[i].series[j].data[p];
            }
            charts[i].tooltip.refresh(d);
        }
    }
}


options = {
    chart: {zoomType: 'x'},
    plotOptions: {
        series: {
            cursor: 'pointer',
            allowPointSelect: true,
            point: {
                events: {
                    click: function() {
                        editPoint(this);
                    },
                    mouseOver: function () {
                        // Note, I converted this.x to this.index
                        syncTooltip(this.series.chart.container, this.index);
                    }
                }
            }
        }
    },
    tooltip: {
        shared: true,
        crosshairs: true
    },
    xAxis: {
        type: 'datetime',
        crosshair: true,
        events: {
            setExtremes: syncExtremes
        }
    }
};

""")
    # to_json can't handle serialization of dt
    df['ticks'] = df['v'].astype(np.int64) // 10 ** 6
    lines = []
    lines2 = []
    if depth == 'all':
        for i, n in enumerate(['d1t', 'd2t', 'd3t', 'd4t', 'd5t']):
            v = df[['ticks', n]].to_json(orient='values')
            lines.append("""{
            name: '"""+DEPTHS[i+1]+""" Temp',
            type: 'line',
            tooltip: {valueDecimal: 1},
            data: """+v+"""
            }
            """)
        for i, n in enumerate(['d1m', 'd2m', 'd3m', 'd4m', 'd5m']):
            v = df[['ticks', n]].to_json(orient='values')
            lines2.append("""{
            name: '"""+DEPTHS[i+1]+""" VSM',
            type: 'line',
            tooltip: {valueDecimal: 3},
            data: """+v+"""
            }
            """)
    else:
        dlevel = "d%st" % (depth, )
        plot_ids = df['plotid'].unique()
        plot_ids.sort()
        for i, plotid in enumerate(plot_ids):
            df2 = df[df['plotid'] == plotid]
            v = df2[['ticks', dlevel]].to_json(orient='values')
            lines.append("""{
            name: '"""+plotid+"""',
            type: 'line',
            tooltip: {valueDecimal: 3},
            data: """+v+"""
            }
            """)
        dlevel = "d%sm" % (depth, )
        plot_ids = df['plotid'].unique()
        plot_ids.sort()
        for i, plotid in enumerate(plot_ids):
            df2 = df[df['plotid'] == plotid]
            v = df2[['ticks', dlevel]].to_json(orient='values')
            lines2.append("""{
            name: '"""+plotid+"""',
            type: 'line',
            tooltip: {valueDecimal: 3},
            data: """+v+"""
            }
            """)
    series = ",".join(lines)
    series2 = ",".join(lines2)
    sys.stdout.write("""
charts[0] = new Highcharts.Chart($.extend(true, {}, options, {
    chart: { renderTo: 'hc1'},
    title: {text: '"""+title+"""'},
    yAxis: {title: {text: 'Temperature [C]'}},
    series: ["""+series+"""]
}));
charts[1] = new Highcharts.Chart($.extend(true, {}, options, {
    chart: { renderTo: 'hc2'},
    title: {text: '"""+title+"""'},
    yAxis: {title: {text: 'Volumetric Soil Moisture [cm3/cm3]'}},
    series: ["""+series2+"""]
}));
    """)

    # ax[1].set_xlabel("Time (%s Timezone)" % (tzname, ))


def main():
    """Do Something"""
    form = cgi.FieldStorage()
    make_plot(form)

if __name__ == '__main__':
    main()
