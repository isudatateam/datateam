#!/usr/bin/env python
"""SM Plot!"""
# pylint: disable=abstract-class-instantiated
import sys
import cgi
import datetime
import os

import pytz
import numpy as np
import pandas as pd
from pandas.io.sql import read_sql
from pyiem.util import get_dbconn, ssw
from common import ERRMSG

DEPTHS = [None, '10 cm', '20 cm', '40 cm', '60 cm', '100 cm', None, None]

LINESTYLE = ['-', '-', '-', '-', '-', '-',
             '-', '-', '-.', '-.', '-.', '-.', '-.',
             '-', '-.', '-.', '-.', '-.', '-.', '-.', '-.', '-.', '-.', '-.']


def send_error():
    """" """
    ssw("Content-type: application/javascript\n\n")
    ssw("alert('" + ERRMSG + "');")
    sys.exit()


def make_plot(form):
    """Make the plot"""
    (uniqueid, plotid) = form.getfirst('site', 'ISUAG::302E').split("::")
    if uniqueid in ['CLAY_R', 'CLAY_U']:
        DEPTHS[1] = '5 cm'
        DEPTHS[2] = '15 cm'
        DEPTHS[3] = '30 cm'
        DEPTHS[4] = '45 cm'
        DEPTHS[5] = '60 cm'
        DEPTHS[6] = '75 cm'
        DEPTHS[7] = '90 cm'
    elif uniqueid in ['BEAR', 'MAASS']:
        DEPTHS[1] = '7 cm'
        DEPTHS[2] = '15 cm'
        DEPTHS[3] = '30 cm'
        DEPTHS[4] = '60 cm'
        DEPTHS[5] = '90 cm'

    sts = datetime.datetime.strptime(form.getfirst('date', '2014-06-10'),
                                     '%Y-%m-%d')
    days = int(form.getfirst('days', 1))
    ets = sts + datetime.timedelta(days=days)
    pgconn = get_dbconn('td')
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
        d1temp_qc as d1t, coalesce(d1temp_qcflag, '') as d1t_f,
        d2temp_qc as d2t, coalesce(d2temp_qcflag, '') as d2t_f,
        d3temp_qc as d3t, coalesce(d3temp_qcflag, '') as d3t_f,
        d4temp_qc as d4t, coalesce(d4temp_qcflag, '') as d4t_f,
        d5temp_qc as d5t, coalesce(d5temp_qcflag, '') as d5t_f,
        d6temp_qc as d6t, coalesce(d6temp_qcflag, '') as d6t_f,
        d7temp_qc as d7t, coalesce(d7temp_qcflag, '') as d7t_f,
        d1moisture_qc as d1m, coalesce(d1moisture_qcflag, '') as d1m_f,
        d2moisture_qc as d2m, coalesce(d2moisture_qcflag, '') as d2m_f,
        d3moisture_qc as d3m, coalesce(d3moisture_qcflag, '') as d3m_f,
        d4moisture_qc as d4m, coalesce(d4moisture_qcflag, '') as d4m_f,
        d5moisture_qc as d5m, coalesce(d5moisture_qcflag, '') as d5m_f,
        d6moisture_qc as d6m, coalesce(d6moisture_qcflag, '') as d6m_f,
        d7moisture_qc as d7m, coalesce(d7moisture_qcflag, '') as d7m_f
        from decagon_data WHERE uniqueid = %s """+plotid_limit+"""
        and valid between %s and %s ORDER by valid ASC
        """, pgconn, params=(uniqueid, sts.date(), ets.date()))
        df['v'] = df['v'].apply(
            lambda x: x.astimezone(pytz.timezone(tzname)))

    elif ptype in ['3', '4']:
        res = 'hour' if ptype == '3' else 'week'
        df = read_sql("""SELECT
        timezone('UTC',
                 date_trunc('"""+res+"""', valid at time zone 'UTC')) as v,
        plotid, avg(d1temp_qc) as d1t, avg(d2temp_qc) as d2t,
        avg(d3temp_qc) as d3t, avg(d4temp_qc) as d4t, avg(d5temp_qc) as d5t,
        avg(d6temp_qc) as d6t, avg(d7temp_qc) as d7t,
        avg(d1moisture_qc) as d1m, avg(d2moisture_qc) as d2m,
        avg(d3moisture_qc) as d3m, avg(d4moisture_qc) as d4m,
        avg(d5moisture_qc) as d5m, avg(d6moisture_qc) as d6m,
        avg(d7moisture_qc) as d7m
        from decagon_data WHERE uniqueid = %s """+plotid_limit+"""
        and valid between %s and %s GROUP by v, plotid ORDER by v ASC
        """, pgconn, params=(uniqueid, sts.date(), ets.date()))
        df['v'] = pd.to_datetime(df['v'], utc=True)
        for n in ['m', 't']:
            for i in range(1, 6):
                df["d%s%s_f" % (n, i)] = '-'
    else:
        df = read_sql("""SELECT
        timezone('UTC', date_trunc('day', valid at time zone %s)) as v, plotid,
        avg(d1temp_qc) as d1t, avg(d2temp_qc) as d2t,
        avg(d3temp_qc) as d3t, avg(d4temp_qc) as d4t, avg(d5temp_qc) as d5t,
        avg(d6temp_qc) as d6t, avg(d7temp_qc) as d7t,
        avg(d1moisture_qc) as d1m, avg(d2moisture_qc) as d2m,
        avg(d3moisture_qc) as d3m, avg(d4moisture_qc) as d4m,
        avg(d5moisture_qc) as d5m, avg(d6moisture_qc) as d6m,
        avg(d7moisture_qc) as d7m
        from decagon_data WHERE uniqueid = %s  """+plotid_limit+"""
        and valid between %s and %s GROUP by v, plotid ORDER by v ASC
        """, pgconn, params=(tzname, uniqueid, sts.date(), ets.date()))
        df['v'] = pd.to_datetime(df['v'], utc=True)
        for n in ['m', 't']:
            for i in range(1, 6):
                df["d%s%s_f" % (n, i)] = '-'
    if len(df.index) < 3:
        send_error()
    if ptype not in ['2', ]:
        df['v'] = df['v'].apply(
            lambda x: x.tz_convert(tzname))

    if viewopt != 'js':
        df['v'] = df['v'].dt.strftime("%Y-%m-%d %H:%M")
        df.rename(columns=dict(v='timestamp',
                               d1t='%s Temp (C)' % (DEPTHS[1], ),
                               d2t='%s Temp (C)' % (DEPTHS[2], ),
                               d3t='%s Temp (C)' % (DEPTHS[3], ),
                               d4t='%s Temp (C)' % (DEPTHS[4], ),
                               d5t='%s Temp (C)' % (DEPTHS[5], ),
                               d6t='%s Temp (C)' % (DEPTHS[6], ),
                               d7t='%s Temp (C)' % (DEPTHS[7], ),
                               d1m='%s Moisture (cm3/cm3)' % (DEPTHS[1], ),
                               d2m='%s Moisture (cm3/cm3)' % (DEPTHS[2], ),
                               d3m='%s Moisture (cm3/cm3)' % (DEPTHS[3], ),
                               d4m='%s Moisture (cm3/cm3)' % (DEPTHS[4], ),
                               d5m='%s Moisture (cm3/cm3)' % (DEPTHS[5], ),
                               d6m='%s Moisture (cm3/cm3)' % (DEPTHS[6], ),
                               d7m='%s Moisture (cm3/cm3)' % (DEPTHS[7], ),
                               d1t_f='%s Temp Flag' % (DEPTHS[1], ),
                               d2t_f='%s Temp Flag' % (DEPTHS[2], ),
                               d3t_f='%s Temp Flag' % (DEPTHS[3], ),
                               d4t_f='%s Temp Flag' % (DEPTHS[4], ),
                               d5t_f='%s Temp Flag' % (DEPTHS[5], ),
                               d6t_f='%s Temp Flag' % (DEPTHS[6], ),
                               d7t_f='%s Temp Flag' % (DEPTHS[7], ),
                               d1m_f='%s Moisture Flag' % (DEPTHS[1], ),
                               d2m_f='%s Moisture Flag' % (DEPTHS[2], ),
                               d3m_f='%s Moisture Flag' % (DEPTHS[3], ),
                               d4m_f='%s Moisture Flag' % (DEPTHS[4], ),
                               d5m_f='%s Moisture Flag' % (DEPTHS[5], ),
                               d6m_f='%s Moisture Flag' % (DEPTHS[6], ),
                               d7m_f='%s Moisture Flag' % (DEPTHS[7], ),
                               ),
                  inplace=True)
        if viewopt == 'html':
            ssw("Content-type: text/html\n\n")
            ssw(df.to_html(index=False))
            return
        if viewopt == 'csv':
            ssw('Content-type: application/octet-stream\n')
            ssw(('Content-Disposition: attachment; '
                 'filename=%s_%s_%s_%s.csv\n\n'
                 ) % (uniqueid, plotid, sts.strftime("%Y%m%d"),
                      ets.strftime("%Y%m%d")))
            ssw(df.to_csv(index=False))
            return
        if viewopt == 'excel':
            ssw('Content-type: application/octet-stream\n')
            ssw(('Content-Disposition: attachment; '
                 'filename=%s_%s_%s_%s.xlsx\n\n'
                 ) % (uniqueid, plotid, sts.strftime("%Y%m%d"),
                      ets.strftime("%Y%m%d")))
            with pd.ExcelWriter('/tmp/ss.xlsx') as writer:
                df.to_excel(writer, 'Data', index=False)
            ssw(open('/tmp/ss.xlsx', 'rb').read())
            os.unlink('/tmp/ss.xlsx')
            return

    # Begin highcharts output
    lbl = "Plot:%s" % (plotid,)
    if depth != 'all':
        lbl = "Depth:%s" % (DEPTHS[int(depth)],)
    title = ("Soil Temperature + Moisture for "
             "Site:%s %s Period:%s to %s"
             ) % (uniqueid, lbl, sts.date(), ets.date())
    ssw("Content-type: application/javascript\n\n")
    ssw("""
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
        for i, n in enumerate(['d1t', 'd2t', 'd3t', 'd4t', 'd5t',
                               'd6t', 'd7t']):
            if df[n].isnull().all():
                continue
            v = df[['ticks', n]].to_json(orient='values')
            lines.append("""{
            name: '"""+DEPTHS[i+1]+""" Temp',
            type: 'line',
            tooltip: {valueDecimal: 1},
            data: """+v+"""
            }
            """)
        for i, n in enumerate(['d1m', 'd2m', 'd3m', 'd4m', 'd5m', 'd6m',
                               'd7m']):
            if df[n].isnull().all():
                continue
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
    ssw("""
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
