"""SM Plot!"""

# pylint: disable=abstract-class-instantiated
import datetime
import sys

import numpy as np
import pandas as pd
from paste.request import parse_formvars
from pyiem.util import get_sqlalchemy_conn

sys.path.append("/opt/datateam/htdocs/td")
from common import COPYWRITE, send_error  # noqa

LINESTYLE = [
    "-",
    "-",
    "-",
    "-",
    "-",
    "-",
    "-",
    "-",
    "-.",
    "-.",
    "-.",
    "-.",
    "-.",
    "-",
    "-.",
    "-.",
    "-.",
    "-.",
    "-.",
    "-.",
    "-.",
    "-.",
    "-.",
    "-.",
]
BYCOL = {"daily": "day", "monthly": "month", "yearly": "year"}


def make_plot(form, start_response):
    """Make the plot"""
    (uniqueid, plotid) = form.get("site", "ISUAG--302E").split("--")

    sts = datetime.datetime.strptime(
        form.get("date", "2014-06-10"), "%Y-%m-%d"
    )
    days = int(form.get("days", 1))
    ets = sts + datetime.timedelta(days=days)
    by = form.get("ptype", "daily")

    with get_sqlalchemy_conn("td") as conn:
        df = pd.read_sql(
            f"SELECT date_trunc('{BYCOL[by]}', date)::date as v, depth, "
            "avg(soil_moisture) as soil_moisture, "
            "avg(soil_temperature) as soil_temperature "
            "from soil_moisture_data where siteid = %s and "
            "coalesce(plotid, location, '') = %s and "
            "date >= %s and date < %s "
            "GROUP by v, depth ORDER by v ASC",
            conn,
            params=(uniqueid, plotid, sts.date(), ets.date()),
        )

    if len(df.index) < 3:
        return send_error(start_response, "js")
    df["ticks"] = pd.to_datetime(df["v"]).astype(np.int64) // 10**6
    smdf = df[["ticks", "depth", "soil_moisture"]].pivot(
        index="ticks", columns="depth", values="soil_moisture"
    )
    smdf = smdf.reset_index()
    stdf = df[["ticks", "depth", "soil_temperature"]].pivot(
        index="ticks", columns="depth", values="soil_temperature"
    )
    stdf = stdf.reset_index()

    title = ("Soil Temperature + Moisture for " "Site:%s Period:%s to %s") % (
        uniqueid,
        sts.date(),
        ets.date(),
    )
    start_response("200 OK", [("Content-type", "application/javascript")])
    res = """
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

"""

    lines = []
    lines2 = []
    for depth in stdf.columns:
        if depth == "ticks" or stdf[depth].isnull().all():
            continue
        v = stdf[["ticks", depth]].to_json(orient="values")
        lines.append(
            """{
        name: '"""
            + str(depth)
            + """ Temp',
        type: 'line',
        tooltip: {valueDecimal: 1},
        data: """
            + v
            + """
        }
        """
        )
    for depth in smdf.columns:
        if depth == "ticks" or smdf[depth].isnull().all():
            continue
        v = smdf[["ticks", depth]].to_json(orient="values")
        lines2.append(
            """{
        name: '"""
            + str(depth)
            + """ Temp',
        type: 'line',
        tooltip: {valueDecimal: 1},
        data: """
            + v
            + """
        }
        """
        )

    series = ",".join(lines)
    series2 = ",".join(lines2)
    res += (
        """
charts[0] = new Highcharts.Chart($.extend(true, {}, options, {
    """
        + COPYWRITE
        + """
    chart: { renderTo: 'hc1'},
    title: {text: '"""
        + title
        + """'},
    yAxis: {title: {text: 'Temperature [C]'}},
    series: ["""
        + series
        + """]
}));
charts[1] = new Highcharts.Chart($.extend(true, {}, options, {
    """
        + COPYWRITE
        + """
    chart: { renderTo: 'hc2'},
    title: {text: '"""
        + title
        + """'},
    yAxis: {title: {text: 'Volumetric Soil Moisture [cm3/cm3]'}},
    series: ["""
        + series2
        + """]
}));
    """
    )
    return res.encode("utf-8")


def application(environ, start_response):
    """Do Something"""
    form = parse_formvars(environ)
    return [make_plot(form, start_response)]
