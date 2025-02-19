"""Shrug."""

from datetime import timedelta
from io import BytesIO

import numpy as np
from pyiem.database import get_dbconn
from pyiem.plot.use_agg import plt
from pyiem.webutil import iemapp


def make_plot(form):
    """Make the make_plot"""
    year = int(form.get("year", 2013))
    varname = form.get("varname", "AGR1")[:10]

    pgconn = get_dbconn("sustainablecorn")
    cursor = pgconn.cursor()
    cursor.execute(
        """
    SELECT date(updated) as d,
    sum(case when value not in ('.') then 1 else 0 end),
    count(*) from agronomic_data WHERE year = %s
    and varname = %s GROUP by d ORDER by d ASC
    """,
        (year, varname),
    )
    x = []
    y = []
    total = 0
    for i, row in enumerate(cursor):
        if i == 0:
            x.append(row[0] - timedelta(days=1))
            y.append(0)
        x.append(row[0])
        y.append(y[-1] + row[1])
        total += row[2]
    pgconn.close()
    xticks = []
    xticklabels = []
    if x:
        now = x[0]
        while now < x[-1]:
            if now.day == 1:
                fmt = (
                    "%b\n%Y" if (len(xticks) == 0 or now.month == 1) else "%b"
                )
                xticks.append(now)
                xticklabels.append(now.strftime(fmt))
            now += timedelta(days=1)

    (fig, ax) = plt.subplots(1, 1)
    ax.plot(x, np.array(y) / float(total) * 100.0)
    ax.set_ylim(0, 100)
    ax.set_yticks([0, 25, 50, 75, 100])
    ax.set_ylabel("Percentage [%]")
    ax.set_xticks(xticks)
    ax.set_xticklabels(xticklabels)
    ax.set_title("CSCAP %s Upload Progress for %s" % (varname, year))
    ax.grid(True)
    return fig


@iemapp()
def application(environ, start_response):
    """Make a plot please"""
    fig = make_plot(environ)

    start_response("200 OK", [("Content-type", "image/png")])

    ram = BytesIO()
    fig.savefig(ram, format="png", dpi=100)
    ram.seek(0)
    res = ram.read()
    return [res]
