"""Some common stuff."""
from io import BytesIO

from pyiem.plot.use_agg import plt

COPYWRITE = """credits: {
        position: {align: 'left', x: 15},
        text: "© Transforming Drainage | transformingdrainage.org | drainagedata.org"
    },"""
COLORS = {
    "UD": "#36454F",
    "ND": "#36454F",
    "CD": "#FFDF00",
    "FD": "#000080",
    "SD": "#ff0000",
    "SH": "#FFA500",
    "SI": "#ADD8E6",
    "CA": "#4169E1",
    "SB": "#00ff00",
    "TDB": "#FFC0CB",
    "n/a": "#FFC0CB",
}

CODES = {
    "UD": "Undrained (No Drainage)",
    "ND": "No Drainage",
    "FD": "Free Drainage (Conventional Drainage)",
    "CD": "Controlled Drainage (Managed Drainage)",
    "SD": "Surface Drainage",
    "SH": "Shallow Drainage",
    "SI": "Controlled Drainage with Subirrigation",
    "CA": "Automated Controlled Drainage",
    "SB": "Saturated Buffer",
    "TBD": "To Be Determined",
    "n/a": "Not Available or Not Applicable",
}

ERRMSG = (
    "No data found. Check the start date falls within the "
    "applicable date range for the research site. "
    "If yes, try expanding the number of days included."
)


def getColor(label, i):
    """Lookup and generate highcharts color

    Args:
      label (str): the label of interest
    """
    if label in COLORS:
        return f"color: '{COLORS[label]}'"
    return f"colorIndex: {i + 1}"


def send_error(start_response, viewopt, msg=ERRMSG):
    """ " """
    if viewopt == "js":
        start_response("200 OK", [("Content-type", "application/javascript")])
        return b"alert('No data found, sorry');"
    fig, ax = plt.subplots(1, 1)
    ax.text(0.5, 0.5, msg, transform=ax.transAxes, ha="center")
    start_response("200 OK", [("Content-type", "image/png")])
    ram = BytesIO()
    fig.savefig(ram, format="png")
    ram.seek(0)
    return ram.read()
