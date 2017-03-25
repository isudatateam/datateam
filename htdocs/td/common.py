COLORS = {'UD': '#36454F',
          'FD': '#FFDF00',
          'CD': '#000080',
          'SD': '#ff0000',
          'SH': '#FFA500',
          'SI': '#ADD8E6',
          'CA': '#4169E1',
          'SB': '#00ff00',
          'TDB': '#FFC0CB',
          'n/a': '#FFC0CB',
          }

CODES = {'UD': 'Undrained (No Drainage)',
         'FD': 'Free Drainage (Conventional Drainage)',
         'CD': 'Controlled Drainage (Managed Drainage)',
         'SD': 'Surface Drainage',
         'SH': 'Shallow Drainage',
         'SI': 'Controlled Drainage with Subirrigation',
         'CA': 'Automated Controlled Drainage',
         'SB': 'Saturated Buffer',
         'TBD': 'To Be Determined',
         'n/a': 'Not Available or Not Applicable'}


def getColor(label, i):
    """Lookup and generate highcharts color

    Args:
      label (str): the label of interest
    """
    if label in COLORS:
        return "color: '%s'" % (COLORS[label], )
    return "colorIndex: %s" % (i+1, )
