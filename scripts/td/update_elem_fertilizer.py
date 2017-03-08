"""Update the Elemental Fertiziler Columns in the backend datasheet..."""
import pyiem.cscap_utils as util

sprclient = util.get_spreadsheet_client(util.get_config())

spreadsheet = util.Spreadsheet(sprclient,
                               '1m797EoMbtAvasqum-pB5ykVMeHsGWwcVvre8MbvE-xM')
spreadsheet.get_worksheets()
sheet = spreadsheet.worksheets['Soil & Fert']


def float2(val):
    if val is None:
        return 0
    return float(val)


for i, entry in enumerate(sheet.get_list_feed().entry):
    if i == 0:
        continue
    d = entry.to_dict()
    if d['productrate'] is None or d['productrate'] in ['-1.0', 'n/a', '-1']:
        # Option B, values are in lb / ac
        n = float2(d['nitrogen']) / 1.12
        p = float2(d['phosphate']) / 1.12
        k = float2(d['potash']) / 1.12
        s = float2(d['sulfur']) / 1.12
        z = float2(d['zinc']) / 1.12
        m = float2(d['magnesium']) / 1.12
        c = float2(d['calcium']) / 1.12
        f = float2(d['iron']) / 1.12
    else:
        # Option A, straight percentages
        prate = float2(d['productrate'])
        n = prate * float2(d['nitrogen']) / 100.
        p = 0
        k = 0
        if d['phosphorus'] is not None and float2(d['phosphorus']) > 0:
            p = prate * float2(d['phosphorus']) / 100.
        if d['phosphate'] is not None and float2(d['phosphate']) > 0:
            p = prate * float2(d['phosphate']) / 100. * 0.437
        if d['potassium'] is not None and float2(d['potassium']) > 0:
            k = prate * float2(d['potassium']) / 100.
        if d['potash'] is not None and float2(d['potash']) > 0:
            k = prate * float(d['potash']) / 100. * 0.830
        s = prate * float2(d['sulfur']) / 100.
        z = prate * float2(d['zinc']) / 100.
        m = prate * float2(d['magnesium']) / 100.
        c = prate * float2(d['calcium']) / 100.
        f = prate * float2(d['iron']) / 100.

    entry.set_value('nitrogenelem', "%.2f" % (n, ))
    entry.set_value('phosphoruselem', "%.2f" % (p, ))
    entry.set_value('potassiumelem', "%.2f" % (k, ))
    entry.set_value('sulfurelem', "%.2f" % (s, ))
    entry.set_value('zincelem', "%.2f" % (z, ))
    entry.set_value('magnesiumelem', "%.2f" % (m, ))
    entry.set_value('calciumelem', "%.2f" % (c, ))
    entry.set_value('ironelem', "%.2f" % (f, ))
    sprclient.update(entry)
