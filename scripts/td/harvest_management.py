"""Harvest the data in the data management store!"""
from __future__ import print_function

import pyiem.cscap_utils as util
from pyiem.util import get_dbconn

TABS = ['Plant & Harvest', 'Soil & Fert', 'Pesticides', 'Residue Mngt',
        'DWM', 'Irrigation', 'Notes']
TABLENAMES = ['plant_harvest', 'soil_fert', 'pesticides', 'residue_mngt',
              'dwm', 'irrigation', 'notes']


def main():
    """Go Main"""
    config = util.get_config()

    pgconn = get_dbconn('td')
    pcursor = pgconn.cursor()

    # Get me a client, stat
    spr_client = util.get_spreadsheet_client(config)

    spread = util.Spreadsheet(spr_client, config['td']['manstore'])

    translate = {'date': 'valid'}

    for sheetkey, table in zip(TABS, TABLENAMES):
        pcursor.execute("""DELETE from """ + table)
        deleted = pcursor.rowcount
        sheet = spread.worksheets[sheetkey]

        added = 0
        for rownum, entry in enumerate(sheet.get_list_feed().entry):
            # Skip the first row of units
            if rownum == 0:
                continue
            data = entry.to_dict()
            cols = []
            vals = []
            for key in data.keys():
                if key.startswith('gio'):
                    continue
                val = data[key]
                if key in ['date', 'biomassdate1', 'biomassdate2',
                           'outletdate']:
                    val = (val if val not in ['unknown', 'N/A', 'n/a', 'TBD']
                           else None)
                vals.append(val)
                cols.append(translate.get(key, key))

            sql = """
                INSERT into %s(%s) VALUES (%s)
                """ % (table, ",".join(cols), ','.join(["%s"]*len(cols)))
            try:
                pcursor.execute(sql, vals)
            except Exception as exp:
                print("[TD] harvest_management traceback")
                print(exp)
                for col, val in zip(cols, vals):
                    print("   |%s| -> |%s|" % (col, val))
                return
            added += 1

        print(("[TD] harvest_management %16s added:%4s deleted:%4s"
               ) % (sheetkey, added, deleted))

    pcursor.close()
    pgconn.commit()
    pgconn.close()


if __name__ == '__main__':
    main()
