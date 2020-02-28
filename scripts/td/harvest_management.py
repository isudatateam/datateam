"""Harvest the data in the data management store!"""

import pyiem.cscap_utils as util
from pyiem.util import get_dbconn, exponential_backoff

TABS = ['Plant & Harvest', 'Soil & Fert', 'Pesticides', 'Residue Mngt',
        'DWM', 'Irrigation', 'Notes']
TABLENAMES = ['plant_harvest', 'soil_fert', 'pesticides', 'residue_mngt',
              'dwm', 'irrigation', 'notes']


def main():
    """Go Main"""
    pgconn = get_dbconn('td')
    pcursor = pgconn.cursor()

    config = util.get_config()
    sheets = util.get_sheetsclient(config, "td")
    f = sheets.spreadsheets().get(
        spreadsheetId=config['td']['manstore'], includeGridData=True
    )
    j = exponential_backoff(f.execute)
    translate = {
        'date': 'valid',
        'plantmaturity_GDD (F)': 'plantmaturity_gdd'}

    for sheet in j['sheets']:
        table = TABLENAMES[TABS.index(sheet['properties']['title'])]
        pcursor.execute("""DELETE from """ + table)
        deleted = pcursor.rowcount
        added = 0
        for grid in sheet['data']:
            cols = [a['formattedValue'] for a in grid['rowData'][0]['values']]
            for row in grid['rowData'][2:]:
                vals = [a.get('formattedValue') for a in row['values']]
                data = dict(zip(cols, vals))
                dbvals = []
                dbcols = []
                for key in data.keys():
                    if key.startswith('gio'):
                        continue
                    val = data[key]
                    if key in ['date', 'biomassdate1', 'biomassdate2',
                               'outletdate']:
                        val = (
                            val
                            if val not in ['unknown', 'N/A', 'n/a', 'TBD']
                            else None
                        )
                    dbvals.append(val)
                    dbcols.append(translate.get(key, key.replace("_", "")))

                sql = """
                    INSERT into %s(%s) VALUES (%s)
                """ % (table, ",".join(dbcols), ','.join(["%s"]*len(dbcols)))
                try:
                    pcursor.execute(sql, dbvals)
                except Exception as exp:
                    print("[TD] harvest_management traceback")
                    print(exp)
                    for col, val in zip(cols, vals):
                        print("   |%s| -> |%s|" % (col, val))
                    return
                added += 1

        print(("[TD] harvest_management %16s added:%4s deleted:%4s"
               ) % (table, added, deleted))

    pcursor.close()
    pgconn.commit()
    pgconn.close()


if __name__ == '__main__':
    main()
