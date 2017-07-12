"""Harvest the data in the data management store!"""
from __future__ import print_function
import pyiem.cscap_utils as util
import psycopg2


def main():
    """Go Main"""
    config = util.get_config()

    pgconn = psycopg2.connect(database='sustainablecorn', user='mesonet',
                              host=config['database']['host'])
    pcursor = pgconn.cursor()

    # Get me a client, stat
    spr_client = util.get_spreadsheet_client(config)

    spread = util.Spreadsheet(spr_client, config['cscap']['manstore'])

    translate = {'date': 'valid'}

    tabs = ['Field Operations', 'Management', 'Pesticides', 'DWM', 'Notes']
    tablenames = ['operations', 'management', 'pesticides', 'dwm', 'notes']
    for sheetkey, table in zip(tabs, tablenames):
        pcursor.execute("""DELETE from """ + table)
        deleted = pcursor.rowcount
        sheet = spread.worksheets[sheetkey]

        added = 0
        for rownum, entry in enumerate(sheet.get_list_feed().entry):
            # Skip the first row of units
            if rownum == 0:
                continue
            d = entry.to_dict()
            cols = []
            vals = []
            for key in d.keys():
                if key.startswith('gio'):
                    continue
                vals.append((d[key] if d[key] not in ['unknown', 'N/A']
                             else None))
                cols.append(translate.get(key, key))

            sql = """
                INSERT into %s(%s) VALUES (%s)
                """ % (table, ",".join(cols), ','.join(["%s"]*len(cols)))
            try:
                pcursor.execute(sql, vals)
            except Exception as exp:
                print("CSCAP harvest_management traceback")
                print(exp)
                for a, b in zip(cols, vals):
                    print("   |%s| -> |%s|" % (a, b))
                return
            added += 1

        print(("harvest_management %16s added:%4s deleted:%4s"
               ) % (sheetkey, added, deleted))

    pcursor.close()
    pgconn.commit()
    pgconn.close()


if __name__ == '__main__':
    main()
