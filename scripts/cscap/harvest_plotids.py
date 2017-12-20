"""Get the Plot IDs harvested!"""
from __future__ import print_function
import sys

import pyiem.cscap_utils as util
from pyiem.util import get_dbconn


def main():
    """Go Main"""
    config = util.get_config()

    pgconn = get_dbconn('sustainablecorn')
    pcursor = pgconn.cursor()

    # Get me a client, stat
    spr_client = util.get_spreadsheet_client(config)
    drive_client = util.get_driveclient(config)

    res = drive_client.files().list(
            q="title contains 'Plot Identifiers'").execute()

    translate = {'column': 'col'}

    lookup = {'tillage': 'TIL',
              'rotation': 'ROT',
              'herbicide': 'HERB',
              'drainage': 'DWM',
              'nitrogen': 'NIT',
              'landscape': 'LND'}

    pcursor.execute("""DELETE from plotids""")
    removed = pcursor.rowcount
    added = 0
    sheets = 0
    for item in res['items']:
        if item['mimeType'] != 'application/vnd.google-apps.spreadsheet':
            continue
        sheets += 1
        spreadsheet = util.Spreadsheet(spr_client, item['id'])
        spreadsheet.get_worksheets()
        # A one off
        worksheet = spreadsheet.worksheets.get('Sheet 1_export')
        if worksheet is None:
            worksheet = spreadsheet.worksheets['Sheet 1']
        for entry2 in worksheet.get_list_feed().entry:
            data = entry2.to_dict()
            cols = []
            vals = []
            for key in data.keys():
                val = data[key]
                if val is None:
                    continue
                if key in lookup:
                    if data[key] is not None:
                        val = data[key].strip(
                            ).replace("[", "").replace("]", "").split()[0]
                        if val not in ['N/A', "n/a"]:
                            val = "%s%s" % (lookup.get(key, ''), val)
                if key == 'uniqueid':
                    val = val.upper()
                if key.startswith('no3') or key.startswith('_'):
                    continue
                vals.append(val)
                cols.append(translate.get(key, key))
            if not cols:
                print("No columns for '%s'?" % (item['title'], ))
                continue
            if 'uniqueid' not in cols:
                print("No uniqueid column for '%s'" % (item['title'],))
            sql = """
                INSERT into plotids(%s) VALUES (%s)
            """ % (",".join(cols), ','.join(["%s"]*len(cols)))
            try:
                pcursor.execute(sql, vals)
            except Exception as exp:
                print(exp)
                print(item['title'])
                print(cols)
                sys.exit()
            added += 1
            # One-time correction of missing nitrogen entries
            # if data['landscape'] == 'N/A':
            #    print("Updating %s %s for landscape" % (data['uniqueid'],
            #                                            data['plotid']))
            #    entry2.set_value('landscape', 'n/a')
            #    spr_client.update(entry2)

    print(("harvest_plotids, removed: %s, added: %s, sheets: %s"
           ) % (removed, added, sheets))
    if removed > (added + 10):
        print("harvest_plotids, aborting due to large difference")
        sys.exit()
    pcursor.close()
    pgconn.commit()
    pgconn.close()


if __name__ == '__main__':
    main()
