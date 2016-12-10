"""Correct the labels used for soil depths in the sheets"""
import pyiem.cscap_utils as util

ALLOWED = ['depth (cm)', 'cm']

config = util.get_config()
spr_client = util.get_spreadsheet_client(config)
drive = util.get_driveclient(config)

# Fake last conditional to make it easy to reprocess one site...
res = drive.files().list(q=("(title contains 'Soil Bulk Density' or "
                            "title contains 'Soil Nitrate Data' or "
                            "title contains 'Soil Fertility Analysis Data' or "
                            "title contains 'Soil Texture Data') and "
                            "title contains 'Soil'"),
                         maxResults=999).execute()

sz = len(res['items'])
for i, item in enumerate(res['items']):
    if item['mimeType'] != 'application/vnd.google-apps.spreadsheet':
        continue
    spreadsheet = util.Spreadsheet(spr_client, item['id'])
    spreadsheet.get_worksheets()
    for year in spreadsheet.worksheets:
        print('%3i/%3i sheet "%s" for "%s"' % (i + 1, sz, year, item['title']))
        lf = spreadsheet.worksheets[year].get_list_feed()
        for entry in lf.entry:
            current = entry.get_value('depth')
            if (current is None or current.find("-") == -1 or
                    current in ALLOWED):
                continue
            tokens = [a.strip() for a in current.split("-")]
            if len(tokens) != 2:
                print("Unparsable %s" % (repr(current),))
                continue
            newval = "%s to %s" % (tokens[0], tokens[1])
            if newval != current:
                entry.set_value('depth', newval)
                print('    "%s" -> "%s"' % (current, newval))
                spr_client.update(entry)
