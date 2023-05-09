"""Change % moisture values into g kg-1"""
import re

import isudatateam.cscap_utils as util

NUM = re.compile("\d+\.?\d*")

config = util.get_config()
spr_client = util.get_spreadsheet_client(config)
drive = util.get_driveclient(config)

# Fake last conditional to make it easy to reprocess one site...
res = (
    drive.files()
    .list(q=("title contains 'Agronomic Data'"), maxResults=999)
    .execute()
)

WANT = ["AGR18", "AGR20", "AGR22"]
KNOWN = ["n/a", "did not collect", None]

sz = len(res["items"])
for i, item in enumerate(res["items"]):
    if item["mimeType"] != "application/vnd.google-apps.spreadsheet":
        continue
    spreadsheet = util.Spreadsheet(spr_client, item["id"])
    spreadsheet.get_worksheets()
    for year in spreadsheet.worksheets:
        print('%3i/%3i sheet "%s" for "%s"' % (i + 1, sz, year, item["title"]))
        lf = spreadsheet.worksheets[year].get_list_feed()
        for rownum, entry in enumerate(lf.entry):
            dirty = False
            data = entry.to_dict()
            for key, value in data.iteritems():
                if key.upper() not in WANT:
                    continue
                if value in KNOWN:
                    continue
                if value.strip().startswith("["):
                    continue
                if value == "%":
                    newvalue = "g kg-1"
                elif NUM.match(value):
                    newvalue = str(float(value.replace("%", "")) * 10.0)
                else:
                    print(f"what {value}")
                    newvalue = value
                if newvalue != value:
                    entry.set_value(key, newvalue)
                    print('    key:%s "%s" -> "%s"' % (key, value, newvalue))
                    dirty = True
            if dirty:
                util.exponential_backoff(spr_client.update, entry)
