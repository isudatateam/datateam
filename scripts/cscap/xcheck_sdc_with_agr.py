"""
 Use the Site Data Collected and then see what columns exist within the
 Agronomic Data Sheets.
"""
import copy
import sys

import isudatateam.cscap_utils as util

YEAR = sys.argv[1]

config = util.get_config()

spr_client = util.get_spreadsheet_client(config)

sdc_feed = spr_client.get_list_feed(config["cscap"]["sdckey"], "od6")
sdc, sdc_names = util.build_sdc(sdc_feed)

drive_client = util.get_driveclient(config)


def adjust_sdc(sitekey, varname):
    """Change what we do here"""
    for entry in spr_client.get_list_feed(
        "1PKK-vWuOryYFOSYSgt4TosrjIDX_F-opHOvrEo5q-i4", "od6"
    ).entry:
        d = entry.to_dict()
        if d["key"] != varname:
            continue
        val = d[sitekey].strip().upper()
        years = []
        for yr in range(11, 16):
            if val.find(str(yr)) > -1 or val == "X":
                years.append(2000 + int(yr))
        years.remove(int(YEAR))
        s = "X (%s)" % (", ".join(["'%s" % (y - 2000,) for y in years]),)
        if len(years) == 0:
            s = ""
        entry.set_value(sitekey, s)
        print("%s %s %s -> %s" % (sitekey, varname, val, s))
        spr_client.update(entry)


res = drive_client.files().list(q="title contains 'Agronomic Data'").execute()
for item in res["items"]:
    if item["mimeType"] != "application/vnd.google-apps.spreadsheet":
        continue
    spreadsheet = util.Spreadsheet(spr_client, item["id"])
    sitekey = item["title"].split()[0].lower()
    print("------------> %s [%s] [%s]" % (YEAR, sitekey, item["title"]))
    if YEAR not in spreadsheet.worksheets:
        print("%s does not have Year: %s in worksheet" % (sitekey, YEAR))
        continue
    worksheet = spreadsheet.worksheets[YEAR]
    worksheet.get_list_feed()
    if len(worksheet.list_feed.entry) == 0:
        print("    EMPTY sheet, skipping")
        continue
    entry2 = worksheet.list_feed.entry[0]
    data = entry2.to_dict()
    keys = data.keys()
    shouldhave = copy.deepcopy(sdc[YEAR][sitekey])
    error = False
    for key in keys:
        if not key.upper().startswith("AGR"):
            continue
        varname = key.upper()
        vals = []
        for entry4 in worksheet.list_feed.entry[2:]:
            d = entry4.to_dict()
            if d[key] not in vals:
                vals.append(d[key])
        if varname not in shouldhave:
            print("EXTRA %s %s" % (varname, vals))
            if input("DELETE? y/n ") == "y":
                print("Deleting...")
                worksheet.del_column(varname)
                worksheet.get_list_feed()
            continue
        else:
            shouldhave.remove(varname)
        for _v in ["n/a", "did not collect"]:
            if _v in vals:
                vals.remove(_v)
        if len(vals) == 0:
            print("DELETING: %s" % (varname,), vals)
            worksheet.del_column(varname)
            worksheet.get_list_feed()
            adjust_sdc(sitekey, varname)

    for sh in shouldhave:
        if sh.startswith("AGR"):
            print("SHOULDHAVE %s" % (sh,))
            error = True
