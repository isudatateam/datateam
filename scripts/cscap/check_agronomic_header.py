"""Gio discovered a mismatch between AG codes and headers"""

import pandas as pd

import isudatateam.cscap_utils as util

config = util.get_config()
drive = util.get_driveclient(config, "cscap")
sheets = util.get_sheetsclient(config, "cscap")

res = drive.files().list(q="title contains 'Agronomic Data'").execute()

IGNORE = [
    "Rep",
    "Tillage",
    "Rotation",
    "Drainage",
    "PlotID",
    "ROW",
    "COLUMN",
    "UniqueID",
    "Nitrogen",
    "Landscape",
]


def build_xref():
    f = sheets.spreadsheets().get(
        spreadsheetId="1PKK-vWuOryYFOSYSgt4TosrjIDX_F-opHOvrEo5q-i4",
        includeGridData=True,
    )
    j = util.exponential_backoff(f.execute)
    sheet = j["sheets"][0]
    griddata = sheet["data"][0]
    d = dict()
    for rowdata in griddata["rowData"]:
        c1 = rowdata["values"][0].get("formattedValue", "n/a")
        c2 = rowdata["values"][1].get("formattedValue", "n/a")
        d[c1.strip()] = c2.strip()
    return d


xref = build_xref()

rows = []
for item in res["items"]:
    if item["mimeType"] != "application/vnd.google-apps.spreadsheet":
        continue
    print("===> %s" % (item["title"],))
    site = item["title"].split()[0]
    f = sheets.spreadsheets().get(
        spreadsheetId=item["id"], includeGridData=True
    )
    j = util.exponential_backoff(f.execute)
    for sheet in j["sheets"]:
        sheet_title = sheet["properties"]["title"]
        print("    |-> %s" % (sheet_title,))
        for griddata in sheet["data"]:
            row1 = griddata["rowData"][0]
            row2 = griddata["rowData"][1]
            for c1, c2 in zip(row1["values"], row2["values"]):
                v1 = c1.get("formattedValue", "n/a").strip()
                v2 = c2.get("formattedValue", "n/a").strip()
                if v1 not in IGNORE and xref.get(v1, "") != v2:
                    print(
                        ("%s -> '%s'\n" "        should be '%s'")
                        % (v1, v2, xref.get(v1))
                    )
                    rows.append(
                        dict(
                            uniqueid=site,
                            year=sheet_title,
                            sdchas=xref.get(v1, ""),
                            sheethas=v2,
                        )
                    )

df = pd.DataFrame(rows)
df.to_csv(
    "report.csv",
    index=False,
    columns=["uniqueid", "year", "sheethas", "sdchas"],
)
