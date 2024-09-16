"""Check ghg plotids"""

import pandas as pd
import psycopg2
from pandas.io.sql import read_sql

import isudatateam.cscap_utils as utils


def main():
    """Do Main"""
    pgconn = psycopg2.connect(database="sustainablecorn")

    plotdf = read_sql(
        """SELECT upper(uniqueid) as uniqueid,
        upper(plotid) as plotid from plotids""",
        pgconn,
        index_col=None,
    )
    plotdf["ghg"] = "no"

    drive = utils.get_driveclient(utils.get_config(), "cscap")
    spr_client = utils.get_spreadsheet_client(utils.get_config())

    X = {
        "2011": "1DSHcfeBNJArVowk0CG_YvzI0YQnHIZXhcxOjBi2fPM4",
        "2012": "1ax_N80tIBBKEnWDnrGxsK4KUa1ssokwMxQZNVHEWWM8",
        "2013": "1UY5JYKlBHDElwljnEC-1tF7CozplAfyGpbkbd0OFqsA",
        "2014": "12NqffqVMQ0M4PMT_CP5hYfmydC-vzXQ0lFHbKwwfqzg",
        "2015": "1FxKx0GDJxv_8fIjKe2xRJ58FGILLlUSXcb6EuSLQSrI",
    }

    years = X.keys()
    years.sort()
    unknown = ["UniqueID_PlotID", "-_-"]
    rows = []
    for year in years:
        spreadsheet = utils.Spreadsheet(spr_client, X.get(year))
        for worksheet in spreadsheet.worksheets:
            lf = spreadsheet.worksheets[worksheet].get_list_feed()
            for entry in lf.entry:
                d = entry.to_dict()
                if d.get("uniqueid") is None or d.get("plotid") is None:
                    continue
                rows.append(d)
                df2 = plotdf[
                    (plotdf["uniqueid"] == d["uniqueid"].upper())
                    & (plotdf["plotid"] == d["plotid"].upper())
                ]
                if len(df2.index) == 0:
                    key = "%s_%s" % (d["uniqueid"], d["plotid"])
                    if key in unknown:
                        continue
                    unknown.append(key)
                    print(
                        ("%s[%s] Unknown uniqueid: |%s| plotid: |%s|")
                        % (year, worksheet, d["uniqueid"], d["plotid"])
                    )
                else:
                    idx = plotdf[
                        (plotdf["uniqueid"] == d["uniqueid"].upper())
                        & (plotdf["plotid"] == d["plotid"].upper())
                    ].index
                    plotdf.at[idx, "ghg"] = "yes"

    df = pd.DataFrame(rows)
    with pd.ExcelWriter("output.xlsx") as writer:
        df.to_excel(writer, sheet_name="Sheet1")

    res = drive.files().list(q="title contains 'Plot Identifiers'").execute()
    for item in res["items"]:
        if item["mimeType"] != "application/vnd.google-apps.spreadsheet":
            continue
        site = item["title"].split()[0]
        print(site)
        spreadsheet = utils.Spreadsheet(spr_client, item["id"])
        spreadsheet.get_worksheets()
        sheet = spreadsheet.worksheets["Sheet 1"]
        for entry in sheet.get_list_feed().entry:
            data = entry.to_dict()
            df2 = plotdf[
                (plotdf["uniqueid"] == site)
                & (plotdf["plotid"] == data["plotid"])
            ]
            res = "no"
            if len(df2.index) == 1:
                res = df2["ghg"].values[0]
            if data["ghg"] != res:
                print(
                    "  GHG  plotid: %s :: %s -> %s"
                    % (data["plotid"], data["ghg"], res)
                )
                entry.set_value("ghg", res)
                spr_client.update(entry)


if __name__ == "__main__":
    main()
