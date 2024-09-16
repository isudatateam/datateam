"""Verify that the Plot IDs are OK."""

import pandas as pd
import psycopg2
from pandas.io.sql import read_sql

import isudatateam.cscap_utils as utils


def main():
    """Go Main"""
    pgconn = psycopg2.connect(database="sustainablecorn")

    plotdf = read_sql(
        """
        SELECT upper(uniqueid) as uniqueid, plotid from plotids
        """,
        pgconn,
        index_col=None,
    )
    plotdf["ipm_usb"] = "no"

    drive = utils.get_driveclient(utils.get_config(), "cscap")

    spr_client = utils.get_spreadsheet_client(utils.get_config())

    res = (
        drive.files()
        .list(
            q=(
                "'0B4fyEPcRW7IscDcweEwxUFV3YkU' in parents and "
                "title contains 'USB'"
            )
        )
        .execute()
    )

    U = {}

    rows = []
    for item in res["items"]:
        if item["mimeType"] != "application/vnd.google-apps.spreadsheet":
            continue
        uniqueid = item["title"].strip().split()[-1]
        spreadsheet = utils.Spreadsheet(spr_client, item["id"])
        for worksheet in spreadsheet.worksheets:
            lf = spreadsheet.worksheets[worksheet].get_list_feed()
            for entry in lf.entry:
                d = entry.to_dict()
                rows.append(dict(uniqueid=uniqueid, plotid=d["plotid"]))

    plotipm = pd.DataFrame(rows)
    for _i, row in plotipm.iterrows():
        df2 = plotdf[
            (plotdf["uniqueid"] == row["uniqueid"])
            & (plotdf["plotid"] == row["plotid"])
        ]
        if df2.empty:
            key = "%s_%s" % (row["uniqueid"], row["plotid"])
            if key in U:
                continue
            U[key] = True
            print(
                "Missing uniqueid: |%s| plotid: |%s|"
                % (row["uniqueid"], row["plotid"])
            )
        else:
            plotdf.at[df2.index, "ipm_usb"] = "yes"

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
                res = df2["ipm_usb"].values[0]
            if data["ipmusb"] != res:
                print(
                    "  IPM_USB  plotid: %s :: %s -> %s"
                    % (data["plotid"], data["ipmusb"], res)
                )
                entry.set_value("ipmusb", res)
                spr_client.update(entry)


if __name__ == "__main__":
    main()
