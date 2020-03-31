"""Get notes and dump to a csv file

Not able to get Comments because of this:
https://issuetracker.google.com/issues/36756650
"""

import pyiem.cscap_utils as util
from tqdm import tqdm
import pandas as pd


def main():
    """Go!"""
    config = util.get_config()

    sheets = util.get_sheetsclient(config, "cscap")
    drive = util.get_driveclient(config)

    res = (
        drive.files()
        .list(
            q=(
                "title contains 'Soil Bulk Density' or "
                "title contains 'Soil Nitrate Data' or "
                "title contains 'Soil Texture Data' or "
                "title contains 'Agronomic Data'"
            ),
            maxResults=999,
        )
        .execute()
    )

    results = []
    for item in tqdm(res["items"]):
        if item["mimeType"] != "application/vnd.google-apps.spreadsheet":
            continue
        title = item["title"]
        f = sheets.spreadsheets().get(
            spreadsheetId=item["id"], includeGridData=True
        )
        j = util.exponential_backoff(f.execute)

        for sheet in j["sheets"]:
            sheet_title = sheet["properties"]["title"]
            for griddata in sheet["data"]:
                startcol = griddata.get("startColumn", 1)
                startrow = griddata.get("startRow", 1)
                header = []
                for row, rowdata in enumerate(griddata["rowData"]):
                    if "values" not in rowdata:  # empty sheet
                        continue
                    for col, celldata in enumerate(rowdata["values"]):
                        if row == 0:
                            header.append(
                                celldata.get("formattedValue", "n/a")
                            )
                        if celldata.get("note") is not None:
                            results.append(
                                {
                                    "title": title,
                                    "header": header[col],
                                    "sheet_title": sheet_title,
                                    "row": row + startrow + 1,
                                    "col": col + startcol + 1,
                                    "note": celldata["note"],
                                }
                            )

    df = pd.DataFrame(results)
    df.to_csv("notes.csv", sep="|")


if __name__ == "__main__":
    main()
