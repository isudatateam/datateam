"""Multiply SOIL13 and SOIL14 by 10 to change units from % to g/kg"""

import pyiem.cscap_utils as util


def main():
    """Go Main Go"""
    config = util.get_config()
    spr_client = util.get_spreadsheet_client(config)
    drive = util.get_driveclient(config)

    # Fake last conditional to make it easy to reprocess one site...
    res = (
        drive.files()
        .list(q=("title contains 'Soil Texture Data'"), maxResults=999)
        .execute()
    )

    HEADERS = [
        "uniqueid",
        "plotid",
        "depth",
        "tillage",
        "rotation",
        "soil6",
        "nitrogen",
        "drainage",
        "rep",
        "subsample",
        "landscape",
        "notes",
        "herbicide",
        "sampledate",
    ]

    sz = len(res["items"])
    for i, item in enumerate(res["items"]):
        if item["mimeType"] != "application/vnd.google-apps.spreadsheet":
            continue
        spreadsheet = util.Spreadsheet(spr_client, item["id"])
        spreadsheet.get_worksheets()
        for year in spreadsheet.worksheets:
            print(
                '%3i/%3i sheet "%s" for "%s"'
                % (i + 1, sz, year, item["title"])
            )
            lf = spreadsheet.worksheets[year].get_list_feed()
            for rownum, entry in enumerate(lf.entry):
                dirty = False
                data = entry.to_dict()
                for key in ["soil13", "soil14"]:
                    if key not in data:
                        continue
                    value = data[key]
                    if rownum == 1 and value == "%":
                        print("updating % to g/kg")
                        entry.set_value(key, "g/kg")
                        dirty = True
                        continue
                    if rownum >= 2:
                        try:
                            newvalue = float(value) * 10.0
                        except Exception:
                            continue
                        print("%s updating %s to %s" % (key, value, newvalue))
                        entry.set_value(key, "%.4f" % (newvalue,))
                        dirty = True
                if dirty:
                    util.exponential_backoff(spr_client.update, entry)


if __name__ == "__main__":
    main()
