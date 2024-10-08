"""Remove a column from all Agronomic Sheets!"""

import isudatateam.cscap_utils as util


def main():
    """Go Main Go."""
    config = util.get_config("cscap")

    # Get me a client, stat
    spr_client = util.get_spreadsheet_client(config)
    drive_client = util.get_driveclient(config)

    res = (
        drive_client.files()
        .list(q="title contains 'Agronomic Data'")
        .execute()
    )

    for item in res["items"]:
        spreadsheet = util.Spreadsheet(spr_client, item["id"])
        for yr in ["2011", "2012", "2013", "2014", "2015"]:
            spreadsheet.worksheets[yr].del_column("AGR392")


if __name__ == "__main__":
    main()
