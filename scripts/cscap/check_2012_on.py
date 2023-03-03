"""
  Look at the Agronomic Sheets and see if the number of rows match between
  2011 and the rest of the years
"""
import isudatateam.cscap_utils as util
from pyiem.util import logger
from gspread_pandas import Spread

LOG = logger()


def main():
    """Go Main Go."""
    config = util.get_config()

    drive = util.get_driveclient(config)

    res = drive.files().list(q="title contains 'Agronomic Data'").execute()

    for item in res["items"]:
        if item["mimeType"] != "application/vnd.google-apps.spreadsheet":
            continue
        LOG.debug(item["title"])
        spread = Spread(item["id"], config=config["cscap"]["service_account"])
        for sheet in spread.sheets:
            df = spread.sheet_to_df(index=None, sheet=sheet)
            LOG.debug("%s %s", sheet.title, len(df.index))


if __name__ == "__main__":
    main()
