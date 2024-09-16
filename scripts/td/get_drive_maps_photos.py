"""Build xref for maps and photos.

alter table meta_site_history add drive_maps_folder text;
alter table meta_site_history add drive_photos_folder text;
"""

import pandas as pd
from pyiem.util import get_dbconn, logger

from isudatateam.cscap_utils import get_config, get_driveclient

LOG = logger()


def main():
    """Go Main Go."""
    config = get_config()
    drive = get_driveclient(config, "td")
    res = (
        drive.files()
        .list(
            q=(
                "('1Xbv-V9xQLc2sYcCO30lyKQhjpizXoE4g' in parents or "
                "'1bm9WXTfeFkSCXILU6qBeVskTn_Do2SeQ' in parents) and "
                "mimeType='application/vnd.google-apps.folder'"
            )
        )
        .execute()
    )
    rows = []
    for item in res["items"]:
        tokens = item["title"].rsplit("_", 1)
        if len(tokens) != 2:
            LOG.info("Unknown title %s", item["title"])
            continue
        siteid, typename = tokens
        rows.append({"siteid": siteid, "res": typename, "id": item["id"]})
    df = pd.DataFrame(rows)
    df = df.pivot(index="siteid", columns="res", values="id")

    pgconn = get_dbconn("td")
    cursor = pgconn.cursor()
    for siteid, row in df.iterrows():
        cursor.execute(
            "UPDATE meta_site_history SET drive_maps_folder = %s, "
            "drive_photos_folder = %s where siteid = %s",
            (row["maps"], row["photos"], siteid),
        )
        if cursor.rowcount != 1:
            LOG.info("failed update for |%s|", siteid)
    cursor.close()
    pgconn.commit()


if __name__ == "__main__":
    main()
