"""Build xref for maps and photos."""

import pandas as pd
from pyiem.database import get_dbconn
from pyiem.util import logger

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
                "('10wvuu-yEZL1M3Mg2Yaen_cKg334c4mDn' in parents or "
                "'1XGdNsU9x5RL_KrkvBc8bGOYU2W4v-3b7' in parents) and "
                "mimeType='application/vnd.google-apps.folder'"
            ),
            corpora="drive",
            driveId="0ABOgL0ZpGFeqUk9PVA",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
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
    if not rows:
        LOG.info("No rows found, exiting")
        return
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
