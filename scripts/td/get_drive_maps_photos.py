"""Build xref for maps and photos."""

from pyiem.cscap_utils import get_driveclient, get_config
from pyiem.util import get_dbconn, logger
import pandas as pd

LOG = logger()


def main():
    """Go Main Go."""
    config = get_config()
    drive = get_driveclient(config, "td")
    res = (
        drive.files()
        .list(
            q=(
                "'1MA6spcXyu_TeyZYkUSizks9fuQTSLC7m' in parents and "
                "mimeType='application/vnd.google-apps.folder'"
            )
        )
        .execute()
    )
    rows = []
    for item in res["items"]:
        siteid, typename = item["title"].rsplit("_", 1)
        rows.append({"siteid": siteid, "res": typename, "id": item["id"]})
    df = pd.DataFrame(rows)
    df = df.pivot("siteid", "res", "id")

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
