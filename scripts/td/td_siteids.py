"""Initialize site ids for the IEM database."""

from pandas.io.sql import read_sql
from pyiem.util import get_dbconn, logger

LOG = logger()


def main():
    """Go Main Go."""
    mesosite = get_dbconn("mesosite")
    cursor = mesosite.cursor()
    cursor.execute("DELETE from stations where network = 'TD'")
    LOG.info("Removed %s current mesosite station entries", cursor.rowcount)
    tdpgconn = get_dbconn("td")
    tdsites = read_sql(
        "SELECT * from meta_site_history", tdpgconn, index_col="siteid"
    )
    for siteid, row in tdsites.iterrows():
        cursor.execute(
            "INSERT into stations (id, name, state, country, "
            "network, online, geom, metasite) VALUES (%s, %s, %s, 'US', "
            "'TD', 'f', %s, 't')",
            (
                siteid,
                row["official_farm_name"].replace(",", " ")[:64],
                row["state"],
                f"SRID=4326;POINT({row['longitude']} {row['latitude']})",
            ),
        )

    cursor.close()
    mesosite.commit()
    mesosite.close()


if __name__ == "__main__":
    main()
