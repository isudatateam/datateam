"""Assign eco regions, I got the shapefile here:
ftp://ftp.epa.gov/wed/ecoregions/us
"""
import psycopg2
import pyiem.cscap_utils as util

pgconn = psycopg2.connect(database="akrherz")
cursor = pgconn.cursor()

config = util.get_config()
spr_client = util.get_spreadsheet_client(config)

feed = spr_client.get_list_feed(
    "1tQvw-TQFtBI6xcsbZpaCHtYF7oKZEXNXy-CZHBr8Lh8", "od6"
)

for entry in feed.entry:
    data = entry.to_dict()
    cursor.execute(
        """select us_l4code, us_l4name, l4_key from echoregions_l4
    WHERE ST_Contains(ST_Transform(geom, 4326),
     ST_SetSRID(ST_GeomFromText('POINT(%s %s)'), 4326))
     """,
        (float(data["longitude"]), float(data["latitude"])),
    )
    newval = cursor.fetchone()[2]
    print("%s %s %s" % (data["latitude"], data["longitude"], newval))
    entry.set_value("epaecoregionlevel4codeandname", newval)
    spr_client.update(entry)
