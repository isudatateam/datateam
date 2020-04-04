"""Initialize site ids for the IEM database."""
import pyiem.cscap_utils as util
from pyiem.util import get_dbconn

pgconn = get_dbconn("mesosite")
cursor = pgconn.cursor()

config = util.get_config()

spr_client = util.get_spreadsheet_client(config)

spreadsheet = util.Spreadsheet(
    spr_client, "1oZ2NEmoa0XHSGTWKaBLt0DJK1kIbpWO6iSZ0I2OE2gA"
)
spreadsheet.get_worksheets()

lf = spreadsheet.worksheets["Research Site Metadata"].get_list_feed()
for entry in lf.entry:
    data = entry.to_dict()
    siteid = data["uniqueid"].strip()
    name = (
        "%s [%s]"
        % (data["farmfieldname"][: (61 - len(data["leadpi"]))], data["leadpi"])
    ).replace("\n", " ")
    if data["latitudedd.d"] == "TBD":
        print(f"Skipping {name} due to TBD location")
        continue
    cursor.execute(
        """SELECT climate_site from stations where id = %s
    and network = 'TD'
    """,
        (siteid,),
    )
    if cursor.rowcount == 0:
        cursor.execute(
            """INSERT into stations (id, name, state, country,
    network, online, geom, metasite) VALUES (%s, %s, %s, 'US',
    'TD', 'f', %s, 't')""",
            (
                siteid,
                name,
                data["state"],
                "SRID=4326;POINT(%s %s)"
                % (data["longitudeddd.d"], data["latitudedd.d"]),
            ),
        )
    else:
        cursor.execute(
            """UPDATE stations SET name = %s
        WHERE id = %s and network = 'TD' RETURNING climate_site
        """,
            (name, siteid),
        )
        climatesite = cursor.fetchone()[0]
        if (
            climatesite is not None
            and climatesite != ""
            and climatesite != data["iemclimatesite"]
        ):
            entry.set_value("iemclimatesite", climatesite)
            print(f"Setting climate site: {climatesite} for site: {siteid}")
            util.exponential_backoff(spr_client.update, entry)

cursor.close()
pgconn.commit()
pgconn.close()
