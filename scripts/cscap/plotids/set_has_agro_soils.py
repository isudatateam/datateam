"""set some metadata"""

from pyiem.util import get_dbconn

import isudatateam.cscap_utils as util


def main():
    """Go Main!"""
    pgconn = get_dbconn("sustainablecorn")
    cursor = pgconn.cursor()

    drive = util.get_driveclient(util.get_config(), "cscap")
    spr_client = util.get_spreadsheet_client(util.get_config())

    res = drive.files().list(q="title contains 'Plot Identifiers'").execute()
    for item in res["items"]:
        if item["mimeType"] != "application/vnd.google-apps.spreadsheet":
            continue
        site = item["title"].split()[0]
        print(site)
        cursor.execute(
            """SELECT distinct plotid from agronomic_data
        WHERE site = %s""",
            (site,),
        )
        agr_plotids = [row[0] for row in cursor]
        cursor.execute(
            """SELECT distinct plotid from soil_data
        WHERE site = %s""",
            (site,),
        )
        soil_plotids = [row[0] for row in cursor]
        spreadsheet = util.Spreadsheet(spr_client, item["id"])
        spreadsheet.get_worksheets()
        sheet = spreadsheet.worksheets["Sheet 1"]
        for entry in sheet.get_list_feed().entry:
            dirty = False
            data = entry.to_dict()
            res = "yes"
            if data["plotid"] not in agr_plotids:
                res = "no"
                # print("%s %s" % (data['plotid'], agr_plotids))
            if data["agro"] != res:
                print(
                    "  AGR  plotid: %s :: %s -> %s"
                    % (data["plotid"], data["agro"], res)
                )
                entry.set_value("agro", res)
                dirty = True

            res = "yes"
            if data["plotid"] not in soil_plotids:
                res = "no"
                # print("%s %s" % (data['plotid'], soil_plotids))
            if data["soil"] != res:
                print(
                    "  SOIL plotid: %s :: %s -> %s"
                    % (data["plotid"], data["soil"], res)
                )
                entry.set_value("soil", res)
                dirty = True
            if dirty:
                spr_client.update(entry)


if __name__ == "__main__":
    main()
