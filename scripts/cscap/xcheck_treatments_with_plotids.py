"""Fill in missing entries in the plotid sheets"""
import isudatateam.cscap_utils as util

TRTKEY = "DWM"
COLKEY = "drainage"


def get_just_code(text):
    """Just the facts ma'am"""
    if text == "":
        return ""
    return text.split()[0]


def main():
    """Go Main!"""
    config = util.get_config()

    spr_client = util.get_spreadsheet_client(config)
    drive_client = util.get_driveclient(config)

    treat_feed = spr_client.get_list_feed(config["cscap"]["treatkey"], "od6")

    treatments, treatment_names = util.build_treatments(treat_feed)

    res = (
        drive_client.files()
        .list(q="title contains 'Plot Identifiers'")
        .execute()
    )

    for item in res["items"]:
        if item["mimeType"] != "application/vnd.google-apps.spreadsheet":
            continue
        spreadsheet = util.Spreadsheet(spr_client, item["id"])
        print("Processing '%s'..." % (item["title"],))
        spreadsheet.get_worksheets()
        worksheet = spreadsheet.worksheets["Sheet 1"]
        for entry in worksheet.get_list_feed().entry:
            data = entry.to_dict()
            sitekey = data.get("uniqueid").lower()
            if sitekey is None:
                continue
            trt = treatments[sitekey]
            if data[COLKEY] is not None and data[COLKEY] != "":
                continue
            if len(trt[TRTKEY]) != 2:
                print("can't deal with this: %s" % (trt[TRTKEY],))
                break
            newval = treatment_names.get(trt[TRTKEY][1], "")
            entry.set_value(COLKEY, newval)
            print(
                ("Setting plotid: %s uniqueid: %s column:%s to %s")
                % (data.get("plotid"), sitekey, COLKEY, newval)
            )
            spr_client.update(entry)


if __name__ == "__main__":
    main()
