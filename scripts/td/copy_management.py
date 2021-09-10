"""Copy management entries from CSCAP to TD"""
import pyiem.cscap_utils as util
import gdata.spreadsheets.data

CSCAP_ID = "1CdPi6IEnO3T35t-OFQajKYzFLpu7hwOaWdL2FG1_kVA"
TD_ID = "1m797EoMbtAvasqum-pB5ykVMeHsGWwcVvre8MbvE-xM"

sprclient = util.get_spreadsheet_client(util.get_config())

cscap_spread = util.Spreadsheet(sprclient, CSCAP_ID)
cscap_spread.get_worksheets()
td_spread = util.Spreadsheet(sprclient, TD_ID)
td_spread.get_worksheets()

WANTED = {"SERF": "SERF_IA", "DPAC": "DPAC", "STJOHNS": "STJOHNS"}


def get_cscap_sheet(sheetname):
    sheet = cscap_spread.worksheets[sheetname]
    return sheet.get_list_feed()


def get_td_sheet(sheetname):
    sheet = td_spread.worksheets[sheetname]
    return sheet.get_list_feed()


def one2one(inname, outname, operations):
    sheetin = get_cscap_sheet(inname)
    for entry in sheetin.entry:
        d = entry.to_dict()
        if (
            d["uniqueid"] not in WANTED
            or d.get("operation", "all") not in operations
        ):
            continue
        entry = gdata.spreadsheets.data.ListEntry()
        for key in d.keys():
            entry.set_value(key, d[key])
        entry.set_value("uniqueid", WANTED[d["uniqueid"]])
        entry.set_value("calendaryear", d["cropyear"])
        sprclient.add_list_entry(
            entry, TD_ID, td_spread.worksheets[outname].id
        )


def do_notes():
    spread = util.Spreadsheet(
        sprclient, "1tQvw-TQFtBI6xcsbZpaCHtYF7oKZEXNXy-CZHBr8Lh8"
    )
    sheet = spread.worksheets["Research Site Metadata"]
    for entry in sheet.get_list_feed().entry:
        d = entry.to_dict()
        if d["uniqueid"] not in WANTED:
            continue
        for year in range(2011, 2016):
            entry = gdata.spreadsheets.data.ListEntry()
            entry.set_value("uniqueid", WANTED[d["uniqueid"]])
            entry.set_value("calendaryear", str(year))
            entry.set_value("cropyear", str(year))
            entry.set_value("notes", d["notes%s" % (year,)])
            entry.set_value("editedby", "akrherz@gmail.com")
            entry.set_value("updated", "4/20/2016 14:00")
            sprclient.add_list_entry(
                entry, TD_ID, td_spread.worksheets["Notes"].id
            )


if __name__ == "__main__":
    # do_notes()
    # one2one('Field Operations', 'Soil & Fert', ['fertilizer_synthetic',
    #                                            'soiladmend_lime'])
    # one2one('Field Operations', 'Plant & Harvest',
    #        ['harvest_corn', 'harvest_other', 'harvest_soy', 'harvest_wheat',
    #         'plant_corn', 'plant_other', 'plant_rye', 'plant_rye-corn-res',
    #         'plant_rye-soy-res', 'plant_soy', 'plant_wheat'])
    # one2one('Management', 'Residue Mngt', ['all'])
    # one2one('Pesticides', 'Pesticides', ['herbicide', ])
    one2one(
        "DWM",
        "DWM",
        [
            "all",
        ],
    )
