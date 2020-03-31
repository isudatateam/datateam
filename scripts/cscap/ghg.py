"""One off."""
import datetime
import pyiem.cscap_utils as util

sprclient = util.get_spreadsheet_client(util.get_config())

spread = util.Spreadsheet(
    sprclient, "1FxKx0GDJxv_8fIjKe2xRJ58FGILLlUSXcb6EuSLQSrI"
)
spread.get_worksheets()
sheet = spread.worksheets["BRADFORD.A"]
for entry in sheet.get_list_feed().entry:
    d = entry.to_dict()
    if d["ghg2"] in [None, "Sampling Date", "-"]:
        continue
    ts = datetime.datetime.strptime(d["ghg2"][:15], "%a %b %d %Y")
    entry.set_value("ghg200", ts.strftime("%m/%d/%Y"))
    sprclient.update(entry)
