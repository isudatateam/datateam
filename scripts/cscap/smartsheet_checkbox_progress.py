import isudatateam.cscap_utils as util

config = util.get_config()
ss = util.get_ssclient(config)

action = ss.Sheets.list_sheets(include_all=True)
for sheet in action.data:
    if not sheet.name.startswith("CSCAP Site Edits Needed_"):
        continue
    s = ss.Sheets.get_sheet(sheet.id)
    titles = []
    for col in s.columns:
        titles.append(col.title)
    idx = titles.index("DONE")
    sidx = titles.index("Site")
    hits = 0
    cnt = 0
    site = sheet.name.split("_", 1)[1]
    for row in s.rows:
        # Check Site Column to see if it is null
        if row.cells[sidx].value is None:
            continue
        # if row.cells[sidx].value != site:
        #    print("WHOA: site: %s row: %s" % (site, row.cells[sidx].value))
        if row.cells[idx].value is True:
            hits += 1
        cnt += 1
    print("%s,%s,%s,%.2f" % (site, hits, cnt, hits / float(cnt) * 100.0))
