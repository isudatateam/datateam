import pyiem.cscap_utils as util

drive = util.get_driveclient(util.get_config(), 'cscap')
spr_client = util.get_spreadsheet_client(util.get_config())

res = drive.files().list(q="title contains 'Plot Identifiers'").execute()
for item in res['items']:
    if item['mimeType'] != 'application/vnd.google-apps.spreadsheet':
        continue
    print item['title']
    spreadsheet = util.Spreadsheet(spr_client, item['id'])
    spreadsheet.get_worksheets()
    sheet = spreadsheet.worksheets['Sheet 1']
    for col in ['AGRO', 'SOIL', 'GHG', 'IPM_CSCAP', 'IPM_USB']:
        sheet.add_column(col)
