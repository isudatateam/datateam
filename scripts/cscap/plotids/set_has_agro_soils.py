import pyiem.cscap_utils as util
import psycopg2

pgconn = psycopg2.connect(database='sustainablecorn', host='iemdb',
                          user='nobody')
cursor = pgconn.cursor()

drive = util.get_driveclient(util.get_config(), 'cscap')
spr_client = util.get_spreadsheet_client(util.get_config())

res = drive.files().list(q="title contains 'Plot Identifiers'").execute()
for item in res['items']:
    if item['mimeType'] != 'application/vnd.google-apps.spreadsheet':
        continue
    site = item['title'].split()[0]
    print site
    cursor.execute("""SELECT distinct plotid from agronomic_data
    WHERE site = %s""", (site, ))
    agr_plotids = [row[0] for row in cursor]
    cursor.execute("""SELECT distinct plotid from soil_data
    WHERE site = %s""", (site, ))
    soil_plotids = [row[0] for row in cursor]
    spreadsheet = util.Spreadsheet(spr_client, item['id'])
    spreadsheet.get_worksheets()
    sheet = spreadsheet.worksheets['Sheet 1']
    for entry in sheet.get_list_feed().entry:
        data = entry.to_dict()
        res = 'yes'
        if data['plotid'] not in agr_plotids:
            res = 'no'
            print data['plotid'], agr_plotids
        if data['agro'] != res:
            print("  AGR  plotid: %s :: %s -> %s" % (data['plotid'],
                                                     data['agro'], res))
            entry.set_value('agro', res)
            spr_client.update(entry)

        res = 'yes'
        if data['plotid'] not in soil_plotids:
            res = 'no'
            print data['plotid'], soil_plotids
        if data['soil'] != res:
            print("  SOIL plotid: %s :: %s -> %s" % (data['plotid'],
                                                     data['soil'], res))
            entry.set_value('soil', res)
            spr_client.update(entry)
