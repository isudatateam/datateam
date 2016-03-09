"""
Synchronize the ACL on the Google Drive to the local DB
"""
import pyiem.cscap_utils as utils
import psycopg2

CURRENT = []
pgconn = psycopg2.connect(database='sustainablecorn', host='iemdb')
cursor = pgconn.cursor()
cursor.execute("""SELECT email from website_users""")
for row in cursor:
    CURRENT.append(row[0])

config = utils.get_config()
ACL = []
for project in ['td', 'cscap']:
    drive = utils.get_driveclient(config, project)
    perms = drive.permissions().list(
                fileId=config[project]['basefolder']).execute()
    for item in perms.get('items', []):
        email = item['emailAddress'].lower()
        if email not in ACL:
            ACL.append(email)
        if email not in CURRENT:
            print("Adding email: '%s' for datateam access" % (email,))
            cursor.execute("""INSERT into website_users(email)
            VALUES (%s)""", (email,))
            CURRENT.append(email)

removed = 0
for email in CURRENT:
    if email in ACL:
        continue
    print("Removing email: '%s' for datateam access" % (email,))
    cursor.execute("""DELETE from website_users where email = %s""", (email,))
    removed += 1

cursor.close()
if removed < 10:
    pgconn.commit()
else:
    print("Aborting database save as removed(%s) > 10" % (removed,))
