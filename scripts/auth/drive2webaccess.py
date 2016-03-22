"""
Synchronize the ACL on the Google Drive to the local DB
"""
import pyiem.cscap_utils as utils
import psycopg2

pgconn = psycopg2.connect(database='sustainablecorn', host='iemdb')
cursor = pgconn.cursor()


removed = 0
config = utils.get_config()
for project in ['td', 'cscap']:
    cursor.execute("""
    SELECT access_level from website_access_levels where appid = %s
    """, (project, ))
    access_level = cursor.fetchone()[0]
    CURRENT = []
    cursor.execute("""
        SELECT email from website_users WHERE access_level = %s
        """, (access_level, ))
    for row in cursor:
        CURRENT.append(row[0])
    drive = utils.get_driveclient(config, project)
    perms = drive.permissions().list(
                fileId=config[project]['basefolder']).execute()
    for item in perms.get('items', []):
        email = item['emailAddress'].lower()
        if email not in CURRENT:
            print(("Adding email: '%s' project: '%s' for datateam access"
                   ) % (email, project))
            cursor.execute("""INSERT into website_users(email, access_level)
            VALUES (%s, %s)""", (email, access_level))
        else:
            CURRENT.remove(email)

    for email in CURRENT:
        print(("Removing email: '%s' project: %s for datateam access"
               ) % (email, project))
        cursor.execute("""
            DELETE from website_users where email = %s
            and access_level = %s""", (email, access_level))
        removed += 1

cursor.close()
if removed < 20:
    pgconn.commit()
else:
    print("Aborting database save as removed(%s) > 20" % (removed,))
