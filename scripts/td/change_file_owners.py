"""We have files on the Google Drive that are solely owned by an *old* service
account.  This is not ideal as if this file is deleted, then it goes into
purgatory."""

from apiclient.discovery import build
from httplib2 import Http
from oauth2client.service_account import ServiceAccountCredentials

import isudatateam.cscap_utils as util

config = util.get_config()


def get_driveclient(config):
    """Return an authorized apiclient"""
    credentials = ServiceAccountCredentials.from_p12_keyfile(
        config["service_account"],
        "CSCAP-6886c10d6ffb.p12",
        scopes=["https://www.googleapis.com/auth/drive"],
    )

    http_auth = credentials.authorize(Http())

    return build("drive", "v2", http=http_auth)


drive = get_driveclient(config)
SA = config["service_account"]

res = drive.files().list(q="'%s' in owners", maxResults=999).execute()
print("Query found %s items" % (len(res["items"]),))
for item in res["items"]:
    owners = [a["emailAddress"] for a in item["owners"]]
    if len(owners) == 1 and owners[0] == SA:
        print("Updating %s" % (item["title"],))
        body = {"value": "gio@iastate.edu", "role": "owner", "type": "user"}
        permission = (
            drive.permissions().insert(fileId=item["id"], body=body).execute()
        )
    else:
        print("%s %s" % (item["title"], ",".join(owners)))
