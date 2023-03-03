"""
 List out the folders on the Google Drive and see who their owners are!
"""
import isudatateam.cscap_utils as util

config = util.get_config()
drive = util.get_driveclient(config)

res = (
    drive.files()
    .list(q="mimeType = 'application/vnd.google-apps.folder'", maxResults=999)
    .execute()
)

for item in res["items"]:
    for owner in item["owners"]:
        if owner["emailAddress"] != "cscap.automation@gmail.com":
            print("%s %s" % (item["title"], owner["emailAddress"]))
