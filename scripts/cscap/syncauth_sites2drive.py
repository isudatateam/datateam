"""
Sync authorized users on Google Sites to Google Drive
"""
import pyiem.cscap_utils as util
import gdata.gauth
import gdata.sites.client as sclient

config = util.get_config()


def get_sites_client(config, site="sustainablecorn"):
    """Return an authorized sites client"""

    token = gdata.gauth.OAuth2Token(
        client_id=config["appauth"]["client_id"],
        client_secret=config["appauth"]["app_secret"],
        user_agent="daryl.testing",
        scope=config["googleauth"]["scopes"],
        refresh_token=config["googleauth"]["refresh_token"],
    )

    sites_client = sclient.SitesClient(site=site)
    token.authorize(sites_client)
    return sites_client


spr_client = get_sites_client(config)
service = util.get_driveclient(config)

site_users = []
for acl in spr_client.get_acl_feed().entry:
    userid = acl.scope.value
    if userid not in site_users:
        site_users.append(acl.scope.value)

# Get a listing of current permissions
perms = (
    service.permissions().list(fileId=config["cscap"]["folderkey"]).execute()
)
for item in perms.get("items", []):
    email = item["emailAddress"]
    if email in site_users:
        site_users.remove(email)
        continue
    print("Email: %s can access Drive, not sites" % (email,))

for loser in site_users:
    print(loser)
    # continue
    id_resp = service.permissions().getIdForEmail(email=loser).execute()
    id2 = id_resp["id"]
    print(
        ("Adding %s[%s] as writer to CSCAP Internal Documents Collection")
        % (loser, id2)
    )
    newperm = dict(
        id=id2, type="user", role="writer", sendNotificationEmails=False
    )
    res = (
        service.permissions()
        .insert(fileId=config["cscap"]["folderkey"], body=newperm)
        .execute()
    )
    print(res)
