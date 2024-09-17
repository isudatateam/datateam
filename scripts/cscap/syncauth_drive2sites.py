"""Copy drive permissions to site."""

import gdata.acl.data
import gdata.data
import gdata.gauth
import gdata.sites.client as sclient
import gdata.sites.data
from gdata import gauth

import isudatateam.cscap_utils as util


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


def main():
    """Go Main."""
    # Get Sites Users
    config = util.get_config()

    _token = gauth.OAuth2Token(
        client_id=config["appauth"]["client_id"],
        client_secret=config["appauth"]["app_secret"],
        user_agent="daryl.testing",
        scope=config["googleauth"]["scopes"],
        refresh_token=config["googleauth"]["refresh_token"],
    )

    # Get Google Drive Users
    drive = util.get_driveclient(config)
    perms = (
        drive.permissions().list(fileId=config["cscap"]["folderkey"]).execute()
    )
    drive_users = []
    for item in perms.get("items", []):
        email = item["emailAddress"]
        drive_users.append(email.lower())

    sites_client = get_sites_client(config)
    site_users = []
    for acl in sites_client.get_acl_feed().entry:
        _userid = acl.scope.value
        site_users.append(acl.scope.value.lower())

    # Compute the delta
    print(
        (
            "The following emails have access to Google Drive, "
            "but not Internal Site"
        )
    )
    acls = []
    for loser in drive_users:
        if loser in site_users:
            continue
        print(loser)
        acls.append(
            gdata.sites.data.AclEntry(
                scope=gdata.acl.data.AclScope(value=loser, type="user"),
                role=gdata.acl.data.AclRole(value="writer"),
                batch_operation=gdata.data.BatchOperation(type="insert"),
            )
        )


if __name__ == "__main__":
    main()
