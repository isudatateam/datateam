"""Set our Google Drive folder to readonly."""
import isudatateam.cscap_utils as utils


def main():
    """Go Main Go."""
    config = utils.get_config()
    drive = utils.get_driveclient(config, "cscap")
    perms = (
        drive.permissions()
        .list(fileId=config["cscap"]["basefolder"])
        .execute()
    )
    for item in perms.get("items", []):
        # Unclear what type of permission this is that does not have this
        # set, maybe a file with an allow for anybody that has the link to it
        if "emailAddress" not in item:
            continue
        email = item["emailAddress"].lower()
        res = input(
            "%s id: %s role: %s revoke?[y]" % (email, item["id"], item["role"])
        )
        if res == "":
            drive.permissions().update(
                fileId=config["cscap"]["basefolder"],
                permissionId=item["id"],
                body={"role": "reader"},
            ).execute()
            print("Del")


if __name__ == "__main__":
    main()
