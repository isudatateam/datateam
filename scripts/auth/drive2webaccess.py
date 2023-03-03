"""
Synchronize the ACL on the Google Drive to the local DB
"""
import isudatateam.cscap_utils as utils
from pyiem.util import get_dbconn


def main():
    """Go Main Go."""
    pgconn = get_dbconn("sustainablecorn")
    cursor = pgconn.cursor()

    removed = 0
    config = utils.get_config()
    for project in ["cscap"]:
        cursor.execute(
            """
        SELECT access_level from website_access_levels where appid = %s
        """,
            (project,),
        )
        access_level = cursor.fetchone()[0]
        CURRENT = []
        cursor.execute(
            """
            SELECT email from website_users WHERE access_level = %s
            """,
            (access_level,),
        )
        for row in cursor:
            CURRENT.append(row[0])
        drive = utils.get_driveclient(config, project)
        perms = (
            drive.permissions()
            .list(fileId=config[project]["basefolder"])
            .execute()
        )
        for item in perms.get("items", []):
            # Unclear what type of permission this is that does not have this
            # set, maybe a file with an allow for anybody that has the link
            # to it
            if "emailAddress" not in item:
                continue
            email = item["emailAddress"].lower()
            if email not in CURRENT:
                print(
                    ("Adding email: '%s' project: '%s' for datateam access")
                    % (email, project)
                )
                cursor.execute(
                    """INSERT into website_users(email, access_level)
                VALUES (%s, %s)""",
                    (email, access_level),
                )
            else:
                CURRENT.remove(email)

        for email in CURRENT:
            print(
                ("Removing email: '%s' project: %s for datateam access")
                % (email, project)
            )
            cursor.execute(
                """
                DELETE from website_users where email = %s
                and access_level = %s""",
                (email, access_level),
            )
            removed += 1

    cursor.close()
    if removed < 20:
        pgconn.commit()
    else:
        print("Aborting database save as removed(%s) > 20" % (removed,))


if __name__ == "__main__":
    main()
