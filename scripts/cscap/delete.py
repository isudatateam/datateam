"""Delete a Google Drive File."""

import sys

import isudatateam.cscap_utils as util


def main():
    """Go Main Go."""
    config = util.get_config()
    drive = util.get_driveclient(config)

    print(drive.files().delete(fileId=sys.argv[1]).execute())


if __name__ == "__main__":
    main()
