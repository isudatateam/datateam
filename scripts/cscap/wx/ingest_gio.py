"""Process a file kindly provided by Gio"""

import pandas as pd
from pyiem.util import get_dbconn


def main():
    """Go Main Go"""
    df = pd.read_csv('/tmp/cscap_onsite_weather.csv', na_values='NA')

    pgconn = get_dbconn('sustainablecorn')
    cursor = pgconn.cursor()
    cursor.execute("TRUNCATE weather_data_daily ")
    for _i, row in df.iterrows():
        cursor.execute("""
        INSERT into weather_data_daily(station, valid, high, low, precip,
        sknt, srad_mj, drct) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (row['station'], row['day'], row['high'], row['low'],
              row['precip'], row['sknt'], row['srad'], row['drct']))

    cursor.close()
    pgconn.commit()


if __name__ == '__main__':
    main()
