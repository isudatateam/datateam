"""Process a file provided by Gio with the GHG data

"uniqueid","plotid","varname","method","position","date","subsample","value"

Note, set subsample equals to 1!, so pivot works
"""
import sys

import pandas as pd
import psycopg2
import StringIO


def main(argv):
    """Process a given filename"""
    filename = argv[1]
    df = pd.read_csv(filename)
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"])
    pdf = df.pivot_table(
        index=[
            "uniqueid",
            "plotid",
            "method",
            "position",
            "date",
            "subsample",
        ],
        columns="varname",
        values="value",
    )
    pdf.reset_index(inplace=True)
    pgconn = psycopg2.connect(database="sustainablecorn", host="iemdb")
    cursor = pgconn.cursor()
    cursor.execute("""DELETE from ghg_data""")
    print("Deleted %s rows" % (cursor.rowcount,))
    data = StringIO.StringIO()
    for row in pdf.itertuples(index=False):
        data.write(
            ("\t".join([str(s) for s in row]) + "\n").replace("nan", "\\N")
        )
    data.seek(0)
    cursor.copy_from(data, "ghg_data", columns=pdf.columns.tolist())
    print("Added %s rows" % (cursor.rowcount,))
    cursor.execute("""UPDATE ghg_data SET year = extract(year from date)""")
    cursor.close()
    pgconn.commit()


if __name__ == "__main__":
    main(sys.argv)
