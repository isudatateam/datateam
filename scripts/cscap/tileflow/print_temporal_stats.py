"""print some time coverage stats"""

import psycopg2


def main():
    """Do Main Things"""
    pgconn = psycopg2.connect(database="sustainablecorn")
    cursor = pgconn.cursor()
    sites = []
    cursor.execute(
        """SELECT distinct uniqueid, plotid from tileflow_data
    ORDER by uniqueid, plotid"""
    )
    for row in cursor:
        sites.append([row[0], row[1]])
    for site, plotid in sites:
        # Does this site have sub-hourly data
        cursor.execute(
            """
        SELECT extract(minute from valid) as minute, count(*)
        from tileflow_data where uniqueid = %s and plotid = %s
        GROUP by minute
        """,
            (site, plotid),
        )
        interval = "1 hour"
        if cursor.rowcount == 4:
            interval = "15 minute"
        cursor.execute(
            """
        WITH potential as (
            select generate_series(min(valid), max(valid),
                                   '"""
            + interval
            + """'::interval) as ts
            from tileflow_data where uniqueid = %s and plotid = %s
        ),
        agg as (
            SELECT ts, discharge_mm_qc from
            potential p LEFT JOIN tileflow_data d on (p.ts = d.valid)
            WHERE d.uniqueid = %s and d.plotid = %s
        )
        SELECT count(*) from agg where discharge_mm_qc is null
        """,
            (site, plotid, site, plotid),
        )
        print(
            ("%s | %s | %s | %s")
            % (site, plotid, cursor.fetchone()[0], interval)
        )


if __name__ == "__main__":
    main()
