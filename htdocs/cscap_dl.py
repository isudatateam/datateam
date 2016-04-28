#!/usr/bin/python
"""

"""
import sys
import psycopg2.extras
import cgi
import datetime
import pandas as pd
import pandas.io.sql as pdsql
import pyiem.cscap_utils as util


config = util.get_config(
    '/mesonet/www/apps/datateam/config/mytokens.json')


def clean(val):
    ''' Clean the value we get '''
    if val is None:
        return ''
    if val.strip().lower() == 'did not collect':
        return 'DNC'
    if val.strip().lower() == 'n/a':
        return 'NA'
    return val.decode('ascii', 'ignore')


def check_auth(form):
    """ Make sure request is authorized """
    if form.getfirst('hash') != config['appauth']['sharedkey']:
        sys.stdout.write("Content-type: text/plain\n\n")
        sys.stdout.write("Unauthorized request!")
        sys.stderr.write(("Unauthorized CSCAP hash=%s"
                          ) % (form.getfirst('hash'),))
        sys.exit()


def get_nitratedata():
    ''' Fetch some nitrate data, for now '''
    pgconn = psycopg2.connect(database='sustainablecorn', host='iemdb',
                              user='nobody')
    cursor = pgconn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    res = "uniqueid,plotid,year,depth,soil15,soil16,soil23\n"
    cursor.execute("""SELECT site, plotid, depth, varname, year, value
    from soil_data WHERE value is not null
    and value ~* '[0-9\.]' and value != '.' and value !~* '<'
    and site in ('MASON', 'KELLOGG', 'GILMORE', 'ISUAG', 'WOOSTER.COV',
    'SEPAC', 'BRADFORD.C', 'BRADFORD.B1', 'BRADFORD.B2', 'FREEMAN')""")
    data = {}
    for row in cursor:
        key = "%s|%s|%s|%s" % (row['site'], row['plotid'], row['year'],
                               row['depth'])
        if key not in data:
            data[key] = {}
        data[key][row['varname']] = clean(row["value"])

    for key in data.keys():
        tokens = key.split("|")
        res += ("%s,%s,%s,%s,%s,%s,%s\n"
                ) % (tokens[0], tokens[1], tokens[2], tokens[3],
                     data[key].get('SOIL15', ''), data[key].get('SOIL16', ''),
                     data[key].get('SOIL23', ''))
    return res


def get_agdata():
    """A specialized report"""
    pgconn = psycopg2.connect(database='sustainablecorn', host='iemdb',
                              user='nobody')
    cursor = pgconn.cursor()

    SITES = ['MASON', 'KELLOGG', 'GILMORE', 'ISUAG', 'WOOSTER.COV',
             'SEPAC', 'FREEMAN', 'BRADFORD.C',
             'BRADFORD.B1', 'BRADFORD.B2']

    # Load up baseline
    df = pdsql.read_sql("""
    with years as (SELECT generate_series(2011, 2015) as year),
    p as (SELECT uniqueid, plotid, rotation, nitrogen, rep, tillage
    from plotids
    WHERE uniqueid in %s)

    SELECT uniqueid, plotid, year, rotation, nitrogen, rep, tillage
    from years, p
    """, pgconn, params=(tuple(SITES),), index_col=None)
    df['plant_corn_date'] = None
    df['termination_rye_corn_date'] = None

    cursor.execute("""
    select uniqueid, valid, operation from operations
    where uniqueid in %s
    and operation in ('plant_corn', 'termination_rye_corn')
    """, (tuple(SITES),))
    for row in cursor:
        a = df[(df['uniqueid'] == row[0]) &
               (df['year'] == row[1].year)]
        df.loc[a.index.values, "%s_date" % (row[2], )] = row[1]

    # Get yield data
    cursor.execute("""
    SELECT site, plotid, varname, year, value from agronomic_data
    WHERE site in %s and
    varname in ('AGR7', 'AGR39', 'AGR16', 'AGR17', 'AGR19')
    and value not in ('', '.', 'n/a', 'did not collect')
    """, (tuple(SITES), ))
    for row in cursor:
        a = df[(df['uniqueid'] == row[0]) &
               (df['plotid'] == row[1]) &
               (df['year'] == row[3])]
        df.loc[a.index.values, row[2]] = row[4]

    # Get the Soil Nitrate Data
    cursor.execute("""
    SELECT site, plotid, depth, year, sampledate, avg(value::numeric) from
    soil_data where site in %s and varname = 'SOIL15'
    and value not in ('', '.', 'n/a', 'did not collect') and value is not null
    and substr(value, 1, 1) != '<'
    GROUP by site, plotid, depth, year, sampledate
    ORDER by sampledate ASC
    """, (tuple(SITES), ))
    for row in cursor:
        season = 'spring' if row[4].month < 7 else 'fall'
        a = df[(df['uniqueid'] == row[0]) &
               (df['plotid'] == row[1]) &
               (df['year'] == row[3])]
        df.loc[a.index.values, '%s Soil15 Date' % (season,)] = row[4]
        df.loc[a.index.values, '%s Soil15 %s' % (season,
                                                 row[2])] = row[5]

    return df.to_csv(index=False)


def get_dl(form):
    """ Process the form provided to us from the Internal website """
    pgconn = psycopg2.connect(database='sustainablecorn', host='iemdb',
                              user='nobody')

    years = form.getlist('years')
    if len(years) == 1:
        years.append('9')
    if "all" in years or len(years) == 0:
        yrlist = "('2011', '2012', '2013', '2014', '2015')"
    else:
        yrlist = str(tuple(years))

    treatlimiter = "1=1"
    treatments = form.getlist('treatments')
    if len(treatments) > 0 and 'all' not in treatments:
        if len(treatments) == 1:
            treatments.append('ZZ')
        s = str(tuple(treatments))
        treatlimiter = """(tillage in %s or rotation in %s or
        nitrogen in %s or landscape in %s)""" % (s, s, s, s)

    sitelimiter = "1=1"
    sites = form.getlist('sites')
    if len(sites) > 0 and 'all' not in sites:
        if len(sites) == 1:
            sites.append('ZZ')
        s = str(tuple(sites))
        treatlimiter = """t.site in %s""" % (s,)

    # columns!
    cols = ['year', 'site', 'plotid', 'depth', 'subsample', 'rep', 'rotation',
            'crop', 'tillage', 'drainage', 'nitrogen', 'landscape',
            'herbicide', 'sampledate']
    dvars = form.getlist("data")
    wants_soil = False
    for dv in dvars:
        if dv.startswith('SOIL') or dv == 'all':
            wants_soil = True
            break

    sys.stderr.write("1. %s\n" % (datetime.datetime.now(), ))
    if wants_soil:
        sql = """
        WITH ad as
        (SELECT site, plotid, ''::text as depth, varname, year,
        value, '1'::text as subsample, null::date as sampledate
         from agronomic_data WHERE year in %s),
        sd as
        (SELECT site, plotid, depth, varname, year, value, subsample,
        sampledate from soil_data WHERE year in %s),
        tot as
        (SELECT * from ad UNION select * from sd)

        SELECT site || '|' || p.plotid
        || '|' || coalesce(depth,'')
        || '|' || coalesce(subsample, '')
        || '|' || year
        || '|' || coalesce(rep, '')
        || '|' || coalesce(rotation, '')
        || '|' || coalesce(tillage, '')
        || '|' || coalesce(drainage, '')
        || '|' || coalesce(nitrogen, '')
        || '|' || coalesce(landscape, '')
        || '|' || coalesce(herbicide, '')
        || '|' || (case when sampledate is null then ''
                   else sampledate::text end) as lbl,
        varname, value
        from tot t JOIN plotids p on (t.site = p.uniqueid and
        t.plotid = p.plotid) WHERE 1=1 and %s and %s
        """ % (yrlist, yrlist, treatlimiter, sitelimiter)
    else:
        sql = """
        WITH ad as
        (SELECT site, plotid, ''::text as depth, varname, year,
        value, ''::text as subsample, ''::text as sampledate
         from agronomic_data WHERE year in %s),
        tot as
        (SELECT * from ad)

        SELECT site || '|' || p.plotid
        || '|' || coalesce(depth,'')
        || '|' || coalesce(subsample, '')
        || '|' || year
        || '|' || coalesce(rep, '')
        || '|' || coalesce(rotation, '')
        || '|' || coalesce(tillage, '')
        || '|' || coalesce(drainage, '')
        || '|' || coalesce(nitrogen, '')
        || '|' || coalesce(landscape, '')
        || '|' || coalesce(herbicide, '')
        || '|' || coalesce(sampledate, '') as lbl,
        varname, value from tot t JOIN plotids p on
        (t.site = p.uniqueid and t.plotid = p.plotid) WHERE 1=1 and %s and %s
        """ % (yrlist, treatlimiter, sitelimiter)

    df = pdsql.read_sql(sql, pgconn)
    sys.stderr.write("2. %s\n" % (datetime.datetime.now(), ))
    # sys.stderr.write(str(df.columns))

    dnc = form.getfirst('dnc', 'DNC')
    missing = form.getfirst('missing', '.')

    def cleaner(val):
        if val is None or val.strip() == '' or val.strip() == '.':
            return missing
        if val is not None:
            if val.strip().lower() == 'did not collect':
                return dnc
            if val.strip() == 'n/a':
                return "N/A"
        return val
    df['value'] = df['value'].apply(cleaner)
    df2 = df.pivot('lbl', 'varname', 'value')
    allcols = df2.columns.values.tolist()
    if 'all' in dvars:
        cols = cols + allcols
    else:
        cols = cols + dvars
    # sys.stderr.write(str(cols))
    (df2['site'], df2['plotid'], df2['depth'], df2['subsample'],
     df2['year'], df2['rep'], df2['rotation'], df2['tillage'],
     df2['drainage'], df2['nitrogen'], df2['landscape'],
     df2['herbicide'], df2['sampledate']
     ) = zip(*[item.split('|') for item in df2.index])
    df2['crop'] = None
    sys.stderr.write("3. %s\n" % (datetime.datetime.now(), ))

    df2cols = df2.columns.values.tolist()
    for col in cols:
        if col not in df2cols:
            cols.remove(col)

    # Assign in Rotations
    rotdf = pdsql.read_sql("""
        SELECT * from xref_rotation
        """, pgconn, index_col='code')

    def find_rotation(rotation, year):
        try:
            return rotdf.at[rotation, 'y%s' % (year, )]
        except:
            return ''
    df2['crop'] = df2[['rotation', 'year']].apply(lambda x:
                                                  find_rotation(x[0], x[1]),
                                                  axis=1)
    sys.stderr.write("4. %s\n" % (datetime.datetime.now(), ))

    fmt = form.getfirst('format', 'csv')
    if fmt == 'excel':
        sys.stdout.write("Content-type: application/vnd.ms-excel\n")
        sys.stdout.write((
            "Content-Disposition: attachment;Filename=cscap.xlsx\n\n"))
        writer = pd.ExcelWriter("/tmp/cscap.xlsx", engine='xlsxwriter')
        df2.to_excel(writer, columns=cols, index=False,
                     encoding='latin-1', sheet_name='Sheet1')
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        format2 = workbook.add_format({'num_format': '@'})
        worksheet.set_column('B:E', None, format2)
        writer.close()
        return open('/tmp/cscap.xlsx', 'rb').read()
    elif fmt == 'tab':
        sys.stdout.write("Content-type: text/plain\n\n")
        return df2.to_csv(columns=cols, sep='\t', index=False)
    sys.stdout.write("Content-type: text/plain\n\n")
    sys.stderr.write("5. %s\n" % (datetime.datetime.now(), ))
    return df2.to_csv(columns=cols, index=False)


def main():
    """Main"""
    form = cgi.FieldStorage()
    check_auth(form)
    report = form.getfirst('report', 'ag1')
    if report == 'ag1':
        sys.stdout.write("Content-type: text/plain\n\n")
        sys.stdout.write(get_agdata())
    elif report == 'dl':  # coming from internal website
        sys.stdout.write(get_dl(form))
    else:
        sys.stdout.write("Content-type: text/plain\n\n")
        sys.stdout.write(get_nitratedata())

if __name__ == '__main__':
    # Do Something
    main()
