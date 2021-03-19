"""This is our fancy pants download function

select string_agg(column_name, ', ') from
    (select column_name, ordinal_position from information_schema.columns where
    table_name='management' ORDER by ordinal_position) as foo;
"""
import sys
import os
import datetime
import shutil
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


from paste.request import parse_formvars
import pandas as pd
from pandas.io.sql import read_sql
import numpy as np
from pyiem.util import get_dbconn

EMAILTEXT = """
Transforming Drainage Research Data
Requested: %s UTC
Website: https://www.drainagedata.org
Contact: isudata@iastate.edu

Please click here to download your spreadsheet file.

    %s (link will remain active for 30 days)

Please reference this data set in publications as:

Chighladze, G., L.J. Abendroth, D. Herzmann, M. Helmers, L. Ahiablame,
B. Allred, L. Bowling, L. Brown, N. Fausey, J. Frankenberger, D. Jaynes,
X. Jia, J. Kjaersgaard, K. King, E. Kladivko, K. Nelson, L. Pease,
B. Reinhart, J. Strock, and M. Youssef 2021. Transforming Drainage
Project Research Data (USDA-NIFA Award No. 2015-68007-23193).
National Agricultural Library - ARS - USDA.
https://doi.org/10.15482/USDA.ADC/1521092

You are encouraged to contact the Transforming Drainage Project personnel
listed above to ensure proper data interpretation and may also consider
co-authoring with them. You shall have no claim of ownership to any
intellectual property rights in any part of the data, content, functions,
features, code, data exploration tools, logos or other intellectual
property comprising this database.

The coauthors individually and the Transforming Drainage Project collectively
shall NOT be responsible or liable for the accuracy, reliability and
completeness of any information included in the database nor the
suitability of its application for any particular purpose. Data users are
encouraged to notify the coauthors regarding data quality and other issues
concerning the data, tools, and functionality of this website. Contact
isudatateam@iastate.edu with any questions or concerns regarding usage
and application of the data available at this site.

"""

FERTELEM = [
    "nitrogen",
    "phosphorus",
    "phosphate",
    "potassium",
    "potash",
    "sulfur",
    "calcium",
    "magnesium",
    "zinc",
    "iron",
]
KGH_LBA = 1.12085

# runtime storage
MEMORY = dict(stamp=datetime.datetime.utcnow())


def pprint(msg):
    """log a pretty message for my debugging fun"""
    utcnow = datetime.datetime.utcnow()
    delta = (utcnow - MEMORY["stamp"]).total_seconds()
    MEMORY["stamp"] = utcnow
    sys.stderr.write("timedelta: %.3f %s\n" % (delta, msg))


def valid2date(df):
    """If dataframe has valid in columns, rename it to date"""
    if "valid" in df.columns:
        df.rename(columns={"valid": "date"}, inplace=True)


def conv(value, detectlimit):
    """Convert a value into something that gets returned"""
    # Careful here, we need to keep missing values for a later replacement
    if value is None or value == "":
        return None
    if value in ["n/a", "did not collect"]:
        return None
    if value.startswith("<"):
        if detectlimit == "1":
            return value
        floatval = float(value[1:])
        if detectlimit == "2":
            return floatval / 2.0
        if detectlimit == "3":
            return floatval / 2 ** 0.5
        if detectlimit == "4":
            return "M"
    try:
        return float(value)
    except Exception:
        return value


def do_dictionary(pgconn, writer):
    """Add Data Dictionary to the spreadsheet"""
    df = read_sql(
        "SELECT * from data_dictionary_export ORDER by ss_order ASC",
        pgconn,
        index_col=None,
    )
    for col in ["ss_order", "number_of_decimal_places_to_round_up"]:
        df.drop(col, axis=1, inplace=True)
    df.to_excel(writer, "Data Dictionary", index=False)
    # Increase column width
    worksheet = writer.sheets["Data Dictionary"]
    worksheet.set_column("A:D", 36)
    worksheet.set_column("E:E", 18)
    worksheet.set_column("F:F", 36)
    worksheet.set_column("G:J", 12)
    worksheet.set_column("K:K", 60)


def do_metadata_master(pgconn, writer, sites, missing):
    """get Metadata master data"""
    df = read_sql(
        """
    SELECT uniqueid,
    nwlon as "NW Lon", nwlat as "NW Lat", swlon as "SW Lon", swlat as "SW Lat",
    selon as "SE Lon", selat as "SE Lat", nelon as "NE Lon", nelat as "NE Lat",
    rawlonlat as "raw LonLat", state, county, city as "city",
    landscapeslope as "landscape slope",
    tilespacing as "tile spacing",
    tiledepth as "tile depth",
    sitearea as "site area",
    numberofplots as "number of plots"
    from metadata_master
    WHERE uniqueid in %s
    ORDER by uniqueid
    """,
        pgconn,
        params=(tuple(sites),),
        index_col=None,
    )
    df.replace(["None", None, ""], np.nan, inplace=True)
    df.dropna(how="all", inplace=True)
    df.fillna(missing, inplace=True)
    df, worksheet = add_bling(
        pgconn, writer, df, "Site Metadata", "Site Metadata"
    )
    worksheet.set_column("A:A", 12)
    worksheet.set_column("L:R", 12)


def do_generic(pgconn, writer, tabtitle, tablename, sites, varnames, missing):
    """generalized datatable dumper."""
    df = read_sql(
        f"SELECT * from {tablename} WHERE siteid in %s " "ORDER by siteid",
        pgconn,
        params=(tuple(sites),),
        index_col=None,
    )
    standard = ["siteid", "plotid", "location", "date", "comments", "year"]
    # filter out unwanted columns
    for col in df.columns:
        if col in varnames or col in standard:
            continue
        df = df.drop(col, axis=1)
    # String aggregate above creates a mixture of None and "None"
    df = df.replace(["None", None], np.nan)
    df = df.dropna(how="all")
    df = df.fillna(missing)
    df = df.reset_index()
    valid2date(df)
    df, _worksheet = add_bling(pgconn, writer, df, tabtitle, tabtitle)


def add_bling(pgconn, writer, df, sheetname, tabname):
    """Do fancy things"""
    # Insert some headers rows
    metarows = [{}, {}]
    cols = df.columns
    for i, colname in enumerate(cols):
        if i == 0:
            metarows[0][colname] = "description"
            metarows[1][colname] = "units"
            continue
    df = pd.concat([pd.DataFrame(metarows), df], ignore_index=True)
    # re-establish the correct column sorting
    df = df.reindex(cols, axis=1)
    df.to_excel(writer, sheetname, index=False)
    worksheet = writer.sheets[sheetname]
    worksheet.freeze_panes(3, 0)
    return df, worksheet


def do_operations(pgconn, writer, sites, missing):
    """Return a DataFrame for the operations"""
    opdf = read_sql(
        """
    SELECT uniqueid, cropyear, operation, valid, cashcrop, croprot,
    plantryemethod, planthybrid, plantmaturity, plantrate, plantrateunits,
    terminatemethod, biomassdate1, biomassdate2, depth, limerate,
    manuresource, manuremethod, manurerate,
    manurerateunits, fertilizerform, fertilizercrop, fertilizerapptype,
    fertilizerformulation, productrate, nitrogenelem, phosphoruselem,
    potassiumelem, sulfurelem, zincelem, magnesiumelem, ironelem, calciumelem,
    stabilizer, stabilizerused, stabilizername, comments,
    -- These are deleted below
    nitrogen, phosphorus, phosphate, potassium,
    potash, sulfur, calcium, magnesium, zinc, iron
    from operations where uniqueid in %s
    ORDER by uniqueid ASC, cropyear ASC, valid ASC
    """,
        pgconn,
        params=(tuple(sites),),
    )
    opdf["productrate"] = pd.to_numeric(opdf["productrate"], errors="coerce")
    for fert in FERTELEM:
        opdf[fert] = pd.to_numeric(opdf[fert], errors="coerce")
    for col in ["biomassdate1", "biomassdate2", "valid"]:
        opdf.at[opdf[col].isnull(), col] = missing

    # __________________________________________________________
    # case 1, values are > 0, so columns are in %
    df = opdf[opdf["productrate"] > 0]
    for elem in FERTELEM:
        opdf.loc[df.index, elem] = (
            df["productrate"] * KGH_LBA * df[elem] / 100.0
        )
    opdf.loc[df.index, "productrate"] = df["productrate"] * KGH_LBA

    # ________________________________________________________
    # case 2, value is -1 and so columns are in lbs / acre
    df = opdf[opdf["productrate"] < 0]
    opdf.loc[df.index, "productrate"] = None
    for elem in FERTELEM:
        opdf.loc[df.index, elem] = df[elem] * KGH_LBA
        del opdf[elem]
    valid2date(opdf)
    del opdf["productrate"]
    opdf, worksheet = add_bling(
        pgconn, writer, opdf, "Field Operations", "Field Operations"
    )
    worksheet.set_column("C:C", 18)
    worksheet.set_column("D:D", 12)
    worksheet.set_column("M:N", 12)


def do_management(pgconn, writer, sites):
    """Return a DataFrame for the management"""
    opdf = read_sql(
        """
    SELECT uniqueid, cropyear, notill, irrigation, irrigationamount,
    irrigationmethod, residueremoval, residuehow, residuebiomassweight,
    residuebiomassmoisture, residueplantingpercentage, residuetype,
    limeyear, comments
    from management where uniqueid in %s
    ORDER by cropyear ASC
    """,
        pgconn,
        params=(tuple(sites),),
    )
    opdf.to_excel(writer, "Residue, Irrigation", index=False)


def do_plotids(pgconn, writer, sites):
    """Write plotids to the spreadsheet"""
    opdf = read_sql(
        "SELECT * from meta_plot_identifier where siteid in %s "
        "ORDER by siteid, plotid ASC",
        pgconn,
        params=(tuple(sites),),
    )
    opdf, worksheet = add_bling(
        pgconn,
        writer,
        opdf[opdf.columns],
        "Plot Identifiers",
        "Plot Identifiers",
    )
    # Make plotids as strings and not something that goes to dates
    workbook = writer.book
    format1 = workbook.add_format({"num_format": "0"})
    worksheet.set_column("B:B", 12, format1)


def do_notes(pgconn, writer, sites, missing):
    """Write notes to the spreadsheet"""
    opdf = read_sql(
        """
        SELECT "primary" as uniqueid, overarching_data_category, data_type,
        replace(growing_season, '.0', '') as growing_season,
        comments
        from highvalue_notes where "primary" in %s
        ORDER by "primary" ASC, overarching_data_category ASC, data_type ASC,
        growing_season ASC
    """,
        pgconn,
        params=(tuple(sites),),
    )
    opdf.replace(["None", None, ""], np.nan, inplace=True)
    opdf.dropna(how="all", inplace=True)
    opdf.fillna(missing, inplace=True)
    opdf[opdf.columns].to_excel(writer, "Notes", index=False)
    # Increase column width
    worksheet = writer.sheets["Notes"]
    worksheet.set_column("B:B", 36)
    worksheet.set_column("C:D", 18)
    worksheet.set_column("E:G", 36)


def do_dwm(pgconn, writer, sites, missing):
    """Write dwm to the spreadsheet"""
    opdf = read_sql(
        """
        SELECT uniqueid, plotid, cropyear, cashcrop, boxstructure,
        outletdepth, outletdate, comments
        from dwm where uniqueid in %s
        ORDER by uniqueid ASC, cropyear ASC
    """,
        pgconn,
        params=(tuple(sites),),
    )
    opdf.replace(["None", None, ""], np.nan, inplace=True)
    opdf.dropna(how="all", inplace=True)
    opdf.fillna(missing, inplace=True)
    _df, worksheet = add_bling(
        pgconn,
        writer,
        opdf[opdf.columns],
        "Drainage Control Structure Mngt",
        "Drainage Control Structure Mngt",
    )
    worksheet.set_column("G:G", 12)
    worksheet.set_column("H:H", 30)


def compare(hascols, wanted):
    """Does any of the following apply."""
    return any([x in hascols for x in wanted])


def do_work(form):
    """do great things"""
    pgconn = get_dbconn("td")
    email = form.get("email")
    sites = form.getall("sites[]")
    if not sites:
        sites.append("XXX")
    agronomic = form.getall("agronomic[]")
    water = form.getall("water[]")
    soil = form.getall("soil[]")
    shm = form.getall("shm[]")
    missing = form.get("missing", "M")
    if missing == "__custom__":
        missing = form.get("custom_missing", "M")
    pprint("Missing is %s" % (missing,))
    # detectlimit = form.get("detectlimit", "1")

    # pylint: disable=abstract-class-instantiated
    writer = pd.ExcelWriter("/tmp/td.xlsx", engine="xlsxwriter")

    # First sheet is Data Dictionary
    if "SHM5a" in shm:
        do_dictionary(pgconn, writer)
        pprint("do_dictionary() is done")

    # Sheet two is plot IDs
    if "SHM4a" in shm:
        do_plotids(pgconn, writer, sites)
        pprint("do_plotids() is done")

    if agronomic:
        agronomic.extend(
            [
                "crop",
            ]
        )
        do_generic(
            pgconn,
            writer,
            "Agronomic",
            "agronomic_data",
            sites,
            agronomic,
            missing,
        )
        pprint("Agronomic is done")
    if water:
        water.extend(["depth", "dwm_treatment", "sample_type", "height"])
        cols = ["soil_moisture", "soil_temperature", "soil_ec"]
        if compare(cols, water):
            do_generic(
                pgconn,
                writer,
                "Soil Moisture",
                "soil_moisture_data",
                sites,
                water,
                missing,
            )
            pprint("Soil Moisture is done")
        cols = (
            "tile_flow discharge nitrate_n_load nitrate_n_removed "
            "tile_flow_filled nitrate_n_load_filled"
        ).split()
        if compare(cols, water):
            do_generic(
                pgconn,
                writer,
                "Tile Flow and Load",
                "tile_flow_and_n_loads_data",
                sites,
                water,
                missing,
            )
            pprint("Tile Flow and Loads is done")
        cols = (
            "nitrate_n_concentration ammonia_n_concentration "
            "total_n_filtered_concentration total_n_unfiltered_concentration "
            "ortho_p_filtered_concentration ortho_p_unfiltered_concentration "
            "total_p_filtered_concentration total_p_unfiltered_concentration "
            "ph water_ec"
        ).split()
        if compare(cols, water):
            do_generic(
                pgconn,
                writer,
                "Water Quality",
                "water_quality_data",
                sites,
                water,
                missing,
            )
            pprint("Tile Flow and Loads is done")
    if soil:
        do_generic(
            pgconn,
            writer,
            "Soil",
            "soil_properties_data",
            sites,
            soil,
            missing,
        )
        pprint("Soil is done")

    """
    # Management
    # Field Operations
    if "SHM1a" in shm:
        do_operations(pgconn, writer, sites, missing)
        pprint("do_operations() is done")
    # Residue and Irrigation
    if "SHM3a" in shm:
        do_management(pgconn, writer, sites)
        pprint("do_management() is done")
    # Site Metadata
    if "SHM8a" in shm:
        do_metadata_master(pgconn, writer, sites, missing)
        pprint("do_metadata_master() is done")
    # Drainage Management
    if "SHM7a" in shm:
        do_dwm(pgconn, writer, sites, missing)
        pprint("do_dwm() is done")
    # Notes
    if "SHM6a" in shm:
        do_notes(pgconn, writer, sites, missing)
        pprint("do_notes() is done")
    """
    # Send to client
    writer.close()
    msg = MIMEMultipart()
    msg["Subject"] = "Transforming Drainage Dataset"
    msg["From"] = "ISU Data Team <isudatateam@iastate.edu>"
    msg["To"] = email
    msg.preamble = "Data"
    # conservative limit of 8 MB
    # if os.stat('/tmp/cscap.xlsx').st_size > 8000000:
    tmpfn = ("td_%s.xlsx") % (
        datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S"),
    )
    shutil.copyfile("/tmp/td.xlsx", "/var/webtmp/%s" % (tmpfn,))
    uri = "https://datateam.agron.iastate.edu/tmp/%s" % (tmpfn,)
    text = EMAILTEXT % (
        datetime.datetime.utcnow().strftime("%d %B %Y %H:%M:%S"),
        uri,
    )

    msg.attach(MIMEText(text))

    _s = smtplib.SMTP("localhost")
    _s.sendmail(msg["From"], msg["To"], msg.as_string())
    _s.quit()
    os.unlink("/tmp/td.xlsx")
    cursor = pgconn.cursor()
    cursor.execute(
        "INSERT into website_downloads(email) values (%s)", (email,)
    )
    cursor.close()
    pgconn.commit()
    pprint("is done!!!")
    return b"Email Delivered!"


def application(environ, start_response):
    """Do Stuff"""
    form = parse_formvars(environ)
    agree = form.get("agree")
    start_response("200 OK", [("Content-type", "text/plain")])
    if agree != "AGREE":
        return [b"You did not agree to download terms."]

    return [do_work(form)]
