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

# Third Party
from paste.request import parse_formvars, MultiDict
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
        "SELECT * from data_dictionary ORDER by file_name ASC",
        pgconn,
        index_col=None,
    )
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
        pgconn, writer, df, "Site Metadata", "meta_site_characteristics.csv"
    )
    worksheet.set_column("A:A", 12)
    worksheet.set_column("L:R", 12)


def do_generic(pgconn, writer, tt, fn, tablename, sites, varnames, missing):
    """generalized datatable dumper."""
    df = read_sql(
        f"SELECT * from {tablename} WHERE siteid in %s " "ORDER by siteid",
        pgconn,
        params=(tuple(sites),),
        index_col=None,
    )
    standard = ["siteid", "plotid", "location", "date", "comments", "year"]
    # filter out unwanted columns
    if "_ALL" not in varnames:
        for col in df.columns:
            if col in varnames or col in standard:
                continue
            df = df.drop(col, axis=1)
    # String aggregate above creates a mixture of None and "None"
    df = (
        df.replace(["None", None], np.nan)
        .dropna(how="all")
        .fillna(missing)
        .reset_index()
    )
    valid2date(df)
    df, _worksheet = add_bling(pgconn, writer, df, tt, fn)


def add_bling(pgconn, writer, df, sheetname, filename):
    """Do fancy things"""
    # Insert some headers rows
    metarows = [{}, {}]
    cols = df.columns
    vardf = get_vardf(pgconn, filename)
    for i, colname in enumerate(cols):
        if i == 0:
            metarows[0][colname] = "description"
            metarows[1][colname] = "units"
            continue
        if colname in vardf.index:
            metarows[0][colname] = vardf.at[colname, "brief_description"]
            metarows[1][colname] = vardf.at[colname, "units"]

    df = pd.concat([pd.DataFrame(metarows), df], ignore_index=True)
    # re-establish the correct column sorting
    df = df.reindex(cols, axis=1)
    df.to_excel(writer, sheetname, index=False)
    worksheet = writer.sheets[sheetname]
    worksheet.freeze_panes(3, 0)
    return df, worksheet


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
        "meta_plot_characteristics.csv",
    )
    # Make plotids as strings and not something that goes to dates
    workbook = writer.book
    format1 = workbook.add_format({"num_format": "0"})
    worksheet.set_column("B:B", 12, format1)


def get_vardf(pgconn, filename):
    """Get a dataframe of descriptors for this tabname"""
    return read_sql(
        """
        select element_or_value_display_name as varname,
        brief_description, units from data_dictionary WHERE
        file_name = %s
    """,
        pgconn,
        params=(filename,),
        index_col="varname",
    )


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
    tmpfn = "td_%s.xlsx" % (
        datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S"),
    )
    writer = pd.ExcelWriter(f"/tmp/{tmpfn}", engine="xlsxwriter")

    # First sheet is Data Dictionary
    if "SHM5" in shm or "_ALL" in shm:
        do_dictionary(pgconn, writer)
        pprint("do_dictionary() is done")

    # Sheet two is plot IDs
    if "SHM4" in shm or "_ALL" in shm:
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
            "agronomic_data.csv",
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
                "soil_moisture_data.csv",
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
                "drain_flow_and_N_loads_data.csv",
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
                "water_quality_data.csv",
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
            "soil_properties_data.csv",
            "soil_properties_data",
            sites,
            soil,
            missing,
        )
        pprint("Soil is done")

    # Management
    # Planting
    if "SHM1" in shm or "_ALL" in shm:
        do_generic(
            pgconn,
            writer,
            "Planting",
            "mngt_planting_data.csv",
            "mngt_planting_data",
            sites,
            ["_ALL"],
            missing,
        )
        pprint("do_operations() is done")
    # DWM
    if "SHM7" in shm or "_ALL" in shm:
        do_generic(
            pgconn,
            writer,
            "DWM",
            "mngt_dwm_data.csv",
            "mngt_dwm_data",
            sites,
            ["_ALL"],
            missing,
        )
        pprint("do_dwm() is done")
    # Notes
    if "SHM6" in shm or "_ALL" in shm:
        do_generic(
            pgconn,
            writer,
            "Notes",
            "mngt_notes_data.csv",
            "mngt_notes_data",
            sites,
            ["_ALL"],
            missing,
        )
        pprint("do_notes() is done")
    # Notes
    if "SHM8" in shm or "_ALL" in shm:
        do_generic(
            pgconn,
            writer,
            "Sites",
            "meta_site_characteristics.csv",
            "meta_site_characteristics",
            sites,
            ["_ALL"],
            missing,
        )
        pprint("do_sites() is done")
    # Send to client
    writer.close()
    msg = MIMEMultipart()
    msg["Subject"] = "Transforming Drainage Dataset"
    msg["From"] = "ISU Data Team <isudatateam@iastate.edu>"
    msg["To"] = email
    msg.preamble = "Data"
    # conservative limit of 8 MB
    # if os.stat('/tmp/cscap.xlsx').st_size > 8000000:
    pprint(f"Created spreadsheet: /tmp/{tmpfn}")
    try:
        shutil.copyfile(f"/tmp/{tmpfn}", f"/var/webtmp/{tmpfn}")
        os.unlink(f"/tmp/{tmpfn}")
    except PermissionError:
        pass
    uri = f"https://datateam.agron.iastate.edu/tmp/{tmpfn}"
    text = EMAILTEXT % (
        datetime.datetime.utcnow().strftime("%d %B %Y %H:%M:%S"),
        uri,
    )

    msg.attach(MIMEText(text))

    if email is not None:
        with smtplib.SMTP("localhost") as s:
            s.sendmail(msg["From"], msg["To"], msg.as_string())
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


def test_crawling():
    """Test that we can crawl."""
    form = MultiDict(
        [
            ("agronomic[]", "_ALL"),
            ("water[]", "_ALL"),
            ("shm[]", "_ALL"),
        ]
    )
    do_work(form)
    assert False
