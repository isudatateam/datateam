"""This is our fancy pants download function

select string_agg(column_name, ', ') from
    (select column_name, ordinal_position from information_schema.columns where
    table_name='management' ORDER by ordinal_position) as foo;
"""

import os
import shutil
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import numpy as np
import pandas as pd
from paste.request import MultiDict, parse_formvars
from pyiem.database import get_dbconn, get_dbconnstr, sql_helper
from pyiem.util import logger, utc
from pymemcache import Client

LOG = logger()
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
MEMORY = dict(stamp=utc())


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
            return floatval / 2**0.5
        if detectlimit == "4":
            return "M"
    try:
        return float(value)
    except Exception:
        return value


def do_dictionary(pgconn, writer):
    """Add Data Dictionary to the spreadsheet"""
    df = pd.read_sql(
        "SELECT * from data_dictionary ORDER by file_name ASC",
        pgconn,
        index_col=None,
    )
    sheetname = "Data Dictionary"
    df.to_excel(writer, sheet_name=sheetname, index=False)
    # Increase column width
    worksheet = writer.sheets[sheetname]
    worksheet.set_column("A:Z", 30)


def do_metadata_master(pgconn, writer, sites, missing):
    """get Metadata master data"""
    df = pd.read_sql(
        sql_helper(
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
    WHERE uniqueid = ANY(:sites)
    ORDER by uniqueid
    """
        ),
        pgconn,
        params={"sites": sites},
        index_col=None,
    )
    df = df.replace(["None", None, ""], np.nan)
    df.dropna(how="all", inplace=True)
    df.fillna(missing, inplace=True)
    df, worksheet = add_bling(
        pgconn, writer, df, "Site Metadata", "meta_site_characteristics.csv"
    )
    worksheet.set_column("A:Z", 30)


def do_generic(pgconn, writer, tt, fn, tablename, sites, varnames, missing):
    """generalized datatable dumper."""
    df = pd.read_sql(
        sql_helper(
            "SELECT * from {table} WHERE siteid = ANY(:sites) ORDER by siteid",
            table=tablename,
        ),
        pgconn,
        params={"sites": sites},
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
    df = df.replace(["None", None], np.nan).dropna(how="all").fillna(missing)
    valid2date(df)
    df, worksheet = add_bling(pgconn, writer, df, tt, fn)
    worksheet.set_column("A:Z", 30)


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
    df = df.reindex(columns=cols)
    df.to_excel(writer, sheet_name=sheetname, index=False)
    worksheet = writer.sheets[sheetname]
    worksheet.freeze_panes(3, 0)
    return df, worksheet


def do_plotids(pgconn, writer, sites):
    """Write plotids to the spreadsheet"""
    opdf = pd.read_sql(
        sql_helper(
            "SELECT * from meta_plot_identifier where siteid = ANY(:sites) "
            "ORDER by siteid, plotid ASC"
        ),
        pgconn,
        params={"sites": sites},
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
    worksheet.set_column("A:Z", 30)
    worksheet.set_column("B:B", 12, format1)


def get_vardf(pgconn, filename):
    """Get a dataframe of descriptors for this tabname"""
    return pd.read_sql(
        "select element_or_value_display_name as varname, brief_description, "
        "units from data_dictionary WHERE file_name = %s",
        pgconn,
        params=(filename,),
        index_col="varname",
    )


def compare(hascols, wanted):
    """Does any of the following apply."""
    return any(x in hascols for x in wanted)


def do_work(form):
    """do great things"""
    pgconn = get_dbconnstr("td")
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

    tmpfn = f"td_{utc():%Y%m%d%H%M%S}.xlsx"
    writer = pd.ExcelWriter(f"/tmp/{tmpfn}", engine="xlsxwriter")

    # First sheet is Data Dictionary
    if "SHM5" in shm or "_ALL" in shm:
        do_dictionary(pgconn, writer)

    # Sheet two is plot IDs
    if "SHM4" in shm or "_ALL" in shm:
        do_plotids(pgconn, writer, sites)
        do_generic(
            pgconn,
            writer,
            "Treatments",
            "meta_treatment_identifier.csv",
            "meta_treatment_identifier",
            sites,
            ["_ALL"],
            missing,
        )

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
        cols = [
            "water_table_depth",
        ]
        if compare(cols, water):
            do_generic(
                pgconn,
                writer,
                "Water Table",
                "water_table_data.csv",
                "water_table_data",
                sites,
                water,
                missing,
            )
        cols = [
            "water_stage",
        ]
        if compare(cols, water):
            do_generic(
                pgconn,
                writer,
                "Water Stage",
                "water_stage_data.csv",
                "water_stage_data",
                sites,
                water,
                missing,
            )
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

    # Management
    if "SHM1" in shm or "_ALL" in shm:
        _titles = "Planting Tillage Residue Fertilizing Harvesting Irrigation"
        for mngt in _titles.split():
            do_generic(
                pgconn,
                writer,
                mngt if mngt != "Irrigation" else "Irrigation Event Based",
                f"mngt_{mngt.lower()}_data.csv",
                f"mngt_{mngt.lower()}_data",
                sites,
                ["_ALL"],
                missing,
            )
        # Secondary Irrigation Tab
        do_generic(
            pgconn,
            writer,
            "Irrigation Daily",
            "irrigation_data.csv",
            "irrigation_data",
            sites,
            ["_ALL"],
            missing,
        )
    # DWM
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
    # Methods
    if "SHM1" in shm or "_ALL" in shm:
        do_generic(
            pgconn,
            writer,
            "Methods",
            "meta_methods.csv",
            "meta_methods",
            sites,
            ["_ALL"],
            missing,
        )

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
        do_generic(
            pgconn,
            writer,
            "Plots",
            "meta_plot_characteristics.csv",
            "meta_plot_characteristics",
            sites,
            ["_ALL"],
            missing,
        )
    # Send to client
    writer.close()
    msg = MIMEMultipart()
    msg["Subject"] = "Transforming Drainage Dataset"
    msg["From"] = "ISU Data Team <isudatateam@iastate.edu>"
    msg["To"] = email
    msg.preamble = "Data"
    try:
        shutil.copyfile(f"/tmp/{tmpfn}", f"/var/webtmp/{tmpfn}")
        os.unlink(f"/tmp/{tmpfn}")
    except PermissionError:
        pass
    uri = f"https://datateam.agron.iastate.edu/tmp/{tmpfn}"
    etext = EMAILTEXT % (
        utc().strftime("%d %B %Y %H:%M:%S"),
        uri,
    )

    msg.attach(MIMEText(etext))

    if email is not None:
        with smtplib.SMTP("localhost") as s:
            s.sendmail(msg["From"], msg["To"], msg.as_string())
    pgconn = get_dbconn("td")
    cursor = pgconn.cursor()
    cursor.execute(
        "INSERT into website_downloads(email) values (%s)", (email,)
    )
    cursor.close()
    pgconn.commit()
    return b"Email Delivered!"


def preventive_log(pgconn, environ):
    """Mostly prevent scripting."""
    cursor = pgconn.cursor()
    for _ in range(4):
        cursor.execute(
            "INSERT into weblog(client_addr, uri, referer, http_status) "
            "VALUES (%s, %s, %s, %s)",
            (
                environ.get("REMOTE_ADDR"),
                environ.get("REQUEST_URI", ""),
                environ.get("HTTP_REFERER"),
                404,
            ),
        )

    cursor.close()


def throttle(environ):
    """Slow down the script kiddies."""
    addr = environ.get("REMOTE_ADDR")
    key = (f"{addr}_datateam_dl").encode("utf-8")
    mc = Client("iem-memcached")
    if mc.get(key):
        mc.close()
        LOG.warning("throttle: %s", addr)
        return True
    mc.set(key, 1, 10)
    mc.close()
    return False


def application(environ, start_response):
    """Do Stuff"""
    with get_dbconn("mesosite") as pgconn:
        preventive_log(pgconn, environ)
        pgconn.commit()
    start_response("200 OK", [("Content-type", "text/plain")])
    if throttle(environ):
        return [b"You did not agree to download terms."]
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
